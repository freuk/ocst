open Obandit
open Events
open Engine
open Easy
open Metrics

let default_policies = [BatList.assoc "wait" Metrics.criteriaList;BatList.assoc "spf" Metrics.criteriaList]

module type PeriodParam = sig
  val period : int
  val policyList : (module CriteriaSig) list
end

type rewardType = Basic | Centered | Ratio | Raw
let rewardTypeEncoding = [("basic",Basic); ("centered",Centered); ("ratio",Ratio); ("raw",Raw)]

module type BanditSelectorParam = sig
  include PeriodParam
  val rewardType : rewardType
  val rate : float
  val jobHeap : Events.event_heap
  val out_reset : out_channel option
  val out_select : out_channel option
end

let listReorderFunctions lc systemModule = 
  let f m = 
    let module M = MakeReservationSelector((val systemModule:SystemParamSig))((val m:CriteriaSig))
    in M.reorder
  in List.map f lc

module DummyRange
(B:Bandit)
:RangedBandit with type bandit=B.bandit
= struct
  type bandit = B.bandit
  let initialBandit = {bandit=B.initialBandit;
                       u=0.;
                       l=0.}
  let step b x = let a,bn = B.step b.bandit x
                 in (Action a, {b with bandit = bn})
end

module MakeBanditSelector
(BSP:BanditSelectorParam)
(Reward:StatSig with type outputStat = float)
(P:SystemParamSig)
: ReservationSelector
= struct

  let lastTimeId = ref (-1)
  let currentPolicy = ref (0)

  (* Makes a comparator function.*)
  let makecmp v x y = Pervasives.compare (v x) (v y)
  (* Gets the argmax of a function on a list.*)
  let argmin f l = fst (BatList.min_max ~cmp:(makecmp f) l)

  let allReorders = listReorderFunctions BSP.policyList (module P:SystemParamSig)

  let karms = List.length BSP.policyList

  let stats = ref (BatList.map (fun p -> (0., 1, p) ) (BatList.range 0 `To (karms-1)))

  let reorder now l =
      if (!lastTimeId = -1) || ((now / BSP.period) > !lastTimeId) then
        begin
          lastTimeId := now / BSP.period;
          let r',n' = Reward.getStat (), Reward.getN ()
          in begin
            Reward.reset ();
            stats := BatList.modify_at !currentPolicy (fun (r,n,p) -> (r +. r',n+n',p)) !stats;
            currentPolicy := 
              if Random.float 1. < BSP.rate then
                let _,_,p = argmin (fun (r,n,_) -> r /. (float_of_int n)) !stats
                in p
              else
                Random.int karms;
            Printf.printf "%d\n" !currentPolicy;
            let module C = (val (List.nth BSP.policyList !currentPolicy):CriteriaSig)
            in BatOption.may (fun c ->  Printf.fprintf c "%s\n" C.desc) BSP.out_select ;
          end
        end;
      (List.nth allReorders !currentPolicy) now l
end

module MakeRandomBanditSelector
(BSP:PeriodParam)
(P:SystemParamSig)
: ReservationSelector
= struct
  let lastTimeId = ref 0
  let currentPolicy = ref (-1)

  let allReorders = listReorderFunctions BSP.policyList (module P:SystemParamSig)

  let reorder now l =
      if  (!lastTimeId = 0) || ((now / BSP.period) > !lastTimeId) then
        begin
          lastTimeId := now / BSP.period;
          currentPolicy := Random.int (List.length BSP.policyList)
        end;
      (List.nth allReorders !currentPolicy) now l
end

module type ClairvoyantBanditParam = sig
  include PeriodParam
  val jobHeap : Events.event_heap
  val out_select : out_channel option
end

module MakeClairvoyantBanditSelector
(BSP:ClairvoyantBanditParam)
(Reward:StatSig with type outputStat = float)
(P:SystemParamSig)
: ReservationSelector
= struct
  let lastReward = ref 0.
  let lastN = ref 0
  let lastTimeId = ref (-1)
  let currentPolicy = ref (module MakeReservationSelector(P)(CriteriaWait):ReservationSelector)
  let stats = ref (BatList.map (fun p -> (0., 0, p) ) BSP.policyList)

  let allReorders = listReorderFunctions BSP.policyList (module P:SystemParamSig)

  (**SIMULATION*)
  let getReward bheap policy=
    begin
     let module SimulatorParam = struct
        let eventheap = bheap
        let output_channel = None
        let output_channel_bf = None
     end
     in let module SystemParam = struct
        let waitqueue = Jobs.empty_job_waiting_queue ()
        let resourcestate = Resources.empty_resources P.resourcestate.maxprocs
        let jobs = P.jobs
     end

     in let module StatWait : StatSig
     with type outputStat=float
     = struct
       type outputStat = float
       module M = MakeWaitAccumulator(SystemParam)
       let n = ref 0
       let getN () = !n
       let getStat = M.get
       let incStat t jl = (M.add t jl; n:=!n+(List.length jl))
     let reset () =  (n := 0; M.reset() )
     end
     in let module Scheduler = MakeEasyGreedy((val policy:CriteriaSig))(CriteriaWait)(SystemParam)
     in let module S = MakeSimulator(StatWait)(Scheduler)(SimulatorParam)(SystemParam)(NoHook)
     in begin
       S.simulate () ;
       StatWait.getStat (), StatWait.getN ()
     end
  end

  let reorder now l =
      if (!lastTimeId = -1) || ((now / BSP.period) > !lastTimeId) then
        begin
          lastTimeId := now / BSP.period;
          let f t =
            let bh = Events.empty_event_heap ()
            in let fins e = Events.submit_job e.id (Jobs.find P.jobs e.id).r bh
            in let () = Events.Heap.iter fins BSP.jobHeap 
            in let x,n,p = t
            in let r , n' = getReward bh p
            in x +. r, n + n', p
          in stats := List.map f !stats;
          let fp t = let x,n,p = t 
            in let module C = (val p:CriteriaSig)
            in  Printf.printf "%f: %s \n" x C.desc
          in List.iter fp !stats;
          while (not (Events.Heap.is_empty BSP.jobHeap)) do
            ignore (Events.pop_event BSP.jobHeap)
          done;
          let f t = 
            let x, n, p = t
            in x /. float_of_int n
          in let comp t1 t2 = compare (f t1) (f t2)
          in currentPolicy := 
             let _,_,p = fst (BatList.min_max ~cmp:comp !stats)
             in let module C = (val p:CriteriaSig)
             in let () = BatOption.may (fun c ->  Printf.fprintf c "%s\n" C.desc) BSP.out_select ;
             in (module MakeReservationSelector(P)(C))
        end;
        let module M = (val !currentPolicy:ReservationSelector)
        in M.reorder now l
end


module MakeNoisyBanditSelector
(BSP:ClairvoyantBanditParam)
(Reward:StatSig with type outputStat = float)
(P:SystemParamSig)
: ReservationSelector
= struct
  let lastReward = ref 0.
  let lastN = ref 0
  let lastTimeId = ref (-1)
  let currentPolicy = ref (module MakeReservationSelector(P)(CriteriaWait):ReservationSelector)
  let stats = ref (BatList.map (fun p -> (0., 0, p) ) BSP.policyList)

  let allReorders = listReorderFunctions BSP.policyList (module P:SystemParamSig)

  (**SIMULATION*)
  let getReward bheap policy=
    begin
     let module SimulatorParam = struct
        let eventheap = bheap
        let output_channel = None
        let output_channel_bf = None
     end
     in let module SystemParam = struct
        let waitqueue = Jobs.empty_job_waiting_queue ()
        let resourcestate = Resources.empty_resources P.resourcestate.maxprocs
        let jobs = P.jobs
     end

     in let module StatWait : StatSig
     with type outputStat=float
     = struct
       type outputStat = float
       module M = MakeWaitAccumulator(SystemParam)
       let n = ref 0
       let getN () = !n
       let getStat = M.get
       let incStat t jl = (M.add t jl; n:=!n+(List.length jl))
     let reset () =  (n := 0; M.reset() )
     end
     in let module Scheduler = MakeEasyGreedy((val policy:CriteriaSig))(CriteriaWait)(SystemParam)
     in let module S = MakeSimulator(StatWait)(Scheduler)(SimulatorParam)(SystemParam)(NoHook)
     in begin
       S.simulate () ;
       let s = StatWait.getStat ()
       in s +. (Random.float (s *. 30. /. 100.)) -. (s *. 15. /. 100.)
     end
  end

  let reorder now l =
      if (!lastTimeId = -1) || ((now / BSP.period) > !lastTimeId) then
        begin
          lastTimeId := now / BSP.period;
          let f t =
            let bh = Events.empty_event_heap ()
            in let fins e = Events.submit_job e.id (Jobs.find P.jobs e.id).r bh
            in let () = Events.Heap.iter fins BSP.jobHeap 
            in let x,n,p = t
            in let r = getReward bh p
            in ((x *. float_of_int n) +. r) /. (float_of_int (n+1)), n+1, p
          in stats := List.map f !stats;
          let fp t = let x,n,p = t 
            in let module C = (val p:CriteriaSig)
            in  Printf.printf "%f: %s \n" x C.desc
          in List.iter fp !stats;
          while (not (Events.Heap.is_empty BSP.jobHeap)) do
            ignore (Events.pop_event BSP.jobHeap)
          done;
          let f t = 
            let x, n, p = t
            in x
          in let comp t1 t2 = compare (f t1) (f t2)
          in currentPolicy := 
             let _,_,p = fst (BatList.min_max ~cmp:comp !stats)
             in let module C = (val p:CriteriaSig)
             in let () = BatOption.may (fun c ->  Printf.fprintf c "%s\n" C.desc) BSP.out_select ;
             in (module MakeReservationSelector(P)(C))
        end;
        let module M = (val !currentPolicy:ReservationSelector)
        in M.reorder now l
end
