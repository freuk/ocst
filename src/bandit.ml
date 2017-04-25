open Engine
open Events
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
  val noise: bool
  val outClv : out_channel option
end

module MakeSimulationSelector
(BSP:ClairvoyantBanditParam)
(Reward:StatSig with type outputStat = float)
(P:SystemParamSig)
: ReservationSelector
= struct
  let lastReward = ref 0.
  let lastN = ref 0
  let lastTimeId = ref (-1)
  let lastWaitQueue = Jobs.empty_job_waiting_queue ()
  let lastResourceState = ref (Resources.empty_resources P.resourcestate.maxprocs)

  let currentPolicy = ref (module MakeReservationSelector(P)(CriteriaWait):ReservationSelector)
  let stats = ref (BatList.map (fun p -> (0., p) ) BSP.policyList)

  let allReorders = listReorderFunctions BSP.policyList (module P:SystemParamSig)

  (**SIMULATION*)
  let getReward wq (rstate:Resources.system_state) bheap policy=
    begin
     let module SimulatorParam = struct
        let eventheap = bheap
        let output_channel = None
        let output_channel_bf = None
     end
     in let module SystemParam = struct
        let waitqueue = ref wq
        let resourcestate = Resources.copy rstate
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
     in let module Scheduler = MakeEasyGreedy((val policy:CriteriaSig))(CriteriaSQF)(SystemParam)
     in let module S = MakeSimulator(StatWait)(Scheduler)(SimulatorParam)(SystemParam)(NoHook)
     in begin
       S.simulate () ;
       let s = StatWait.getStat ()
       in if BSP.noise then  s *. ((Random.float 0.4) +. 0.8)
       else s
     end
  end

  let stringState rs wq now = 
    let module Pp = struct
      let jobs=P.jobs
      let resourcestate = rs
      let waitqueue = ref wq
    end
    in let module FeatureMap = Features.MakeSystemFeatures(Pp)
    in let printer = BatArray.print BatFloat.print
    in BatIO.to_string printer (FeatureMap.makeVector now)

  let reorder now l =
      if (!lastTimeId = -1) || ((now / BSP.period) > !lastTimeId) then
        begin
          lastTimeId := now / BSP.period;
          let fprintclv chan = Printf.fprintf chan "%s" (stringState !lastResourceState !lastWaitQueue (now-BSP.period))
          in BatOption.may fprintclv BSP.outClv;
          let f t =
            let bh = Events.empty_event_heap ()
            in let fins e = 
                Events.submit_job e.id ((Jobs.find P.jobs e.id).r) bh
            in let () = Events.Heap.iter fins BSP.jobHeap 
            in let fend (t,i) = Heap.add bh {time=t; id=i; event_type=End}
            in let () = List.iter fend !lastResourceState.jobs_running_list
            in let x,p = t
            in let r  = getReward !lastWaitQueue !lastResourceState bh p
            in begin
              let fprintclv chan =  Printf.fprintf chan " %f" r
              in BatOption.may fprintclv BSP.outClv;
              x +. r, p
            end
          in stats := List.map f !stats;
          let fprintclv chan =  Printf.fprintf chan "%s" "\n"
          in BatOption.may fprintclv BSP.outClv;
          lastWaitQueue := !P.waitqueue;
          lastResourceState := Resources.copy P.resourcestate;
          let fp t = let x,p = t 
            in let module C = (val p:CriteriaSig)
            in  Printf.printf "%f: %s \n" x C.desc
          in List.iter fp !stats;
          while (not (Events.Heap.is_empty BSP.jobHeap)) do
            ignore (Events.pop_event BSP.jobHeap)
          done;
          let f t = 
            let x, p = t
            in x
          in let comp t1 t2 = compare (f t1) (f t2)
          in currentPolicy := 
             let _,p = fst (BatList.min_max ~cmp:comp !stats)
             in let module C = (val p:CriteriaSig)
             in let () = BatOption.may (fun c ->  Printf.fprintf c "%s\n" C.desc) BSP.out_select ;
             in (module MakeReservationSelector(P)(C))
        end;
        let module M = (val !currentPolicy:ReservationSelector)
        in M.reorder now l
end
