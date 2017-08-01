open System

(*util*)
let compose_binop f g = fun x y -> f (g x) (g y)

(*************************** Common parameter *************************************)
module type SchedulerParam = sig
  val jobs : job_table
end

(***************************Primary and backfilling selectors***********************)

(**** Primary Selector ****)
module type Primary = sig
  val desc : string
  val reorder :
    system:System.system ->  (*system state before primary jobs are started*)
    now:int ->               (*now*)
    log:System.log ->        (*log*)
    (System.log * int list)
end

module MakeGreedyPrimary
    (C:Metrics.Criteria)
    (S:SchedulerParam)
  : Primary =
struct
  let desc = C.desc
  let crit = C.criteria S.jobs
  let reorder ~system:s ~now:now ~log:log =
    let lv =
      let process_waiting i = match crit s now i with
        |Value v -> (i,v,[])
        |ValueLog (v,l) -> (i,v,l)
      in List.map process_waiting s.waiting
    in
    (lv |> List.fold_left (fun acc (i,_,x) -> ((float_of_int now)::(float_of_int i)::x)::acc) log),
    lv |> List.sort (compose_binop Pervasives.compare (fun (_,x,_) -> -. x))
    |> List.map (fun (x,_,_) -> x)
end

module type Threshold = sig
  val threshold : int 
end

module MakeThreshold
    (T:Threshold)
    (P:Primary)
    (S:SchedulerParam)
  : Primary =
struct
  let desc = P.desc
  let push_front now idlist = 
    let dth = (fun x -> (now - (Hashtbl.find S.jobs x).r) > T.threshold)
    in let a,b = BatList.partition dth idlist
    in (List.sort (compose_binop Pervasives.compare (fun i -> (Hashtbl.find S.jobs i).r )) a) @ b
  let reorder ~system:s ~now:now ~log:log = 
    let (l,idlist) = P.reorder ~system:s ~now:now ~log:log
    in (l,push_front now idlist)
end

type sampling = Softmax | Linear
let sampling_types = 
  [ ("softmax",Softmax);
    ("linear",Linear) ]
module type ProbaPolParam = sig
  val sampling :  sampling
  val criterias : Metrics.criteria list
  val alpha : float list
end

module MakeProbabilisticPrimary
    (P:ProbaPolParam)
    (S:SchedulerParam)
  : Primary =
struct
  let desc = "probabilistic."
  let pick_random_weighted l=
    let r = Random.float (float_of_int 1)
    in let rec nextrandom s i' = function
        |(p,i)::is when s +. p >= r -> i
        |(p,i)::is -> nextrandom (s +. p) i is
        |[]-> i'
    in nextrandom 0. (snd (List.hd l)) l

  let reorder ~system:s ~now:now ~log:log =
    let probas = match P.sampling with
      |Softmax ->
          let exp_alphas = (List.map exp P.alpha)
          in let denom = BatList.fsum exp_alphas
          in List.map (fun x -> x /. denom) exp_alphas
      |Linear ->
          let min_alphas = fst (BatList.min_max P.alpha)
          in let scaled_alphas = List.map (fun x -> x -. min_alphas) P.alpha
          in let denom = BatList.fsum scaled_alphas
          in List.map (fun x -> x /. (max 0.000001 denom)) scaled_alphas
    in let crit = pick_random_weighted (BatList.map2 (fun p i -> (p,i)) probas P.criterias)
    in let process_waiting i = match crit S.jobs s now i with
        |Value v -> (i,v)
        |ValueLog (v,l) -> (i,v)
    in let v = List.map process_waiting s.waiting
               |> List.sort (compose_binop Pervasives.compare snd) 
               |> List.map fst
    in ([],v)
end

module type HysteresisParam = sig
  val thresholds : float * float
  val policies : Metrics.criteria * Metrics.criteria
end

module MakeHysteresisPrimary
    (P:HysteresisParam)
    (S:SchedulerParam)
  : Primary =
struct
  type features = float list [@@deriving protobuf {protoc}]
  type pick = int [@@deriving protobuf {protoc}]
  let desc = "hysteresis"

  module M1 = MakeGreedyPrimary(
    struct 
      let desc="1"
      let criteria = fst P.policies
    end)(S)

  module M2 = MakeGreedyPrimary(
    struct 
      let desc="2"
      let criteria = snd P.policies
    end)(S)

  let memory = ref true

  let reorder ~system:s ~now:now ~log:log = 
    let values = List.map 
        (fun (_,f) -> Metrics.get_value (f S.jobs s now 0)) 
        Metrics.features_system
    in let var = List.nth values 12
    in let t1,t2 = P.thresholds
    in let high = max t1 t2
    in let low = min t1 t2
    in let pol1, pol2 = 
         if t1 < t2 then
           M1.reorder, M2.reorder
         else
           M2.reorder, M1.reorder
    in if (not !memory) then  (*if we are on P_low*)
      if var > high then
        (memory := true;
         pol2 ~system:s ~now:now ~log:log)
      else
        (assert(!memory = false);
         pol1 ~system:s ~now:now ~log:log)
    else
    if var < low then (*if we are on P_high*)
      (memory := false;
       pol1 ~system:s ~now:now ~log:log)
    else
      (assert(!memory = true);
       pol2 ~system:s ~now:now ~log:log)
end

module type ContextualParam = sig
  val period : int
  val policies : Metrics.criteria list
  val ipc : string
end

module MakeContextualPrimary
    (P:ContextualParam)
    (S:SchedulerParam)
  : Primary =
struct
  type features = float list [@@deriving protobuf {protoc}]
  type pick = int [@@deriving protobuf {protoc}]
  let desc = "resimulation"
  let context = ZMQ.Context.create ()
  let requester = ZMQ.Socket.create context ZMQ.Socket.req
  let () = ZMQ.Socket.connect requester @@ "ipc://"^P.ipc

  let reorder ~system:s ~now:now ~log:log = 
    let criteria =
      let nn_pick (s:system) (now:int)  = 
        let values = List.map 
            (fun (_,f) -> Metrics.get_value (f S.jobs s now 0)) 
            Metrics.features_system
        in let packet = 
             let e = Protobuf.Encoder.create () 
             in (features_to_protobuf values e; Protobuf.Encoder.to_string e)
        in begin
          ZMQ.Socket.send requester packet;
          let reply = ZMQ.Socket.recv requester 
          in Protobuf.Decoder.decode_exn pick_from_protobuf @@ Bytes.of_string reply
        end
      in List.nth P.policies (nn_pick s now)
    in let lv =
         let process_waiting i = match criteria S.jobs s now i with
           |Value v -> (i,v,[])
           |ValueLog (v,l) -> (i,v,l)
         in List.map process_waiting s.waiting
    in
    lv |> List.fold_left (fun acc (i,_,x) ->
        ((float_of_int now)::(float_of_int i)::x)::acc) log,
    lv |> List.sort (compose_binop Pervasives.compare (fun (_,x,_) -> x))
    |> List.map (fun (x,_,_) -> x)

end

module type OocvxParam = sig
  val period : int
  val policies : Metrics.criteria list
end

module MakeOocvxPrimary
    (P:OocvxParam)
    (S:SchedulerParam)
  : Primary =
struct
  type features = float list
  type pick = int
  let desc = "resimulation"

  let reorder ~system:s ~now:now ~log:log = 
    let criteria =
      let x = List.map 
          (fun (_,f) -> Metrics.get_value (f S.jobs s now 0)) 
          Metrics.features_system
      in List.nth P.policies 1
    in let lv =
         let process_waiting i = match criteria S.jobs s now i with
           |Value v -> (i,v,[])
           |ValueLog (v,l) -> (i,v,l)
         in List.map process_waiting s.waiting
    in
    lv |> List.fold_left (fun acc (i,_,x) ->
        ((float_of_int now)::(float_of_int i)::x)::acc) log,
    lv |> List.sort (compose_binop Pervasives.compare (fun (_,x,_) -> x))
    |> List.map (fun (x,_,_) -> x)

end

(**** Backfilling Selector ****)
module type Secondary = sig
  val pick :
    system:system ->                (*full state of the system BEFORE starting easy*)
    now:int ->                      (*now*)
    backfillable:int list ->        (*backfillable jobs*)
    reservationWait:int ->          (*time of the reservation*)
    reservationID:int ->            (*reservation job*)
    reservationFree:int ->          (*free resources above the reservation job*)
    freeNow:int ->                  (*free resources before backfilling*)
    int list
end

(*list jobs eligible for backfilling*)

module MakeGreedySecondary
    (C:Metrics.Criteria)
    (S:SchedulerParam)
  : Secondary =
struct

  let is_eligible ~freeNow:r ~freeResa:r' ~resaWait:t ~q:q ~p_est:p_est =
    assert (t>0);
    assert (r>=0);
    assert (r'>=0);
    q <= (min r' r) || (q <= r && p_est <= t)

  let crit = C.criteria S.jobs

  let pick
      ~system:s
      ~now:now
      ~backfillable:bfable
      ~reservationWait:restime
      ~reservationID:resjob
      ~reservationFree:free'
      ~freeNow:free =
    let rec picknext f f' picked = function
      |[] -> picked
      |i::is ->
          let j = Hashtbl.find S.jobs i
          in let is_shorter = j.p_est <= restime
          in if (j.q <= (min f f') || (j.q <= f && is_shorter )) then
            picknext
              (f-j.q)
              (f' - (if not is_shorter then j.q else 0))
              (i::picked)
              is
          else
            picknext f f' picked is
    and sorted = List.sort 
        (compose_binop Pervasives.compare (fun x -> -. Metrics.get_value (crit s now x))) 
        bfable
    in picknext free free' [] sorted
end

(************************************ Scheduler ***************************************)
module type Scheduler = sig
  val schedule : int -> System.system -> System.log -> ((int list) * System.log)
end

module MakeEasyScheduler
    (Primary:Primary)
    (Secondary:Secondary)
    (P:SchedulerParam)
  : Scheduler =
struct
  let fj = Hashtbl.find P.jobs

  (*should we attempt to backfill?*)
  type easy =
      Simple of int list      (*no backfilling needed*)
    | Backfill of (int list * (*to start now*)
                   int *      (*remaining resources after*)
                   int *      (*id of job to backfill*)
                   int list)  (*pontentially eligible*)
  let get_easy ~free:free ~waitqueue:wq =
    assert (free >= 0);
    let rec easy decision free = function
      | [] -> Simple decision
      | i :: is  -> let remaining = free-(fj i).q
          in if remaining >= 0 then easy (decision@[i]) remaining is
          else Backfill (decision,free,i,is)
    in easy [] free wq

  let reserve ~now:now ~free:free ~q:needed ~running:running ~decision:decision =
    let rec fits f = function
      | [] -> failwith "impossible to backfill this job. check MaxProcs."
      | (t,i)::tis ->
          let f' = f+ (fj i).q
          in if f' >= needed then
            let resaWait = t-now;
            in ( assert ( resaWait > 0 ); ( resaWait, f'-needed ))
          else
            fits (f') tis
    and projected =
      let sort = (List.sort (fun (t,_) (t',_) -> compare t t'))
      and project = (List.map (fun (t,i) -> (t+ (fj i).p_est,i)))
      in (running @ (List.map (fun i -> (now,i)) decision))
         |> project |> sort
    in fits free projected


  let schedule now s log =
    let log, reordered = Primary.reorder ~system:s ~now:now ~log:log
    in match get_easy s.free reordered with
    | Simple decision                     -> (decision,log)
    | Backfill (decision, _, _, [])       -> (decision,log)
    | Backfill (decision, free, id, rest) ->
        assert (free >= 0);
        let resaWait, resaFree =
          reserve ~now:now ~free:free ~q:(fj id).q ~running:s.running
            ~decision:decision
        in let () = assert (resaWait>0)
        in let () = assert (resaFree>=0)
        in let backfilled =
             Secondary.pick ~system:s ~now:now ~backfillable:rest
               ~reservationWait:resaWait ~reservationID:id ~reservationFree:resaFree
               ~freeNow:free
        in ((decision @ backfilled), log)
end
