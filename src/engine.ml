open System
(*open Easy*)
(************************************** Events ***********************************)
module EventHeap = struct
  type event_type = Submit | Finish

  module OrderedEvents =
  struct
    type t = { time : int;
               id : int ;
               event_type : event_type }
    let compare e1 e2 = compare e1.time e2.time
  end

  include BatHeap.Make (OrderedEvents)

  let of_job_table job_table =
    let f i j h  = add ({time=j.r; id=i; event_type=Submit}:elem) h
    in Hashtbl.fold f job_table empty

  type unloadedEvents = Events of (t * int * (elem list)) | EndSimulation

  let unloadEvents h =
    if (size h = 0) then
      EndSimulation
    else
      let firstEvent = find_min h
      in let rec getEvent h eventList =
        if (size h = 0) then
          Events (h, firstEvent.time, eventList)
        else
          let e = find_min h
          in if e.time > firstEvent.time then
            Events (h, firstEvent.time, eventList)
          else
            getEvent (del_min h) (e::eventList)
      in getEvent (del_min h) [firstEvent]
end

(************************************** Engine ***********************************)
(*TODO refactor. Statistics module*)
module type Stat = sig
  type outputStat
  val incStat : int   (* time now*)
  -> int list         (*actual jobs to start now*)
  -> unit
  val getStat : unit -> outputStat
  val getN : unit -> int
  val reset : unit -> unit
end
(*END TODO*)

module type SimulatorParam = sig
  val output_channel : out_channel option
  val output_channel_bf : out_channel option
  val jobs : job_table
end

(*simulator module*)
module type Simulator = sig
  val simulate : EventHeap.t -> system -> unit
end

(*simulator building functor*)
module MakeSimulator
  (Sch:Easy.Scheduler)
  (P:SimulatorParam)
  :Simulator =
struct
  (*apply some events' effect on the system; this can be optimized.*)
  let executeEvents ~system:system ~eventList:el : system=
    let execute s (e:EventHeap.elem) =
      match e.event_type with
        | Submit -> { s with waiting = e.id::s.waiting }
        | Finish ->
            let q = (Hashtbl.find P.jobs e.id).q
            in { running = List.filter (fun (_,i) -> i!=e.id) s.running;
                 free = s.free + q;
                 waiting=s.waiting ; }
    in List.fold_left execute system el

  (*apply some scheduling decisions on the system and the event heap.*)
  let executeDecisions s h now idList =
    let jobList = List.map (fun i -> Hashtbl.find P.jobs i) idList
    in
      {
        free = s.free - (List.fold_left (fun acc j -> acc + j.q) 0 jobList);
        running = s.running @ (List.map (fun i -> (now,i)) idList);
        waiting = List.filter (fun x -> not (List.mem x idList)) s.waiting;
      },
      let eventList =
        let f j i : EventHeap.elem = {time=now+j.p; id=i; event_type=Finish}
        in List.map2 f jobList idList
      in (EventHeap.merge h (EventHeap.of_list eventList))

  let simulate (eventheap:EventHeap.t) (system:system) =
    (*step h s where h is the event heap and s is the system*)
    let rec step h s =
      match EventHeap.unloadEvents h with
        | EventHeap.EndSimulation -> ()
        | EventHeap.Events (h, now, eventList) ->
            let s = executeEvents ~system:s ~eventList:eventList
            in let decisions = Sch.schedule now s
            in let s, h = executeDecisions s h now decisions
            in step h s
    in step eventheap system
end

    (*let executeDecisions system h now decisions =*)
    (*let execute acc id =*)
    (*let s,h = acc*)
    (*in let j = (Jobs.find S.job_table id)*)
    (*and waiting = List.filter (fun i -> i!=id) s.waiting;*)
    (*in let free = s.free - j.q*)
    (*and running = s.running @ [(j.p_est+now,id)]*)
    (*in let h =  Heap.add h {time=time+j.p; id=id; event_type=Finish}*)
    (*in BatOption.may (job_to_swf_row now S.jobs id) P.output_channel;*)
    (*in List.fold_left execute (system ,h) decisions*)




(************************************** Stats ************************************)

(*module StatWait (P:Easy.SchedulerParam) : Stat*)
                   (*with type outputStat = float =*)
(*struct*)
  (*type outputStat = float*)
  (*module M = MakeWaitAccumulator(SystemParam)*)
  (*let n = ref 0*)
  (*let m = ref 0*)
  (*let getN () = !n*)
  (*let getStat () = !m*)
  (*let incStat t jl =*)
    (*(m:= List.fold_left (fun acc j -> acc + Metrics.CriteriaWait.criteria P.jobs now i) !m jl;*)
     (*n:= !n + (List.length jl))*)
  (*let reset () =  (n := 0; m:=0 )*)
(*end*)

(*module type StatMetricSig = sig*)
  (*val add : int -> int list -> unit*)
  (*val get : unit -> float*)
  (*val reset : unit -> unit*)
(*end*)

