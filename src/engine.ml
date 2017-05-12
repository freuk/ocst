open System
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

  let unloadEvents heap =
    if (size heap = 0) then
      EndSimulation
    else
      let firstEvent = find_min heap
      in let rec getEvent h eventList =
        if (size h = 0) then
          Events (h, firstEvent.time, eventList)
        else
          let e = find_min h
          in if e.time > firstEvent.time then
            Events (h, firstEvent.time, eventList)
          else
            getEvent (del_min h) (e::eventList)
      in getEvent (del_min heap) [firstEvent]
end

(************************************** History **********************************)

type history = (int*int) list (*list of (id,sub_time)*)


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
  val jobs : job_table
end

(*simulator module*)
module type Simulator = sig
  val simulate : EventHeap.t -> system -> history -> log -> (history * log)
  val simulate_logstates :
    output_list:'a list
    -> ?period:int
    -> heap:EventHeap.t
    -> system:system
    -> history:history
    -> log:log
    -> (history * log)
end

(*simulator building functor*)
module MakeSimulator
  (Sch:Easy.Scheduler)
  (P:SimulatorParam)
  :Simulator =
struct
  (*apply some events' effect on the system; this can be optimized.*)
  let executeEvents ~eventList:el system=
    let execute s (e:EventHeap.elem) =
      match e.event_type with
        | Submit -> { s with waiting = e.id::s.waiting }
        | Finish ->
            let q = (Hashtbl.find P.jobs e.id).q
            in { running = List.filter (fun (_,i) -> i!=e.id) s.running;
                 free = s.free + q;
                 waiting=s.waiting; }
    in List.fold_left execute system el

  (*apply some scheduling decisions on the system and the event heap.*)
  let executeDecisions system heap now idList history =
    let jobList = List.map (fun i -> (i,Hashtbl.find P.jobs i)) idList
    in let f (s,h,hist) (i,j) =
      { free = s.free - j.q;
        running = (now,i)::s.running;
        waiting = List.filter (fun x -> not (i=x)) s.waiting;
      }
      ,EventHeap.insert h {time = now+j.p; id=i; event_type=Finish}
      ,(i,now)::hist
    in List.fold_left f (system,heap,history) jobList

  let simulate (eventheap:EventHeap.t) (system:system) (hist:history) (log:log)=
    (*step h s where h is the event heap and s is the system*)
    let rec step syst heap hist log =
      match EventHeap.unloadEvents heap with
        | EventHeap.EndSimulation -> (hist, log)
        | EventHeap.Events (h, now, eventList) ->
            let s = executeEvents ~eventList:eventList syst
            in let decisions, log = Sch.schedule now s log
            in let s,h,hi = (executeDecisions s h now decisions hist)
            in step s h hi log
    in step system eventheap hist log

  let simulate_logstates
    ~output_list:outputl
    ?period:(period=86400)
    ~heap:eventheap
    ~system:system
    ~history:history
    ~log:log =
    (*step h s where h is the event heap and s is the system*)
    let rec step syst heap hist log last_system_log_time last_heap outl =
      match EventHeap.unloadEvents heap with
        | EventHeap.EndSimulation -> (hist, log)
        | EventHeap.Events (h, now, eventList) ->
          begin
            let outl, last_system_log_time, last_heap = match outl with
              |[] -> outl, last_system_log_time, last_heap
              |x::xs ->
                  if now-last_system_log_time < period then
                    outl, last_system_log_time, last_heap
                  else
                    let out_state,out_addjobs,out_intervalsum = x
                    in begin
                      Io.print_state P.jobs out_state syst;
                      Io.print_addjobs P.jobs out_addjobs syst;
                      Io.print_intervalsub P.jobs last_heap period out_intervalsum;
                      (xs,now,heap)
                    end
            in let s = executeEvents ~eventList:eventList syst
            in let decisions, log = Sch.schedule now s log
            in let s,h,hi = (executeDecisions s h now decisions hist)
            in step s h hi log last_system_log_time last_heap outl
          end
    in step system eventheap history log (EventHeap.find_min eventheap).time outputl
end

(************************************** Stats ************************************)
