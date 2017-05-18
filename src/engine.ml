open System

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
  val simulate : Events.EventHeap.t -> system -> history -> log -> (history * log)
  val simulate_logstates :
    output_list:(string * string * string * string ) list
    -> ?period:int
    -> heap:Events.EventHeap.t
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
    let execute s (e:Events.EventHeap.elem) =
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
      ,Events.EventHeap.insert h {time = now+j.p; id=i; event_type=Finish}
      ,(i,now)::hist
    in List.fold_left f (system,heap,history) jobList

  let simulate (eventheap:Events.EventHeap.t) (system:system) (hist:history) (log:log)=
    (*step h s where h is the event heap and s is the system*)
    let rec step syst heap hist log =
      match Events.EventHeap.unloadEvents heap with
        | Events.EventHeap.EndSimulation -> (hist, log)
        | Events.EventHeap.Events (h, now, eventList) ->
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
    let rec step syst heap hist log last_system_log_time last_heap last_syst outl =
      match Events.EventHeap.unloadEvents heap with
        | Events.EventHeap.EndSimulation -> (hist, log)
        | Events.EventHeap.Events (h, now, eventList) ->
          begin
            if BatList.is_empty outl then (hist,log)
            else
              let outl, last_system_log_time, last_heap, last_syst = match outl with
                |[] -> outl, last_system_log_time, last_heap, last_syst
                |x::xs ->
                    if now-last_system_log_time < period then
                      outl, last_system_log_time, last_heap, last_syst
                    else
                      let out_state,out_now,out_addjobs,out_intervalsum = x
                      in begin
                        Io.print_state P.jobs out_state last_syst;
                        Io.print_now P.jobs out_now last_system_log_time;
                        Io.print_addjobs P.jobs out_addjobs last_syst;
                        Io.print_intervalsub P.jobs last_heap period out_intervalsum last_system_log_time period;
                        (xs,now,heap,syst)
                      end
              in let s = executeEvents ~eventList:eventList syst
              in let decisions, log = Sch.schedule now s log
              in let s,h,hi = (executeDecisions s h now decisions hist)
              in step s h hi log last_system_log_time last_heap last_syst outl
          end
    in step system eventheap history log (Events.EventHeap.find_min eventheap).time eventheap system outputl
end

(************************************** Stats ************************************)
