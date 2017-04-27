open Easy
(************************************** Jobs *************************************)
type job = { r:int; p:int; p_est:int; q:int} (*job data*)
type jobTable = (int,job) Hashtbl.t          (*id-indexed table*)

(************************************** System ***********************************)
type system =
    { free: int;                             (*free resources*)
      running: (int*int) list;               (*currently running jobs (startT,id)*)
      waiting : int list;                    (*queud jobs (id)*)
    }

let emptySystemState maxprocs = { free = maxprocs; running = []; waitQueue = []; }

(************************************** Events ***********************************)
type eventType = Submit | Finish
type event = { time : int;
               id : int ;
               eventType : eventType }
module EventHeap = BatHeap.Make( struct
                                   type t = event
                                   let compare e1 e2 = compare e1.time e2.time
                                 end )
type unloadedEvents = Events of (EventHeap.t * int * (event list)) | EndSimulation
let unloadEvents h =
  in if (EventHeap.size h = 0) then
    EndSimulation
  else
    let firstEvent = EventHeap.find_min h
    in let getEvent h eventList =
      if (EventHeap.size h = 0) then
        Events (h, time, eventList)
      else
        let e = EventHeap.find_min h
        in if e.time > firstEvent.time then
          Events (h, time, eventList)
        else
          getEvent (EventHeap.del_min h) (e::eventList)
    in getEvent (EventHeap.del_min h) [e]

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

(*TODO refactor. Hook module*)
module type Hook = sig
  val hook : int -> unit
end
module NoHook = struct
  let hook _ = ()
end
(*END TODO*)

module type SimulatorParam = sig
  val output_channel : out_channel option
  val output_channel_bf : out_channel option
  val jobs : jobTable
end

(*simulator module*)
module type Simulator = sig
  type outputStat
  val simulate : EventHeap.t -> system -> unit
end

(*simulator building functor*)
module MakeSimulator
  (St:Stat) (*TODO refactor*)
  (Sch:Scheduler)
  (P:SimulatorParam)
  (Hk:Hook) (*TODO refactor*)
  :Simulator with type outputStat = St.outputStat = struct
    type outputStat = St.outputStat

    (*apply some events' effect on the system; this can be optimized.*)
    let executeEvents system eventList =
      let execute e s =
        match e.event_type with
          | Submit -> { s with waitQueue = i::s.waitQueue }
          | End ->
              let q = ( S.jobTable e.id).q
              in { s with
                       running = List.filter (fun (_,i) -> i!=e.id) s.running;
                       free = s.free + q; }
      in List.fold_left execute system eventList

    (*apply some scheduling decisions on the system and the event heap.*)
    let executeDecisions s h now idList =
      let jobList = List.map (fun i ->  S.jobTable i) idList
      in
        {
          free = (let sumLoad = List.fold_left (acc j -> acc + j.q) jobList
                  in s.free - sumLoad);
          running =
            (let nr = List.map2 (fun j i -> (j.p_est+now,i)) jobList idList
             in s.running @ nr) ;
          waiting = List.filter (fun x -> not (List.mem x idList)) s.waiting;
        },
        let eventList =
          let f j i = {time=time+j.p; id=i; eventType=Finish}
          in List.map2 f jobList idList
        in (EventHeap.merge h (EventHeap.of_list eventList))

    let simulate (eventheap:EventHeap.t) (system:system) =
      (*step h s where h is the event heap and s is the system*)
      let step h s =
        match unloadEvents () with
          | EndSimulation -> ()
          | Events h, now, eventList ->
              let s = executeEvents eventList
              in let decisions = Sch.schedule now s
              in let () = St.incStat now decisions
              in let s, h = executeDecisions s h now decisions
              in let () = Hk.hook now;
              in step h s
      in step eventheap system
  end

    (*let executeDecisions system h now decisions =*)
    (*let execute acc id =*)
    (*let s,h = acc*)
    (*in let j = (Jobs.find S.jobTable id)*)
    (*and waiting = List.filter (fun i -> i!=id) s.waiting;*)
    (*in let free = s.free - j.q*)
    (*and running = s.running @ [(j.p_est+now,id)]*)
    (*in let h =  Heap.add h {time=time+j.p; id=id; eventType=Finish}*)
    (*in BatOption.may (job_to_swf_row now S.jobs id) P.output_channel;*)
    (*in List.fold_left execute (system ,h) decisions*)
