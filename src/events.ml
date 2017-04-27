open Jobs

type eventType = Submit | Finish

type event = { time : int;
               id : int ;
               eventType : eventType }

module Heap = BatHeap.Make( struct
                              type t = event
                              let compare e1 e2 = compare e1.time e2.time
                            end )

(*apply a function to all events.*)
let iterevents h = List.iter (Heap.to_list h)

(*start a job at a given time and return a new event heap*)
let start_job jobTable time jobId h =
  let j = Jobs.find jobTable jobId
  in Heap.add h {time=time+j.p; id=jobId; eventType=Finish}

(*submit a job and return a new event heap*)
let submit_job jobTable jobId h =
  let j = Jobs.find jobTable jobId
            Heap.add h {time=j.r; id=jobId; eventType=Submit}

(* this type encodes the new heap, the time, and the unloaded job list, if simulation
 * has to continue.*)
type unloadedEvents = Events of (Heap.t * int * ) | EndSimulation

(*unloadEvents gets the next event and all the events at that time.*)
let unloadEvents h =
  in if (Events.Heap.size h = 0) then
    EndSimulation
  else
    let firstEvent = Heap.find_min h
    in let getEvent h eventList =
      if (Events.Heap.size h = 0) then
        Events (h, time, eventList)
      else
        let e = Heap.find_min h
        in if e.time > firstEvent.time then
          Events (h, time, eventList)
        else
          getEvent (Heap.del_min h) (e::eventList)
    in getEvent (Heap.del_min h) [e]
