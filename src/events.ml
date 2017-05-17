open System

(************************************** Events ***********************************)
module EventHeap = struct
  type event_type = Submit | Finish [@@deriving show]
  type event = { time : int;
             id : int ;
             event_type : event_type } [@@deriving show]

  module OrderedEvents =
  struct
    type t = event
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
