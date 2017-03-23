open Jobs

type event_type = Submit | End

type event = { time: int;
               id: int ;
               event_type: event_type }

module Heap = Binary_heap.Make( struct
                                  type t = event
                                  let compare e1 e2 = - compare e1.time e2.time
                                end )

type event_heap = Heap.t

let empty_event_heap () = Heap.create 1000

let is_empty eventheap = Heap.is_empty eventheap

let iterevents = Heap.iter

let pop_event eventheap =
  Heap.pop_maximum eventheap

let peek_next_time eventheap =
  (Heap.maximum eventheap).time

let is_next_event_at_time now eventheap =
  (not (is_empty eventheap))
  &&
  peek_next_time eventheap == now

let start_job jobs now id eventheap =
  let j= find jobs id in
  Heap.add
  eventheap
  {time=now+j.p; id=id; event_type=End}

let submit_job id time eventheap =
  Heap.add eventheap {time=time; id=id; event_type=Submit}
