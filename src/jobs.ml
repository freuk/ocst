type job= { r:int;                         (** Submit Time *)
            p:int;                         (** Run Time *)
            p_est:int;                     (** User Estimated Run Time *)
            q:int;                    (** User Id*)
            u:int}                         (** Required Resources *)
type job_table =  (int,job) Hashtbl.t

let iter = Hashtbl.iter
let find = Hashtbl.find
let add = Hashtbl.add
let new_job_table () = Hashtbl.create 1000

type wait_queue = (int list) ref

let empty_job_waiting_queue () : wait_queue = ref []

let enqueue_job id waitqueue : unit = waitqueue := !waitqueue @ [id]

let dequeue_job id waitqueue = waitqueue := List.filter (fun i -> i!=id) !waitqueue

let is_empty waitqueue = List.length !waitqueue = 0
