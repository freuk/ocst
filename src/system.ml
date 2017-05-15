open Sexplib.Std

(************************************** Jobs *************************************)
type job = { r:int; p:int; p_est:int; q:int; u:int} (*job data*)
type job_table = (int,job) Hashtbl.t                (*id-indexed table*)

(************************************** System ***********************************)
type system =
    { free: int;                             (*free resources*)
      running: (int*int) list;               (*currently running jobs (startT,id)*)
      waiting : int list;                    (*queud jobs (id)*)
    } [@@deriving sexp]

let empty_system maxprocs = { free = maxprocs; running = []; waiting = []; }

(************************************** Criteria Log *****************************)

type log = (float list) list (*list of (id,sub_time)*)

