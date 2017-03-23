open Jobs

type step = int * int
type system_state = { maxprocs: int;
                      mutable free: int;
                      mutable jobs_running_list: (int*int) list }

let empty_resources maxprocs : system_state =
  { maxprocs = maxprocs;
    free = maxprocs;
    jobs_running_list = []}

let copy resourcestate =
  { maxprocs = resourcestate.maxprocs;
  free = resourcestate.free;
  jobs_running_list = resourcestate.jobs_running_list }

let free_resources jobs id resourcestate =
  let j = (find jobs id) in
  resourcestate.jobs_running_list <- List.filter (fun (_, i) -> i!=id) resourcestate.jobs_running_list;
  resourcestate.free <- resourcestate.free + j.q

let use_resources jobs id now resourcestate =
  let j = (find jobs id) in
  resourcestate.jobs_running_list <- resourcestate.jobs_running_list @ [(j.p_est+now,id)];
  resourcestate.free <- resourcestate.free - j.q

let currently_free_resources resourcestate =
  resourcestate.free
