open Jobs
open Events
open Resources
open Printf

let print_joblist_scarce (jobs: job_table) (idlist: (int) BatRefList.t) : unit =
  begin
    BatRefList.iter
        (fun i -> let j = find jobs i in printf "(id: %d r: %d) " i j.q )
        idlist;
    printf "%s" "\n"
  end

let print_job jobs id : unit =
  let j = find jobs id in
  begin
    printf "Job of id %d, " id;
    printf "submission time %d, " j.r;
    printf "resource requirement %d, " j.q;
    printf "runtime %d, " j.p;
    printf "user estimated runtime %d.\n" j.p_est
  end

let print_event jobs (e:event) : unit =
  begin
    printf "%s at time %d: "
           ( match e.event_type with
             | Submit -> "Job SUB"
             | End ->  "Job END" )
           e.time;
    print_job jobs e.id;
  end

let print_resourcestate jobs (r:system_state) : unit =
  begin
    printf "Resource state (running list)\n %s" "";
    List.iter (fun (t,i) -> printf "(pet %d,id %d,res %d) " t i (find jobs i).q) r.jobs_running_list;
    printf "\n %s" ""
  end

let print_unloaded_events jobs now event_list =
    (printf "Unloading some events at time %d:\n" now;
     List.iter (print_event jobs) event_list)

let print_scheduler_actions jobs now (decisions:int list) free =
  begin
    printf "SCHEDULING: FCFS time %d, %d available resources. " now free;
    if List.length decisions > 0 then
      begin
        printf "%s" "scheduled jobs: ";
        List.iter
            (fun i -> let j = find jobs i in printf "(id:%d, req:%d) " i j.q)
            decisions
      end
     else
       printf "%s" "no scheduled jobs.";
    printf "%s" "\n"
  end

let print_actions_consequences jobs now decisions resourcestate waitqueue =
  begin
    print_joblist_scarce jobs waitqueue;
    print_scheduler_actions jobs now decisions (currently_free_resources resourcestate);
    List.iter
    (fun i -> Printf.printf "Job START at time %d: " now; print_job jobs i)
    decisions
  end;
