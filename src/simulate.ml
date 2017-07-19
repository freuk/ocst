open Metrics

let timesched f x =
  let () = Printf.printf "%s\n" "Simulating.."
  in let start = Unix.gettimeofday ()
  in let res = f x
  in let stop = Unix.gettimeofday ()
  in let () = Printf.printf "Done. Simulation time: %fs\n%!" (stop -. start)
  in res

type copts = {
  swf_in: string;
  swf_out : string option;
  initial_state : string option;
  additional_jobs : string option;
  max_procs : int;
  stats : (module Statistics.Stat) list;
  debug : bool}
let copts swf_in swf_out initial_state additional_jobs max_procs debug seed stats =
  Random.init seed;
  {swf_in; swf_out; initial_state; additional_jobs; max_procs; debug; stats}

let run_simulator ?period:(period=86400) ?state_out:(state_out = None) ?log_out:(log_out=None) copts reservation backfill job_table max_procs=
  let h,s =
    let heap_before = Events.EventHeap.of_job_table job_table
    in let () = match copts.additional_jobs with
      |None -> ()
      |Some fn -> let jt,_ = Io.parse_jobs fn
    in Hashtbl.iter (fun i j -> Hashtbl.add job_table i j) jt;
      in let s,h = match copts.initial_state with
      |None ->
          let real_mp = max max_procs copts.max_procs
          in System.empty_system real_mp,heap_before
      |Some fn ->
          let s = Io.load_system fn
            in let events =
              let make_event (t,i) : Events.EventHeap.elem=
                { time=(Hashtbl.find job_table i).p+t;
                  id=i;
                  event_type=Events.EventHeap.Finish}
          in List.map make_event s.running
              in let h = List.fold_left Events.EventHeap.insert heap_before events
            in s,h
              in h,s
      in let module SchedulerParam = struct let jobs = job_table end
  in let module CSec = (val backfill:Metrics.Criteria)
      in let module Primary = (val reservation:Easy.Primary)
  in let module Secondary = Easy.MakeGreedySecondary(CSec)(SchedulerParam)
      in let module Scheduler = Easy.MakeEasyScheduler(Primary)(Secondary)(SchedulerParam)
  in let module S =
    Engine.MakeSimulator(Scheduler)(struct include SchedulerParam end)
      in let hist,log = match state_out with
    |Some s_out -> (S.simulate_logstates ~output_list:s_out ~period:period ~heap:h ~system:s ~history:[] ~log:[])
    |None -> (S.simulate h s [] [])
  in (Io.hist_to_swf job_table copts.swf_out hist;
      Io.log_to_file log_out Primary.desc log;
      let f s =
        let module M = (val s:Statistics.Stat)
        in M.stat
        in let stv = List.map (fun s -> (f s) job_table hist) copts.stats
      in let sts = String.concat "," (List.map (Printf.sprintf "%0.3f") stv)
        in Printf.printf "%s" sts)

let fixed copts reservation backfill threshold =
  let jt,mp = Io.parse_jobs copts.swf_in
  in let module T = struct let threshold = threshold end 
  in let module M = Easy.MakeThreshold(T)(Easy.MakeGreedyPrimary((val reservation:Metrics.Criteria))(struct let jobs = jt end))(struct let jobs = jt end)
  in run_simulator copts (module M:Easy.Primary) backfill jt mp

let mixed copts backfill feature_out alpha alpha_threshold alpha_poly alpha_system proba sampling threshold=
  let jt,mp = Io.parse_jobs copts.swf_in
  in if proba then
    let module Pc
    = struct
      let sampling = sampling
      let criterias =
        let f ftlist (param:float list * float list * float list) : Metrics.criteria list = List.map snd ftlist
          in [ BatOption.map (f features_job_plus)                alpha;
               BatOption.map (f features_job_threshold) alpha_threshold;
               BatOption.map (f features_job_advanced)       alpha_poly;
               BatOption.map (f features_system_job)       alpha_system;]
    |> List.filter BatOption.is_some
      |> List.map BatOption.get
      |> BatList.reduce List.append
      let alpha =
        [alpha;alpha_threshold;alpha_poly;alpha_system]
    |> List.filter BatOption.is_some
      |> List.map BatOption.get
      |> List.map BatTuple.Tuple3.first
      |> BatList.reduce List.append
  end
        in let module T = struct let threshold = threshold end 
        in let module M = Easy.MakeThreshold(T)(Easy.MakeProbabilisticPrimary(Pc)(struct let jobs=jt end))(struct let jobs=jt end)
      in run_simulator copts (module M:Easy.Primary) backfill jt mp
  else
    let m = [ BatOption.map (Metrics.makeMixed features_job_plus)               alpha;
              BatOption.map (Metrics.makeMixed features_job_threshold) alpha_threshold;
              BatOption.map (Metrics.makeMixed features_job_advanced) alpha_poly;
              BatOption.map (Metrics.makeMixed features_system_job) alpha_system;]
    |> List.filter BatOption.is_some
      |> List.map BatOption.get
      |> BatList.reduce Metrics.makeSum
        in let module P = (val m:Metrics.Criteria)
        in let module T = struct let threshold = threshold end 
        in let module M = Easy.MakeThreshold(T)(Easy.MakeGreedyPrimary(P)(struct let jobs=jt end))(struct let jobs=jt end)
        in run_simulator ~log_out:feature_out copts (module M:Easy.Primary) backfill jt mp

let contextual copts period policies ipc=
  let getcrit m =
    let module M = (val m:Metrics.Criteria)
    in M.criteria
  and getdesc m =
    let module M = (val m:Metrics.Criteria)
    in M.desc
  (*in let () = List.iter (fun x -> Printf.printf "%s" @@ getdesc x) policies*)
  in let jt,mp = Io.parse_jobs copts.swf_in
  in let module P =
      struct
        let period = period
        let policies = List.map getcrit policies
        let ipc = "/tmp/"^ipc^".ipc"
    end
  in let module M = Easy.MakeContextualPrimary(P)(struct let jobs = jt end)
  in let mbackfill = (module Metrics.FCFS:Metrics.Criteria)
  in run_simulator copts (module M:Easy.Primary) (module Metrics.FCFS:Criteria) jt mp

let hysteresis copts (thresholds: float*float) policies=
  let getcrit m =
    let module M = (val m:Metrics.Criteria)
    in M.criteria
  and jt,mp = Io.parse_jobs copts.swf_in
  and t1,t2 = thresholds
  in let module P =
    struct
      let thresholds = 10000. *. (abs_float t1) , 10000. *. (abs_float t2)
      let policies = (getcrit (fst policies),getcrit (snd policies))
    end
  in let module M = Easy.MakeHysteresisPrimary(P)(struct let jobs = jt end)
  in let mbackfill = (module Metrics.FCFS:Metrics.Criteria)
  in run_simulator copts (module M:Easy.Primary) (module Metrics.FCFS:Criteria) jt mp


let printstate copts period state_out now_out additional_out swfin_out=
  let jt,mp = Io.parse_jobs copts.swf_in
  in let l =
    let l = BatList.map2 (fun x y -> (x,y)) state_out now_out
    in let l = BatList.map2 (fun (x,y) z -> (x,y,z)) l additional_out
    in BatList.map2 (fun (x,y,z) z' -> (x,y,z,z')) l swfin_out
    in let mbackfill = (module Metrics.FCFS:Metrics.Criteria)
  in let module M = Easy.MakeGreedyPrimary(Metrics.FCFS)(struct let jobs = jt end)
    in run_simulator ~state_out:(Some l) copts (module M:Easy.Primary) mbackfill jt mp
