open Metrics
open Engine

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
  max_procs : int;
  stats : (module Statistics.Stat) list;
  debug : bool}
let copts swf_in swf_out initial_state max_procs debug seed stats =
  Random.init seed;
  {swf_in; swf_out; initial_state; max_procs; debug; stats}

let run_simulator ?log_out:(log_out=None) copts reservation backfill job_table max_procs=
  let max_procs,h,s =
    let real_mp = max max_procs copts.max_procs
    in real_mp,Engine.EventHeap.of_job_table job_table,System.emptySystemState real_mp
  in let module SchedulerParam = struct let jobs = job_table end
  in let module CSec = (val backfill:Metrics.Criteria)
  in let module Primary = (val reservation:Easy.Primary)
  in let module Secondary = Easy.MakeGreedySecondary(CSec)(SchedulerParam)
  in let module Scheduler = Easy.MakeEasyScheduler(Primary)(Secondary)(SchedulerParam)
  in let module S =
    Engine.MakeSimulator(Scheduler)(struct include SchedulerParam end)
  in let hist,log =(S.simulate h s [] [])
  in (Io.hist_to_swf job_table copts.swf_out hist;
      Io.log_to_file log_out Primary.desc log;
      let f s =
        let module M = (val s:Statistics.Stat)
        in M.stat
      in let stv = List.map (fun s -> (f s) job_table hist) copts.stats
      in let sts = String.concat "," (List.map (Printf.sprintf "%0.3f") stv)
      in Printf.printf "%s" sts)

let fixed copts reservation backfill =
  let jt,mp = Io.parse_jobs copts.swf_in
  in let module M = Easy.MakeGreedyPrimary((val reservation:Metrics.Criteria))(struct let jobs = jt end)
  in run_simulator copts (module M:Easy.Primary) backfill jt mp

let mixed copts backfill feature_out alpha alpha_threshold alpha_poly alpha_system proba sampling =
  let jt,mp = Io.parse_jobs copts.swf_in
  in if proba then
    let module Pc =struct
            let sampling = sampling
            let criterias = 
              let f ftlist (param:float list * float list * float list) : Metrics.criteria list = List.map snd ftlist
              in [ BatOption.map (f features_job_plus)               alpha;
                   BatOption.map (f features_job_threshold) alpha_threshold;
                   BatOption.map (f features_job_advanced) alpha_poly;
                   BatOption.map (f features_system_job) alpha_system;]
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
    in let module M = Easy.MakeProbabilisticPrimary(Pc)(struct let jobs=jt end)
    in run_simulator copts (module M:Easy.Primary) backfill jt mp
  else
    let m =
      [ BatOption.map (Metrics.makeMixed features_job_plus)               alpha;
        BatOption.map (Metrics.makeMixed features_job_threshold) alpha_threshold;
        BatOption.map (Metrics.makeMixed features_job_advanced) alpha_poly;
        BatOption.map (Metrics.makeMixed features_system_job) alpha_system;]
        |> List.filter BatOption.is_some
      |> List.map BatOption.get
      |> BatList.reduce Metrics.makeSum
    in let module P = (val m:Metrics.Criteria)
    in let module M = Easy.MakeGreedyPrimary(P)(struct let jobs=jt end)
    in run_simulator ~log_out:feature_out copts (module M:Easy.Primary) backfill jt mp

let contextual copts period perf_out policies=
  let jt,mp = Io.parse_jobs copts.swf_in
  and getcrit m =
    let module M = (val m:Metrics.Criteria)
    in M.criteria
  in let module P = 
    struct
      let period = period
      let policies = List.map getcrit policies 
    end
  in let module M = Easy.MakePeriodPrimary(P)(struct let jobs = jt end)
  in let mbackfill = (module Metrics.FCFS:Metrics.Criteria)
  in run_simulator ~log_out:perf_out copts (module M:Easy.Primary) (module Metrics.FCFS:Criteria) jt mp
