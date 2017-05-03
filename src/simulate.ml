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
  max_procs : int;
  stats : (module Statistics.Stat) list;
  debug : bool}
let copts swf_in swf_out max_procs debug seed stats =
  Random.init seed;
  {swf_in; swf_out; max_procs; debug; stats}

let fixed ~criteria_out:log_fn copts reservation backfill =
  let job_table,max_procs,h,s =
    let jt,mp = Io.parse_jobs copts.swf_in
    in let real_mp = max mp copts.max_procs
    in jt,real_mp,Engine.EventHeap.of_job_table jt,System.emptySystemState real_mp
  in let module SchedulerParam = struct let jobs = job_table end
  in let module CPrim = (val reservation:Metrics.Criteria)
  in let module Primary = Easy.MakeGreedyPrimary(CPrim)(SchedulerParam)
  in let module CSec = (val backfill:Metrics.Criteria)
  in let module Secondary = Easy.MakeGreedySecondary(CSec)(SchedulerParam)
  in let module Scheduler = Easy.MakeEasyScheduler(Primary)(Secondary)(SchedulerParam)
  in let module S =
    Engine.MakeSimulator(Scheduler)(struct include SchedulerParam end)
  in let hist,log =(S.simulate h s [] Easy.empty_log)
  in (Io.hist_to_swf job_table copts.swf_out hist;
      let f s =
        let module M = (val s:Statistics.Stat)
        in M.stat
      in let stv = List.map (fun s -> (f s) job_table hist) copts.stats
      in let sts = String.concat "," (List.map (Printf.sprintf "%0.3f") stv)
      in Printf.printf "%s" sts)

let mixed copts backfill feature_out alpha alpha_poly alpha_system =
  (*generate 'reservation'*)
  let m =
    [ BatOption.map Metrics.makeBasicMixed alpha;
      BatOption.map Metrics.makePolynomialMixed alpha_poly;
      BatOption.map Metrics.makeSystemMixed alpha_system;]
      |> List.filter BatOption.is_some
    |> List.map BatOption.get
    |> BatList.reduce Metrics.makeSum
  in fixed ~criteria_out:feature_out copts m backfill

(*type mixingType = Probabilistic | Scorebased*)
(*let mixingList = *)
    (*[("prob", Probabilistic);*)
    (*("score", Scorebased);]*)

(*let mixed copts alpha backfill objective threshold mixType=*)
  (*let getmodule systemmodule = begin*)

   (*let module O = (val objective:CriteriaSig)*)

   (*in let module T = struct let threshold = threshold end*)

   (*in let module MixParam = struct*)
     (*let alpha = alpha*)
   (*end*)

   (*in let  module CritBackfill = MakeThresholdedCriteria(T)(O)((val backfill:CriteriaSig))*)
   (*in let module SP = (val systemmodule : SystemParamSig)*)
   (*in let module CritReserve = MakeThresholdedCriteria(T)(O)(MakeMixedMetric(MixParam)(SP))*)
   (*in (module MakeEasyGreedy(CritReserve)(CritBackfill)(SP):SchedulerSig)*)

  (*end*)
  (*in simulator_boilerplate getmodule copts*)





(*let threshold copts reservation backfill objective threshold=*)
  (*let getmodule systemmodule = begin*)

   (*let module O = (val objective:CriteriaSig)*)

   (*in let module T = struct let threshold= threshold end*)

   (*in let module CritReserve = MakeThresholdedCriteria(T)(O)((val reservation:CriteriaSig))*)
   (*in let  module CritBackfill = MakeThresholdedCriteria(T)(O)((val backfill:CriteriaSig))*)

   (*in let () = if copts.debug then*)
     (*begin*)
       (*Printf.printf "Running the EASY scheduler on input workload %s using:\n" copts.swf_in;*)
       (*Printf.printf "Running the SARSA scheduler on input workload %s using backfiller %s.\n" copts.swf_in (CritBackfill.desc);*)
       (*Printf.printf "Reservation criteria: %s\n" (CritReserve.desc);*)
       (*Printf.printf "Backfilling criteria: %s\n" (CritBackfill.desc);*)
     (*end;*)


   (*in let module SP = (val systemmodule : SystemParamSig)*)
   (*in (module MakeEasyGreedy(CritReserve)(CritBackfill)(SP):SchedulerSig)*)
  (*end*)
  (*in simulator_boilerplate getmodule copts*)







(*let randomBandit copts period backfill threshold policies=*)
  (*let getmodule systemmodule = begin*)

   (*let module CritBackfill = (val backfill:CriteriaSig)*)

   (*in let () = if copts.debug then*)
     (*begin*)
       (*Printf.printf "Running the EASY scheduler on input workload %s using:\n" copts.swf_in;*)
       (*Printf.printf "Backfilling criteria: %s\n" (CritBackfill.desc);*)
     (*end;*)

   (*in let module SP = (val systemmodule : SystemParamSig)*)
   (*in let module PP = struct *)
     (*let period=period *)
     (*let policyList = List.map (threshold_wait_criteria threshold) policies*)
   (*end*)

   (*in let module CritReserve = MakeRandomBanditSelector(PP)(SP)*)

   (*in (module MakeEasyScheduler(CritReserve)(MakeGreedySelector(CritBackfill)(SP))(SP): SchedulerSig)*)

  (*end*)
  (*in simulator_boilerplate getmodule copts*)







(*let bandit copts explo rewardType period backfill threshold policies reset_out clv noisy select_out clvOut=*)
  (*begin*)
   (*let module CritBackfill = (val backfill:CriteriaSig)*)

   (*in let () = if copts.debug then*)
   (*begin*)
     (*Printf.printf "Running the SARSA scheduler on input workload %s using backfiller %s.\n" copts.swf_in (CritBackfill.desc);*)
     (*BatOption.may (fun x-> Printf.printf "Writing result on output file %s\n" x) copts.swf_out;*)
     (*BatOption.may (fun x-> Printf.printf "Writing backfill data on output file %s\n" x) copts.backfill_out*)
   (*end;*)

   (*in let job_table,max_procs = Io.parse_jobs copts.swf_in*)

   (*in let oc,ocb,ocr,ocs,occ = *)
     (*begin*)
       (*if copts.debug then Printf.printf "%s \n" "Opening channels..";*)
       (*let oc = BatOption.map open_out copts.swf_out*)
       (*and ocb =  BatOption.map open_out copts.backfill_out*)
       (*and ocr =  BatOption.map open_out reset_out *)
       (*and ocs =  BatOption.map open_out select_out *)
       (*and occ =  BatOption.map open_out clvOut*)
       (*in (if copts.debug then Printf.printf "%s \n" "Done"; oc,ocb,ocr,ocs,occ)*)
     (*end*)

   (*in try*)
     (*let module SimulatorParam = struct*)
        (*let eventheap =*)
         (*let h = empty_event_heap ()*)
         (*in let () = iter (fun id j -> submit_job id j.r h) job_table*)
         (*in h*)
        (*let output_channel = oc*)
        (*let output_channel_bf = ocb*)
     (*end*)

     (*in let module SystemParam = struct*)
        (*let waitqueue = empty_job_waiting_queue ()*)
        (*let resourcestate = empty_resources (max !max_procs copts.max_procs)*)
        (*let jobs = job_table*)
     (*end*)

     (*in let bheap = empty_event_heap ()*)

     (*in let module StatWait : StatSig*)
     (*with type outputStat=float*)
     (*= struct*)
       (*type outputStat = float*)
       (*module M = MakeWaitAccumulator(SystemParam)*)
       (*let n = ref 0*)
       (*let getN () = !n*)
       (*let getStat = M.get*)
       (*let incStat t jl = begin*)
         (*M.add t jl;*)
         (*List.iter (fun i -> Events.submit_job i (Jobs.find job_table i).r bheap) jl;*)
         (*n:=!n+(List.length jl)*)
       (*end*)
       (*let reset () =  (n := 0; M.reset() )*)
     (*end*)

     (*in let cr = if clv then*)
       (*let module BSP = struct*)
         (*let period = period*)
         (*let jobHeap = bheap*)
         (*let policyList = List.map (threshold_wait_criteria threshold) policies*)
         (*let out_select = ocs*)
         (*let noise = noisy*)
         (*let outClv = occ*)
       (*end*)
       (*in (module MakeSimulationSelector(BSP)(StatWait)(SystemParam):ReservationSelector)*)
     (*else *)
       (*let module BSP = struct*)
         (*let rate = explo*)
         (*let rewardType = rewardType*)
         (*let period = period*)
         (*let jobHeap = bheap*)
         (*let policyList = List.map (threshold_wait_criteria threshold) policies*)
         (*let out_reset = ocr*)
         (*let out_select = ocs*)
       (*end*)
       (*[>in let module B = (val (bandit_module (List.length BSP.policyList) explo algo):RangedBandit)<]*)
       (*in (module MakeBanditSelector(BSP)(StatWait)(SystemParam):ReservationSelector)*)

     (*in let module Scheduler = MakeEasyScheduler((val cr:ReservationSelector))(MakeGreedySelector(CritBackfill)(SystemParam))(SystemParam)*)

     (*in let module S = MakeSimulator(StatWait)(Scheduler)(SimulatorParam)(SystemParam)(NoHook)*)

     (*in if copts.debug then*)
       (*(timesched S.simulate () ;*)
       (*Printf.printf "avgwait %f \n" (StatWait.getStat () /. float_of_int (StatWait.getN ())))*)
     (*else*)
       (*S.simulate ();*)

     (*if copts.debug then Printf.printf "%s \n" "Closing channels..";*)
     (*BatOption.may close_out oc;*)
     (*BatOption.may close_out ocb;*)
     (*if copts.debug then Printf.printf "%s \n" "Done"*)
   (*with e ->*)
     (*begin*)
       (*BatOption.may close_out_noerr oc;*)
       (*BatOption.may close_out_noerr ocb;*)
       (*BatOption.may close_out_noerr occ;*)
       (*raise e;*)
     (*end*)
  (*end*)





(*type banditalg = Exp3 | Ucb1 | HorizonExp | ExpGreedy | ExpGreedyDecay*)
(*let bandit_encoding = [("exp",Exp3);("ucb",Ucb1);("horizonexp",HorizonExp);("expgreedy",ExpGreedy);("expgreedy-decay",ExpGreedyDecay)]*)
(*let bandit_module k h1 bt =*)
  (*let module KP = struct*)
    (*let k = k*)
  (*end*)
  (*in let module KHP = struct*)
    (*let k = k*)
    (*let n = int_of_float h1*)
  (*end*)
  (*in let module Egp = struct*)
    (*let k = k*)
    (*let epsilon = h1*)
  (*end*)
  (*in let module Egpd = struct*)
    (*let k = k*)
    (*let rate t = h1 /. (float_of_int t)*)
  (*end*)
  (*in let module Rp = struct*)
    (*let lower= -. 40000.*)
    (*let upper= 0.*)
  (*end*)
  (*in match bt with*)
  (*|Exp3 -> (module WrapRange(Rp)(MakeDecayingExp3(KP)):RangedBandit)*)
  (*|Ucb1 -> (module WrapRange(Rp)(MakeUCB1(KP)):RangedBandit)*)
  (*|HorizonExp -> (module WrapRange(Rp)(MakeDecayingExp3(KP)):RangedBandit)*)
  (*|ExpGreedyDecay -> (module Bandit.DummyRange(MakeParametrizableEpsilonGreedy(Egpd)):RangedBandit)*)
  (*|ExpGreedy -> (module Bandit.DummyRange(MakeEpsilonGreedy(Egp)):RangedBandit)*)












   (*in match mixType with*)
     (*|Scorebased ->*)
     (*|Probabilistic ->*)
       (*let module MixParam = struct*)
         (*let p = alpha*)
       (*end*)

       (*in let  module CritBackfill = MakeThresholdedCriteria(T)(O)((val backfill:CriteriaSig))*)

       (*in let module SP = (val systemmodule : SystemParamSig)*)
       (*in (module MakeEasyBernouilli(C1)(C2)(MixParam)(CritBackfill)(SP):SchedulerSig)*)
