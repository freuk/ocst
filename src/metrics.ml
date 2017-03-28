open Jobs

module type JobTableParamSig = sig
  open Jobs
  val jobs: job_table
end

module type CriteriaSig = sig
  val desc : string
  val criteria : Jobs.job_table -> int -> int -> float
end

module type StatMetricSig = sig
  val add : int -> int list -> unit
  val get : unit -> float
  val reset : unit -> unit
end

module Make2MetricAccumulator (C:CriteriaSig) (J:JobTableParamSig) : StatMetricSig
                                                 = struct
                                                   let m = ref 0.
                                                   let add now = List.iter (fun i -> m := !m +. (C.criteria J.jobs now i))
                                                   let get () = !m
                                                   let reset () = m := 0.
                                                 end

module MakeMinus(C:CriteriaSig) : CriteriaSig =
struct
  let desc = "Inverse " ^ C.desc
  let criteria j n i = -. (C.criteria j n i)
end

module type ParamMixing = sig
  val alpha : float
end

module MakeMixedMetric(P:ParamMixing)(C1:CriteriaSig)(C2:CriteriaSig) : CriteriaSig =
struct
  let desc = "Mixed metric with " ^ C1.desc ^ " and " ^ C2.desc ^ (Printf.sprintf " , mixing parameter %f." P.alpha)
  let maxC1 = ref 0.
  let maxC2 = ref 0.
  let minC1 = ref 0.
  let minC2 = ref 0.
  let criteria j n i = 
    let normcrit crit minref maxref = 
      let c = crit j n i
      in begin
        maxref := max !maxref c;
        minref := min !minref c;
        (c -. !minref) /. (max 0.000001 (!maxref -. !minref))
      end
    in let c1 = normcrit C1.criteria minC1 maxC1
       and c2 = normcrit C2.criteria minC2 maxC2
    in ((1. -. P.alpha) *. c1) +. (P.alpha *. c2)
end

module CriteriaBSLD = struct
  let desc="BSLD"
  let criteria jobs now id = max 1.
                             (float_of_int (now - (find jobs id).r) /.
                              max (float_of_int (find jobs id).p_est) 600. )
end

module CriteriaWait = struct
  let desc="Waiting Time"
  let criteria jobs now id = float_of_int (now - (find jobs id).r)
end

module CriteriaMinusWait:CriteriaSig = MakeMinus(CriteriaWait)

module CriteriaSRF = struct
  let desc="Q/P ratio"
  let criteria jobs now id = let j = find jobs id in float_of_int j.q /. (float_of_int (max 1 j.p_est))
end

module CriteriaLRF = MakeMinus(CriteriaLRF)

module CriteriaSAF = struct
  let desc="Job maximum Area"
  let criteria jobs now id = let j = find jobs id in float_of_int (j.q * j.p_est)
end

module CriteriaLAF = MakeMinus(CriteriaLAF)

module CriteriSQF = struct
  let desc="Resource Requirement"
  let criteria jobs now id = float_of_int (find jobs id).q
end

module CriteriaLQF = MakeMinus(CriteriaLQF)

module CriteriaSPF = struct
  let desc="Processing time"
  let criteria jobs now id = float_of_int (find jobs id).p_est
end

module CriteriaLPF = MakeMinus(CriteriaLPF)

module CriteriaMExpFact = struct
  let desc="Expansion Factor"
  let criteria jobs now id = (float_of_int (now - (find jobs id).r + (find jobs id).p_est)) /. float_of_int (find jobs id).p_est
end

module CriteriaExpFact = MakeMinus(CriteriaExpFact)

module type ThresholdSig = sig
  val threshold : float
end

module MakeThresholdedCriteria (T:ThresholdSig)(O:CriteriaSig)(C:CriteriaSig) : CriteriaSig =
struct
  let desc=(Printf.sprintf "%0.3f-Thresholded " T.threshold) ^ C.desc
  let criteria jobs now id =
    let crit= O.criteria jobs now id
    in if crit > T.threshold then
      9999999.0 +. crit
    else C.criteria jobs now id
end

module MakeWaitAccumulator = Make2MetricAccumulator(CriteriaWait)

module MakeBsdlAccumulator = Make2MetricAccumulator(CriteriaBSLD)

let criteriaList = 
    [("wait", (module CriteriaWait : CriteriaSig));
    ("mwait", (module CriteriaMinusWait : CriteriaSig));
    ("lpf", (module CriteriaLPF : CriteriaSig));
    ("spf", (module CriteriaSPF : CriteriaSig));
    ("sqf", (module CriteriaSQF : CriteriaSig));
    ("lqf", (module CriteriaLQF : CriteriaSig));
    ("expfact", (module CriteriaExpFact : CriteriaSig));
    ("mexpfact", (module CriteriaMExpFact : CriteriaSig));
    ("lrf", (module CriteriaLRF : CriteriaSig));
    ("srf", (module CriteriaSRF : CriteriaSig));
    ("laf", (module CriteriaLAF : CriteriaSig));
    ("saf", (module CriteriaSAF : CriteriaSig))]
