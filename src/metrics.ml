open Jobs
open System

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
  val alpha : float list
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
  let desc="P/Q ratio"
  let criteria jobs now id = let j = find jobs id in float_of_int j.p /. (float_of_int (max 1 j.q))
end

module CriteriaLRF = MakeMinus(CriteriaSRF)

module CriteriaSAF = struct
  let desc="Job maximum Area"
  let criteria jobs now id = let j = find jobs id in float_of_int (j.q * j.p_est)
end

module CriteriaLAF = MakeMinus(CriteriaSAF)

module CriteriaSQF = struct
  let desc="Resource Requirement"
  let criteria jobs now id = float_of_int (find jobs id).q
end

module CriteriaLQF = MakeMinus(CriteriaSQF)

module CriteriaSPF = struct
  let desc="Processing time"
  let criteria jobs now id = float_of_int (find jobs id).p_est
end

module CriteriaLPF = MakeMinus(CriteriaSPF)

module CriteriaMExpFact = struct
  let desc="Expansion Factor"
  let criteria jobs now id = (float_of_int (now - (find jobs id).r + (find jobs id).p_est)) /. float_of_int (find jobs id).p_est
end

module CriteriaSRwF = struct
  let desc="wait/q ratio"
  let criteria jobs now id = let j = find jobs id in float_of_int (now - j.r) /. (float_of_int (max 1 j.q))
end

module CriteriaLRwF = MakeMinus(CriteriaSRwF)

module CriteriaExpFact = MakeMinus(CriteriaMExpFact)

module type ThresholdSig = sig
  val threshold : float
end

let rawPolicyList = [CriteriaSPF.criteria;
                     CriteriaSQF.criteria;
                     CriteriaWait.criteria;
                     CriteriaLRF.criteria;
                     CriteriaLAF.criteria;
                     CriteriaExpFact.criteria]
let zeroMixed = List.map (fun _ -> 0.) rawPolicyList
let makeProduct cl x y z = ((List.hd cl) x y z) *. ((List.nth cl 1) x y z)
let makeSquare c = makeProduct [c;c]

(*off https://codereview.stackexchange.com/questions/40366/combinations-of-size-k-from-a-list-in-ocaml*)
let rec combnk k lst =
  let rec inner acc k lst =
    match k with
    | 0 -> [[]]
    | _ ->
      match lst with
      | []      -> acc
      | x :: xs ->
        let rec accmap acc f = function
          | []      -> acc
          | x :: xs -> accmap ((f x) :: acc) f xs
        in
          let newacc = accmap acc (fun z -> x :: z) (inner [] (k - 1) xs)
          in
            inner newacc k xs
    in
      inner [] k lst

let baseDim = List.length rawPolicyList

module MakeMixedMetric(P:ParamMixing)(SP:SystemParamSig) : CriteriaSig =
struct

  module  FtUtil = Features.MakeFeatureUtil(SP)

  let makeSystemFeatureValues () = [float_of_int SP.resourcestate.free] @
      FtUtil.makeStat !SP.waitqueue

  let systemDim = List.length (makeSystemFeatureValues ())
  let attributeDim =  baseDim + systemDim * baseDim

  let () = 
    let real = List.length P.alpha
    in if (not (real = attributeDim)) then
    begin
      Printf.printf "real vector size %d, expected %d;\n" real attributeDim;
      Printf.printf "baseDim %d systemDim %d\n" baseDim systemDim;
      failwith "alpha dim error." 
    end

  let desc = "Mixed metric."
  let scales = ref (BatList.make attributeDim 0.)
  let updateScales xl = scales := BatList.map2 (fun s x -> if (abs_float x) > s then x else s) !scales xl
  let scale x = List.map2 (fun s x-> if s>0. then x /. s else 0.) !scales x

  let criteria j n i = 
    let jobFeatureValues = List.map (fun crit -> crit j n i) rawPolicyList
    and systemFeatureValues = makeSystemFeatureValues ()
    in let attributeList = 
      jobFeatureValues @
      (List.map (fun (x,y) -> x *. y) (BatList.cartesian_product jobFeatureValues systemFeatureValues)) 
    in let () = updateScales attributeList
    in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0. P.alpha (scale attributeList)
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
