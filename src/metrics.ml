open System 

module type Criteria = sig
  val desc : string
  val criteria : job_table -> system -> int -> int -> float
end

module MakeMinus(C:Criteria) : Criteria =
struct
  let desc = "L" ^ C.desc
  let criteria s j n i = -. (C.criteria s j n i)
end

module BSLD = struct
  let desc="BSLD"
  let criteria jobs _ now id = max 1.
                             (float_of_int (now - (Hashtbl.find jobs id).r) /.
                              max (float_of_int (Hashtbl.find jobs id).p_est) 600. )
end

module FCFS = struct
  let desc="Waiting Time"
  let criteria jobs _ now id = float_of_int (now - (Hashtbl.find jobs id).r)
end

module LCFS:Criteria = MakeMinus(FCFS)

module SRF = struct
  let desc="P/Q ratio"
  let criteria jobs _ now id = let j = Hashtbl.find jobs id in float_of_int j.p /. (float_of_int (max 1 j.q))
end

module LRF = MakeMinus(SRF)

module SAF = struct
  let desc="Job maximum Area"
  let criteria jobs _ now id = let j = Hashtbl.find jobs id in float_of_int (j.q * j.p_est)
end

module LAF = MakeMinus(SAF)

module SQF = struct
  let desc="Resource Requirement"
  let criteria jobs _ now id = float_of_int (Hashtbl.find jobs id).q
end

module LQF = MakeMinus(SQF)

module SPF = struct
  let desc="Processing time"
  let criteria jobs _ now id = float_of_int (Hashtbl.find jobs id).p_est
end

module LPF = MakeMinus(SPF)

module LEXP = struct
  let desc="Expansion Factor"
  let criteria jobs _ now id = (float_of_int (now - (Hashtbl.find jobs id).r + (Hashtbl.find jobs id).p_est)) /. float_of_int (Hashtbl.find jobs id).p_est
end

module SEXP = MakeMinus(LEXP)

let criteriaList = 
    [("fcfs", (module FCFS : Criteria));
    ("lcfs", (module LCFS : Criteria));
    ("lpf", (module LPF : Criteria));
    ("spf", (module SPF : Criteria));
    ("sqf", (module SQF : Criteria));
    ("lqf", (module LQF : Criteria));
    ("lexp", (module LEXP : Criteria));
    ("sexp", (module SEXP : Criteria));
    ("lrf", (module LRF : Criteria));
    ("srf", (module SRF : Criteria));
    ("laf", (module LAF : Criteria));
    ("saf", (module SAF : Criteria))]

module type ParamMixing = sig
  val alpha : float list
end

let rawPolicyList = [SPF.criteria;
                     SQF.criteria;
                     FCFS.criteria;
                     LRF.criteria;
                     LAF.criteria;
                     ExpFact.criteria]
let zeroMixed = BatList.map (fun _ -> 0.) rawPolicyList

module MakeMixedMetric(P:ParamMixing)(SP:SystemParam) : Criteria =
struct

  let makeStat (jobList: int list) =
   List.map float_of_int
   ( [List.length jobList;
    makesum (fun x -> x.q) jobList;
    makesum (fun x -> x.p_est) jobList;
    List.fold_left (fun acc i -> ((Jobs.find P.jobs i).q * (Jobs.find P.jobs i).p_est) +acc) 0 jobList])

  let makeVector now : float array =
     let l =
        [float_of_int P.resourcestate.free] @
         makeStat (List.map snd P.resourcestate.jobs_running_list) @
         makeStat !P.waitqueue
     in Array.of_list l

  let makeSystemFeatureValues s = [float_of_int s.free] @ FtUtil.makeStat !SP.waitqueue

  let () = 
   let systemDim = List.length (makeSystemFeatureValues ())
   in let expected = baseDim + systemDim * baseDim
      and real = List.length P.alpha
   in if (not (real = expected)) then
     begin
       Printf.printf "real vector size %d, expected %d;\n" real expected;
       Printf.printf "baseDim %d systemDim %d\n" baseDim systemDim;
       assert (real=expected)
     end

  let desc = "Mixed metric."
  let criteria j n i = 
    let jobFeatureValues = List.map (fun crit -> crit j n i) rawPolicyList
    and systemFeatureValues = makeSystemFeatureValues ()
    in let attributeList = 
      jobFeatureValues @
      (List.map (fun (x,y) -> x *. y) (BatList.cartesian_product jobFeatureValues systemFeatureValues)) 
    in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0. P.alpha attributeList
end


(*module type Threshold = sig*)
  (*val threshold : float*)
(*end*)

(*let zeroMixed = List.map (fun _ -> 0.) rawPolicyList*)
(*let makeProduct cl x y z = ((List.hd cl) x y z) *. ((List.nth cl 1) x y z)*)
(*let makeSquare c = makeProduct [c;c]*)

(*[>off https://codereview.stackexchange.com/questions/40366/combinations-of-size-k-from-a-list-in-ocaml<]*)
(*let rec combnk k lst =*)
  (*let rec inner acc k lst =*)
    (*match k with*)
    (*| 0 -> [[]]*)
    (*| _ ->*)
      (*match lst with*)
      (*| []      -> acc*)
      (*| x :: xs ->*)
        (*let rec accmap acc f = function*)
          (*| []      -> acc*)
          (*| x :: xs -> accmap ((f x) :: acc) f xs*)
        (*in*)
          (*let newacc = accmap acc (fun z -> x :: z) (inner [] (k - 1) xs)*)
          (*in*)
            (*inner newacc k xs*)
    (*in*)
      (*inner [] k lst*)

(*let baseDim = List.length rawPolicyList*)

(*module MakeThresholdedCriteria (T:Threshold)(O:Criteria)(C:Criteria) : Criteria =*)
(*struct*)
  (*let desc=(Printf.sprintf "%0.3f-Thresholded " T.threshold) ^ C.desc*)
  (*let criteria jobs now id =*)
    (*let crit= O.criteria jobs now id*)
    (*in if crit > T.threshold then*)
      (*9999999.0 +. crit*)
    (*else C.criteria jobs now id*)
(*end*)

(*module MakeWaitAccumulator = Make2MetricAccumulator(CriteriaWait)*)

(*module MakeBsdlAccumulator = Make2MetricAccumulator(CriteriaBSLD)*)

