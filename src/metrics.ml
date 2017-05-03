open System

module type Criteria = sig
  type log
  val desc : string
  val criteria : job_table -> system -> int -> int -> (float * log_entry)
  val combine_log : log -> log_entry -> log
end

module MakeMinus(C:Criteria) : Criteria with type log = C.log =
struct
  include C
  let desc = "L" ^ C.desc
  let criteria j s n i = -. (criteria j s n i)
end

module BSLD = struct
  type log = float
  let desc="BSLD"
  let criteria jobs _ now id = max 1.
                               (float_of_int (now - (Hashtbl.find jobs id).r) /.
                                max (float_of_int (Hashtbl.find jobs id).p_est) 600. )
end

module FCFS = struct
  type log = float
  let desc="Waiting Time"
  let criteria jobs _ now id = float_of_int (now - (Hashtbl.find jobs id).r)
end

module LCFS:Criteria = MakeMinus(FCFS)

module SRF = struct
  type log = float
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

let features_job =
  [SPF.criteria;
   SQF.criteria;
   FCFS.criteria;]

let features_job_2 =
  [LRF.criteria;
   LAF.criteria;
   LEXP.criteria;
   fun jobs _ now id -> float_of_int (Hashtbl.find jobs id).p_est;]

let features_system = [fun j s n i -> float_of_int s.free;]

let zeroMixed = BatList.map (fun _ -> 0.) features_job

module MakeSum(C:Criteria)(C2:Criteria)=
struct
  let desc = "Polynomial Mixed metric."
  let criteria j s n i = (C.criteria j s n i) +. (C2.criteria j s n i)
end
let makeSum m1 m2 =
  let module M = (val m1:Criteria)
  in let module M2 = (val m1:Criteria)
  in let module Mr = MakeSum(M)(M2)
  in (module Mr:Criteria)

module type Alpha = sig
  val alpha : float list
end

module MakeBasicMixed(P:Alpha): Criteria =
struct
  let desc = "Basic Mixed metric."
  let criteria j s n i =
    let attributeList = List.map (fun crit -> crit j s n i) features_job
    in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0. P.alpha attributeList
end
let makeBasicMixed alpha =
  let module M = MakeBasicMixed(struct let alpha = alpha end) in (module M:Criteria)

module MakePolynomialMixed(P:Alpha): Criteria =
struct
  let desc = "Polynomial Mixed metric."
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
  let criteria j s n i =
    let atl = List.map (fun crit -> crit j s n i) features_job
    in let polyAttrList = List.map (fun (x::xs) -> x *. List.hd xs) (combnk 2 atl)
    in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0. P.alpha polyAttrList
end
let makePolynomialMixed alpha =
  let module M = MakePolynomialMixed(struct let alpha = alpha end) in (module M:Criteria)

module MakeSystemMixed(P:Alpha): Criteria =
struct
  let desc = "System Mixed metric."
  let criteria j s n i =
    let attributeList =
      let jl = List.map (fun crit -> crit j s n i) features_job
      and sl = List.map (fun crit -> crit j s n i) features_system
      in List.map (fun (x, y) -> x *. y) (BatList.cartesian_product jl sl)
    in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0. P.alpha attributeList
end
let makeSystemMixed alpha =
  let module M = MakeSystemMixed(struct let alpha = alpha end) in (module M:Criteria)

(*module type Threshold = sig*)
  (*val threshold : float*)
(*end*)

(*let zeroMixed = List.map (fun _ -> 0.) features_job*)
(*let makeProduct cl x y z = ((List.hd cl) x y z) *. ((List.nth cl 1) x y z)*)
(*let makeSquare c = makeProduct [c;c]*)

(*[>off https://codereview.stackexchange.com/questions/40366/combinations-of-size-k-from-a-list-in-ocaml<]*)

(*let baseDim = List.length features_job*)

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

  (*let makeStat (jobList: int list) =*)
  (*List.map float_of_int*)
  (*( [List.length jobList;*)
  (*makesum (fun x -> x.q) jobList;*)
  (*makesum (fun x -> x.p_est) jobList;*)
  (*List.fold_left (fun acc i -> ((Jobs.find P.jobs i).q * (Jobs.find P.jobs i).p_est) +acc) 0 jobList])*)

  (*let makeVector now : float array =*)
  (*let l =*)
  (*[float_of_int P.resourcestate.free] @*)
  (*makeStat (List.map snd P.resourcestate.jobs_running_list) @*)
  (*makeStat !P.waitqueue*)
  (*in Array.of_list l*)

  (*let makeSystemFeatureValues s = [float_of_int s.free] @ FtUtil.makeStat !SP.waitqueue*)

  (*let () = *)
  (*let systemDim = List.length (makeSystemFeatureValues ())*)
  (*in let expected = baseDim + systemDim * baseDim*)
  (*and real = List.length P.alpha*)
  (*in if (not (real = expected)) then*)
  (*begin*)
  (*Printf.printf "real vector size %d, expected %d;\n" real expected;*)
  (*Printf.printf "baseDim %d systemDim %d\n" baseDim systemDim;*)
  (*assert (real=expected)*)
  (*end*)





          (*and systemFeatureValues = makeSystemFeatureValues ()*)
          (*in let attributeList = *)
           (*jobFeatureValues @*)
           (*(List.map (fun (x,y) -> x *. y) (BatList.cartesian_product jobFeatureValues systemFeatureValues)) *)
