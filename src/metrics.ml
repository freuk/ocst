open System

type crit_output = Value of float | ValueLog of (float * (float list))
let get_value = function
  |Value v -> v
  |ValueLog (v,_) -> v
let value_of_int x = Value (float_of_int x)

type criteria = job_table -> system -> int -> int -> crit_output

module type Criteria = sig
  val desc : string
  val criteria : criteria
end

module MakeMinus(C:Criteria) : Criteria =
struct
  include C
  let desc = "L" ^ C.desc
  let criteria j s n i = match (criteria j s n i) with
    |Value v -> Value (-. v)
    |ValueLog (v,l) -> ValueLog ((-. v),l)
end

module BSLD = struct
  type log = float
  let desc="BSLD"
  let criteria jobs _ now id =
    Value ( max 1.
            (float_of_int (now - (Hashtbl.find jobs id).r) /.
             max (float_of_int (Hashtbl.find jobs id).p_est) 600. ))
end

module FCFS = struct
  type log = float
  let desc="Waiting Time"
  let criteria jobs _ now id = Value (float_of_int (now - (Hashtbl.find jobs id).r))
end

module LCFS:Criteria = MakeMinus(FCFS)

module SRF = struct
  type log = float
  let desc="P/Q ratio"
  let criteria jobs _ now id =
    let j = Hashtbl.find jobs id
    in Value (float_of_int j.p /. (float_of_int (max 1 j.q)))
end

module LRF = MakeMinus(SRF)

module SAF = struct
  let desc="Job maximum Area"
  let criteria jobs _ now id =
    let j = Hashtbl.find jobs id
    in Value (float_of_int (j.q * j.p_est))
end

module LAF = MakeMinus(SAF)

module SQF = struct
  let desc="Resource Requirement"
  let criteria jobs _ now id = Value (float_of_int (Hashtbl.find jobs id).q)
end

module LQF = MakeMinus(SQF)

module SPF = struct
  let desc="Processing time"
  let criteria jobs _ now id = Value (float_of_int (Hashtbl.find jobs id).p_est)
end

module LPF = MakeMinus(SPF)

module LEXP = struct
  let desc="Expansion Factor"
  let criteria jobs _ now id = Value ((float_of_int (now - (Hashtbl.find jobs id).r + (Hashtbl.find jobs id).p_est)) /. float_of_int (Hashtbl.find jobs id).p_est)
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

(*************************************** FEATURES ***********************************)

let features_job : criteria list =
  [SPF.criteria;
   SQF.criteria;
   FCFS.criteria;]

let multf (f:criteria) (g:criteria) : criteria =
  fun j s n i ->
    Value ((get_value (f j s n i)) *. (get_value (f j s n i)))

let features_job_poly : criteria list  =
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
  in
    List.map (fun (x::xs) -> multf x (List.hd xs)) (combnk 2 features_job)

let features_system : criteria list =
  [(fun _ s _ _ -> value_of_int s.free);
   (fun _ s _ _ -> (value_of_int (List.length s.waiting)));]

let features_system_job : criteria list  =
  List.map (fun (x, y) -> multf x y)
    (BatList.cartesian_product features_job features_system )

let features_job_2 : criteria list  =
  [LRF.criteria;
   LAF.criteria;
   LEXP.criteria;]

module MakeSum(C:Criteria)(C2:Criteria)=
struct
  let desc = "Polynomial Mixed metric."
  let criteria j s n i =
    let f critf = match critf j s n i with
      |Value v -> v,[v]
      |ValueLog (v,l) ->  v,l
    in let c1,l1 = f C.criteria
    and c2,l2 = f C2.criteria
    in ValueLog ((c1 +. c2), (l1@l2))
end
let makeSum m1 m2 =
  let module M = (val m1:Criteria)
  in let module M2 = (val m1:Criteria)
  in let module Mr = MakeSum(M)(M2)
  in (module Mr:Criteria)

(*************************************** MIXING ***********************************)

module type Alpha = sig
  val alpha : (float list) * (float list) * (float list)
  val ftlist : criteria list
end

module MakeMixed(P:Alpha): Criteria =
struct
  if (List.exists (fun x -> x = 0.) (BatTuple.Tuple3.third P.alpha)) then
    failwith "The third parameter vector to the mixed heuristic can not have null elements."
  let () = assert (List.length (BatTuple.Tuple3.first P.alpha) = List.length P.ftlist)
  let () = assert (List.length (BatTuple.Tuple3.second P.alpha) = List.length P.ftlist)
  let () = assert (List.length (BatTuple.Tuple3.third P.alpha) = List.length P.ftlist)
  let desc = "Basic Mixed metric."
  let criteria j s n i =
    let attributeList =
      List.map (fun crit -> get_value (crit j s n i)) P.ftlist
    in let v =
      let normalize l = l
        |> (List.map2 (fun avg x -> x -. avg ) (BatTuple.Tuple3.second P.alpha))
        |> (List.map2 (fun var x -> x /. var ) (BatTuple.Tuple3.third P.alpha))
      in List.fold_left2 (fun s weight x -> s +. (weight *. x)) 0.
         (BatTuple.Tuple3.first P.alpha) (normalize attributeList)
    in ValueLog (v, attributeList)
end
let makeMixed ftlist alpha =
  let module P = struct
    let alpha = alpha
    let ftlist = ftlist
  end
    in let module M = MakeMixed(P)
    in (module M:Criteria)

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
