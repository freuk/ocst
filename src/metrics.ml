
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

module LRF = struct
  type log = float
  let desc="P/Q ratio"
  let criteria jobs _ now id =
    let j = Hashtbl.find jobs id
    in Value (float_of_int j.p_est /. (float_of_int (max 1 j.q)))
end

module SRF = MakeMinus(LRF)

module LAF = struct
  let desc="Job Area"
  let criteria jobs _ now id =
    let j = Hashtbl.find jobs id
    in Value (float_of_int (j.q * j.p_est))
end

module SAF = MakeMinus(LAF)

module LQF = struct
  let desc="Resource Requirement"
  let criteria jobs _ now id = Value (float_of_int (Hashtbl.find jobs id).q)
end

module SQF = MakeMinus(LQF)

module LPF = struct
  let desc="Processing time"
  let criteria jobs _ now id = Value (float_of_int (Hashtbl.find jobs id).p_est)
end

module SPF = MakeMinus(LPF)

module LEXP = struct
  let desc="Expansion Factor"
  let criteria jobs _ now id = Value ((float_of_int (now - (Hashtbl.find jobs id).r + (Hashtbl.find jobs id).p_est)) /. float_of_int (Hashtbl.find jobs id).p_est)
end

module SEXP = MakeMinus(LEXP)

let criteriaList =
  [("fcfs", (module FCFS : Criteria));
   ("lcfs", (module LCFS : Criteria));
   ("lpf" , (module LPF  : Criteria));
   ("spf" , (module SPF  : Criteria));
   ("sqf" , (module SQF  : Criteria));
   ("lqf" , (module LQF  : Criteria));
   ("lexp", (module LEXP : Criteria));
   ("sexp", (module SEXP : Criteria));
   ("lrf" , (module LRF  : Criteria));
   ("srf" , (module SRF  : Criteria));
   ("laf" , (module LAF  : Criteria));
   ("saf" , (module SAF  : Criteria))]

(*************************************** FEATURES ***********************************)

let feature_w =("w",FCFS.criteria)
let feature_q =("q",SQF.criteria)
let feature_p =("p",SPF.criteria)

let features_job_mayzero : (string*criteria) list = [feature_w;]
let features_job_nonzero : (string*criteria) list = [feature_q;feature_p]

let features_job : (string*criteria) list = features_job_nonzero @ features_job_mayzero

let features_job_threshold =
  let mkth (t, crit) = (fst crit, fun j s n i -> Value (max 0. ((get_value ((snd crit) j s n i)) -. t)))
  and mkth' (t, crit) = (fst crit, fun j s n i -> Value (max 0. (t -. (get_value ((snd crit) j s n i)))))
  and thresholds = 
    let raw = [2.;1.;0.5;0.25;0.1]
    in raw@[0.]@(List.map (fun x -> -. x) raw)
  in List.map mkth (BatList.cartesian_product thresholds features_job)
  @ (List.map mkth' (BatList.cartesian_product thresholds features_job))

let features_system =
    let get_min accessor j = function
    |[] -> 0
     |x -> List.fold_left (fun acc i -> min acc (accessor (Hashtbl.find j i)) ) max_int x
    and get_min_float accessor j = function
    |[] -> 0.
     |x ->  List.fold_left (fun acc i -> min acc (accessor (Hashtbl.find j i))) max_float x
    and get_max accessor j =
      List.fold_left (fun acc i -> max acc (accessor (Hashtbl.find j i)) ) 0
    and get_max_float accessor j =
      List.fold_left (fun acc i -> max acc (accessor (Hashtbl.find j i)) ) 0.
    and sum_zero accessor j =
      List.fold_left (fun acc i -> accessor (Hashtbl.find j i) + acc ) 0
    and sum_zero_float accessor j =
      List.fold_left (fun acc i -> accessor (Hashtbl.find j i) +. acc ) 0.
    and sum_elapsed now = List.fold_left (fun acc (ts,_) -> now - ts + acc ) 0
    and sum_remain now j =
      List.fold_left (fun acc (ts,i) -> (Hashtbl.find j i).p_est + ts - now + acc ) 0
    and sum_elapsed_q now j =
      List.fold_left (fun acc (ts,i) -> (Hashtbl.find j i).q * (now - ts) + acc ) 0
    and sum_remain_q now j =
      List.fold_left
        (fun acc (ts,i) ->
           (Hashtbl.find j i).q *((Hashtbl.find j i).p_est + ts -now) + acc ) 0
    in [("free",fun _ s _ _ -> value_of_int s.free);
       ("lwait",fun _ s _ _ -> (value_of_int (List.length s.waiting)));
       ("maxw_queue",fun j s n _ -> value_of_int (get_max (fun j -> n-j.r) j s.waiting));
       ("maxq_queue",fun j s _ _ -> value_of_int (get_max (fun j -> j.q) j s.waiting));
       ("maxp_queue",fun j s _ _ -> value_of_int (get_max (fun j -> j.p_est) j s.waiting));
       (*("minw_queue",fun j s n _ -> value_of_int (get_min (fun j -> n-j.r) j s.waiting));*)
       ("minq_queue",fun j s _ _ -> value_of_int (get_min (fun j -> j.q) j s.waiting));
       ("minp_queue",fun j s _ _ -> value_of_int (get_min (fun j -> j.p_est) j s.waiting));
       ("sumw_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> n-j.r) j s.waiting));
       ("sumq_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> j.q) j s.waiting));
       ("sumq2_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> j.q * j.q) j s.waiting));
       ("sumqlog_queue",fun j s n _ -> Value (sum_zero_float (fun j -> log (float_of_int j.q)) j s.waiting));
       ("sumpq_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> j.q*j.p_est) j s.waiting));
       ("sumr_queue",fun j s n _ -> Value (sum_zero_float (fun j -> (float_of_int j.p_est) /. (max 1. (float_of_int j.q)) ) j s.waiting));
       ("maxpq_queue",fun j s n _ -> value_of_int (get_max (fun j -> j.q*j.p_est) j s.waiting));
       ("maxr_queue",fun j s n _ -> Value (get_max_float(fun j -> (float_of_int j.p_est) /. (max 1. (float_of_int j.q)) ) j s.waiting));
       ("minpq_queue",fun j s n _ -> value_of_int (get_min (fun j -> j.q*j.p_est) j s.waiting));
       ("minr_queue",fun j s n _ -> Value (get_min_float(fun j -> (float_of_int j.p_est) /. (max 1. (float_of_int j.q)) ) j s.waiting));
       ("sump_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> j.p_est) j s.waiting));
       ("sump_queue",fun j s n _ -> value_of_int (sum_zero (fun j -> j.p_est) j s.waiting));
       ("sump_rem_run",fun j s n _ -> value_of_int (sum_remain n j s.running));
       ("sump_elap_run",fun j s n _ -> value_of_int (sum_elapsed n  s.running));
       ("sumpq_rem_run",fun j s n _ -> value_of_int (sum_remain_q n j s.running));
       ("sumpq_elap_run",fun j s n _ -> value_of_int (sum_elapsed_q n j s.running));
      ]

let features_job_advanced, features_system_job =
  (*helpers*)
  let divf (f:string *criteria) (g:string*criteria) : (string*criteria) =
    let divcrit j s n i = Value ((get_value ((snd f) j s n i)) /. (get_value ((snd g) j s n i)))
    in (((fst f)^"/"^(fst g)),divcrit)
  and multf (f:string *criteria) (g:string*criteria) : (string*criteria) =
    let mulcrit j s n i = Value ((get_value ((snd f) j s n i)) *. (get_value ((snd g) j s n i)))
    in (((fst f)^"*"^(fst g)), mulcrit)
  in let multf3 (f:string *criteria) (g:string*criteria) (h:string*criteria) : (string*criteria) =
    let mulcrit j s n i = Value ((get_value ((snd f) j s n i)) *. (get_value ((snd g) j s n i))*. (get_value ((snd h) j s n i)))
    in (((fst f)^"*"^(fst g)^"*"^(fst h)), mulcrit)
  in let ft_cartesian_product l1 l2=
    List.map (fun (x, y) -> multf x y) (BatList.cartesian_product l1 l2)
  in let ft_triangular_product ft_list =
    let rec triangle result = function
      |[] -> result
      |(x::xs) -> triangle ((List.map (fun y -> (x,y)) (x::xs)) @ result) xs
    in List.map (fun (x, y) -> multf x y) (triangle [] ft_list)
  (*features*)
  in let features_system_job : (string*criteria) list =
    ft_cartesian_product features_job features_system
  in let features_job_square = ft_triangular_product features_job
  and features_job_div : (string*criteria) list  =
    List.map (fun (x, y) -> divf x y)
      (BatList.cartesian_product features_job_mayzero features_job_nonzero)
  and features_job_square_div : (string*criteria) list =
    List.map (fun (x, y) -> divf x y)
      (BatList.cartesian_product
         (ft_cartesian_product features_job_mayzero features_job_mayzero)
         features_job_nonzero)
  and features_job_square_div2 : (string*criteria) list =
    List.map (fun (x, y) -> divf x y)
      [(feature_p,feature_q);
       (feature_q,feature_p);
       (multf feature_q feature_q,feature_p);
       (multf feature_p feature_p,feature_q);
       (multf feature_w feature_p,feature_q);
       (multf feature_w feature_q,feature_p);
      ];
  and features_job_semantic : (string*criteria) list =
    [("exp",LEXP.criteria);]
  in (features_job_semantic@features_job_square@features_job_div@features_job_square_div@features_job_square_div2), features_system_job

let features_job_plus : (string*criteria) list =
  features_job_nonzero @ features_job_mayzero @
  [("exp",SEXP.criteria);
  ("r",SRF.criteria);
  ("a",SAF.criteria);]

(*************************************** FEATURE TOOLKIT **************************)

let state_features state_fn now_fn additional_fn output_fn =
  let s = Io.load_system state_fn
  and jobs,_ = Io.parse_jobs additional_fn
  and now = Io.load_now now_fn
  in let values = List.map (fun (_,f) -> get_value (f jobs s now 0)) features_system
  in let s = String.concat "," (List.map (Printf.sprintf "%0.3f") values)
  in let printer chan = Printf.fprintf chan "%s\n" s
  in Io.wrap_io_out (Some output_fn) printer

(*************************************** MIXING ***********************************)

module MakeSum(C:Criteria)(C2:Criteria)=
struct
  let desc = C.desc^","^C2.desc
  let criteria j s n i =
    let f critf = match critf j s n i with
      |Value v -> v,[v]
      |ValueLog (v,l) ->  v,l
    in let c1,l1 = f C.criteria
    and c2,l2 = f C2.criteria
    in ValueLog ((c1 +. c2), (l1@l2))
end
let makeSum m1 m2 =
  let module M = MakeSum((val m1:Criteria))((val m2:Criteria))
  in (module M:Criteria)

module type Alpha = sig
  val alpha : (float list) * (float list) * (float list)
  val ftlist : (string * criteria) list
end

module MakeMixed(P:Alpha): Criteria =
struct
  let () =

    let check_dim accessor s =
      let actual=List.length (accessor P.alpha)
      and expected=List.length P.ftlist
      in if not (actual = expected) then
        failwith
          (Printf.sprintf
             "%s parameter vector of wrong dimension. expected %d, obtained %d"
                                                                     s expected actual)
      else ()
    in begin
      check_dim BatTuple.Tuple3.first "first";
      check_dim BatTuple.Tuple3.second "second";
      check_dim BatTuple.Tuple3.third "third";
    end

    let desc = String.concat "," (List.map fst P.ftlist)
  let criteria j s n i =
    let attributeList =
      List.map (fun (_,crit) -> get_value (crit j s n i)) P.ftlist
    in let v =
      let normalize l = l
        |> (List.map2 (fun avg x -> x -. avg ) (BatTuple.Tuple3.second P.alpha))
        |> (List.map2 (fun var x -> if var = 0. then 0. else x /. var ) (BatTuple.Tuple3.third P.alpha))
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
