open System

type jobstatlist = (string * (int -> job -> float)) list
type tracestatlist = (string * (System.job_table -> Engine.history -> float)) list

module type Stat = sig
  val desc : string
  val stat : System.job_table -> Engine.history -> float
end

let statList : jobstatlist = [
  ("wait", fun t j -> float_of_int (t-j.r));
  ("stretch", fun t j -> (float_of_int (t-j.r+j.p)) /. (float_of_int (max 1 j.p)));
  ("bsld", fun t j -> max 1. ((float_of_int (t-j.r+j.p)) /. (float_of_int (max 60 j.p))));
  ("ppsld", fun t j -> max 1. ((float_of_int (t-j.r+j.p)) /. (float_of_int (j.q * (max 60 j.p)))));
]

let allStats =
  let allStatList =
    let cumulative_metric metric jobs hist =
      let f s (i,t)=
        let j = Hashtbl.find jobs i
        in s +. (metric t j)
      in List.fold_left f 0. hist
    in let average_metric metric jobs hist =
      (cumulative_metric metric jobs hist) /. (float_of_int (max 1 (List.length hist)))
    (*and geometric_metric metric jobs hist =*)
      (*let f s (i,t)=*)
        (*let j = Hashtbl.find jobs i*)
        (*in s *.  (1. +. (metric t j))*)
      (*in sqrt ( List.fold_left f 1. hist)*)
    (*in List.map (fun (s,x) -> ("cum"^s, geometric_metric x)) statList*)
    in List.map (fun (s,x) -> ("avg"^s, average_metric x)) statList
    @ List.map (fun (s,x) -> ("cum"^s, cumulative_metric x)) statList
  and modularize (desc,f) =
    let module M = struct let desc = desc let stat = f end
    in (desc,(module M:Stat))
    in List.map modularize allStatList
