open System

let wait t j = float_of_int (t-j.r)
let stretch t j= (float_of_int (t-j.r+j.p)) /. (float_of_int (max 1 j.p))
let stretch t j= (float_of_int (t-j.r+j.p)) /. (float_of_int (max 1 j.p))

let cumulative_metric metric jobs hist = 
  let f s (i,t)=
    let j = Hashtbl.find jobs i
    in s +. (metric t j)
  in List.fold_left f 0. hist

let average_metric metric jobs hist = 
  (cumulative_metric metric jobs hist) /. (float_of_int (max 1 (List.length hist)))

let print_allstats chan jobs hist= 
  let f s1 metric s2  = Printf.fprintf chan "%s %s %0.3f\n" s1 s2 (metric jobs hist)
  in let f_cum s m = 
    (f "cumulative" (cumulative_metric m) s;
    f "average" (average_metric m) s)
  in 
    f_cum "waiting time" wait;
    f_cum "stretch" stretch
