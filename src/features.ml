(*open Engine*)
(*open Events*)
open System

(**System feature vector*)
module type SystemFeatures = sig
  val dimension : int
  val makeVector : int -> float array
  (**Argument: now *)
end

(**Job list feature vector*)
module type JobFeatures = sig
  val dimension : int
  val makeVector : int list -> int -> float array
  (**Argument: job list, now*)
end

(**Utility module for inclusion in feature builders*)
module MakeFeatureUtil(P:SystemParamSig) = struct
  (*function to make a comparator based on job attribute*)
  let cmpmake accessor x y = Pervasives.compare (accessor (Jobs.find P.jobs x)) (accessor (Jobs.find P.jobs y))

  (*function to get min and max of an attribute on a list*)
  let makemm accessor jobList =
    try
      let a,b = ( BatList.min_max ~cmp:(cmpmake accessor) jobList)
      in [accessor (Jobs.find P.jobs a);accessor (Jobs.find P.jobs b)]
    with (Invalid_argument _) -> [0;0]

  (*function to get avg attribute on a list*)
  let makesum accessor jobList =
    List.fold_left (fun acc i -> accessor (Jobs.find P.jobs i) + acc ) 0 jobList

  let makeavg accessor jobList =
    if List.length jobList = 0 
    then 0.
    else float_of_int (makesum accessor jobList) /. float_of_int (List.length jobList)

  let makeStat (jobList: int list) =
   List.map float_of_int
   (makemm (fun x -> x.q) jobList @
   makemm (fun x -> x.p_est) jobList @
   [List.length jobList;
    makesum (fun x -> x.q) jobList;
    makesum (fun x -> x.p_est) jobList;
    List.fold_left (fun acc i -> ((Jobs.find P.jobs i).q * (Jobs.find P.jobs i).p_est) +acc) 0 jobList])
   @[
    makeavg (fun x -> x.q) jobList;
    makeavg (fun x -> x.p_est) jobList;
   ]
end

(**Basic job features*)
module MakeJobFeatures
(P:SystemParamSig)
:JobFeatures = struct
  include MakeFeatureUtil(P)
  let makeVector il now = Array.of_list (makeStat il)
  let dimension = 8
end

(**Minimal job features*)
module MakeMinimalJobFeatures
(P:SystemParamSig)
:JobFeatures = struct
  include MakeFeatureUtil(P)
  let makeVector il _ = [|float_of_int(List.length il)|]
  let dimension = 1
end

(**Basic system features*)
module MakeSystemFeatures
(P:SystemParamSig)
:SystemFeatures = struct
  include MakeFeatureUtil(P)
  let makeVector now : float array =
     let l =
        [float_of_int P.resourcestate.free] @
         makeStat (List.map snd P.resourcestate.jobs_running_list) @
         makeStat !P.waitqueue
     in Array.of_list l
  let dimension = 17
end

(**Basic system features*)
module MakeEmptySystemFeatures
(P:SystemParamSig)
:SystemFeatures = struct
  include MakeFeatureUtil(P)
  let makeVector _ = [||]
  let dimension = 0
end

(**A state-action feature mapper*)
module type Phi = sig
  val phi : int -> (int * int list) -> float array
  val dim : int
end

module type Dim = sig
  val dim : int
end

module PhiUtil (D:Dim) = struct
  let scales = Array.make D.dim 0.000000000000001
  let scaleElem i e =
    if e<scales.(i) then e /. scales.(i) else begin
      scales.(i) <- e; 1.
    end
  let scaleVec v = Array.mapi scaleElem v
end

(**Building an "append" state-action feature mapper*)
module MakeAppendPhi
(P:SystemParamSig)
(SM:SystemFeatures)
(JB:JobFeatures)
:Phi
= struct
  module SM = MakeSystemFeatures(P)
  module JM = MakeJobFeatures(P)
  let dim = JM.dimension+SM.dimension
  include PhiUtil(struct let dim=dim end)
  let phi now a =
    let il = snd a
    in if now = 0 then
      Array.make dim 0.
    else
      scaleVec (Array.append (SM.makeVector now) (JM.makeVector il now))
end

(**Building an "append" state-action feature mapper*)
module MakeProductPhi
(P:SystemParamSig)
(SM:SystemFeatures)
(JB:JobFeatures)
:Phi
= struct
  module SM = MakeSystemFeatures(P)
  module JM = MakeJobFeatures(P)
  let dim = JM.dimension * SM.dimension + JM.dimension
  include PhiUtil(struct let dim=dim end)

  let phi now a =
    let il = snd a
    in if now = 0 then
      Array.make dim 0.
    else
      let v1 = (SM.makeVector now)
      and v2 = (JM.makeVector il now)
      (*in let () = begin*)
        (*Printf.printf "SYSTEM vec: %s" "\n";*)
        (*Array.iter (fun x -> Printf.printf "%f " x) (v1);*)
        (*Printf.printf "%s" "\n";*)
        (*Printf.printf "JOB vec: %s" "\n";*)
        (*Array.iter (fun x -> Printf.printf "%f " x) (v2);*)
        (*Printf.printf "%s" "\n"*)
      (*end*)
      and f x y = x *. y
      in let vec = Array.concat ([v2] @ Array.to_list (Array.map (fun x -> Array.map (f x) v1) v2))
      in scaleVec vec
      (*in  begin*)
        (*Printf.printf "FEATURE vec: %s" "\n";*)
        (*Array.iter (fun x -> Printf.printf "%f " x) (rvec);*)
        (*Printf.printf "%s" "\n";*)
        (*rvec*)
      (*end*)
end

