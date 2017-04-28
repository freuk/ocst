open Io
open System
open BatEnum

let weeksize = 604800

let rec permutation list =
   let rec extract acc n = function
     | [] -> raise Not_found
     | h :: t -> if n = 0 then (h, acc @ t) else extract (h::acc) (n-1) t
   in
   let extract_rand list len =
     extract [] (Random.int len) list
   in
   let rec aux acc list len =
     if len = 0 then acc else
       let picked, rest = extract_rand list len in
       aux (picked :: acc) rest (len-1)
   in
   aux [] list (List.length list);;

let () =
  let jobs, args = do_io_perturbator ()
  in try
  let () = Random.init args.random_seed
  in let cmpj x y = Pervasives.compare (x.r) (y.r)
  in let jmin, jmax = BatList.min_max ~cmp:cmpj jobs
  in let cmpu x y = Pervasives.compare (x.u) (y.u)
  in let umin, umax = BatList.min_max ~cmp:cmpu jobs
  in let iwmin,iwmax = (jmin.r/weeksize), (jmax.r/weeksize)

  in let shl u = permutation (BatList.of_enum ((iwmin+1) -- (iwmax-1)))

  in let shls = List.map shl (BatList.of_enum (umin.u -- umax.u))

  in let weekshift t u=
    let wid = t / weeksize
    and wt = t mod weeksize
    in if (iwmin < wid)  && ( wid < iwmax) then
      Some (((List.nth (List.nth shls (u-umin.u)) (wid-iwmin-1)) * weeksize) + wt)
    else
      None

  in let i = ref 1
  in let f j =
    begin
      i := !i + 1;
      BatOption.may
      (fun wsr -> printjob_shift wsr j !i args.output_channel)
      (weekshift j.r j.u)
    end

  in List.iter f jobs;
    close_out args.output_channel
  with e ->
    close_out_noerr args.output_channel

