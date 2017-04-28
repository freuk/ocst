open Io
open System
open BatEnum

let weeksize = 604800

let () =
  let jobs, args = do_io_cleaner ()
  in try
  let cmp x y = Pervasives.compare (x.r) (y.r)
  in let jmin, jmax = BatList.min_max ~cmp:cmp jobs
  in let iwmin,iwmax = (jmin.r/weeksize), (jmax.r/weeksize)

  in let i = ref 1

  in let f j =
    begin
      i := !i + 1;
      if (j.r/weeksize > iwmin && j.r/weeksize < iwmax ) then
      printjob j.r j !i args.output_channel
    end

  in List.iter f jobs;
    close_out args.output_channel
  with e ->
    close_out_noerr args.output_channel
