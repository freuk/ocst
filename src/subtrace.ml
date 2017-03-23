open Io
open Jobs
open BatEnum

let weeksize = 604800

let () =
  let jobs, args = do_io_subtrace ()
  in try
  let cmp x y = Pervasives.compare (x.r) (y.r)
  in let jmin, jmax = BatList.min_max ~cmp:cmp jobs
  in let iwmin,iwmax = (jmin.r/weeksize), (jmax.r/weeksize)

  in let i = ref 1

  in let f j =
    begin
      i := !i + 1;
      if (j.r/weeksize - iwmin > args.wid  && j.r/weeksize -iwmin < args.wid + args.weekspan) then
      printjob j.r j !i args.output_channel
    end

  in List.iter f jobs;
    close_out args.output_channel
  with e ->
    close_out_noerr args.output_channel
