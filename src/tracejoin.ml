open Io 
open System

let printJobSub r j id output_channel =
  Printf.fprintf output_channel
      "%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d\n"
      id        (* 1  Job Number                     *)
      r         (* 2  Submit Time                    *)
      0         (* 3  Wait Time                      *)
      j.p       (* 4  Run Time                       *)
      j.q       (* 5  Number Of Allocated Processors *)
      0         (* 6  Average CPU Time Used          *)
      0         (* 7  Used Memory                    *)
      j.q       (* 8  Requested Number Of Processors *)
      j.p_est   (* 9  Requested Time                 *)
      0         (* 10 Requested Memory               *)
      1         (* 11 Status                         *)
      0         (* 12 User ID                        *)
      0         (* 13 Group ID                       *)
      0         (* 14 Executable Number              *)
      0         (* 15 Queue Number                   *)
      0         (* 16 Partition Number               *)
      0         (* 17 Preceding Job Number           *)
      0         (* 18 Think Time From Preceding Job  *)

let weeksize = 604800
let traceJoin output swfList =
  begin
    Printf.printf "Writing to %s\n" output;
    let oc = open_out output
    in try
      begin
        let i = ref 1
        and w = ref 0
        in let processFile filename =
          let jtable, maxprocs = Io.parse_jobs filename
          in let rmin = Hashtbl.fold (fun i j jm -> if j.r < jm then j.r else jm) jtable 99999998
          in let () = Printf.printf "%d\n" rmin;
          in let printShifted _ job =
            begin
              i := !i + 1;
              printJobSub (job.r - rmin + (!w * weeksize)) job !i oc
            end
          in (Hashtbl.iter printShifted jtable; w := !w + 1)
        in List.iter processFile swfList;
        close_out oc
      end
    with e ->
      begin
        close_out_noerr oc;
        raise e
      end
  end
