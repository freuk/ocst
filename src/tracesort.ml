open Io 
open System

let trace_sort output input =
  begin
    Printf.printf "Writing to %s\n" output;
    let oc = open_out output
    in try
      let cmp (ix,x) (iy,y) = Pervasives.compare (x.r) (y.r)
      and jtable, maxprocs = Io.parse_jobs input 
      in let jlist = Hashtbl.fold (fun i j l-> l@[(i,j)] ) jtable []
      in let jlist_sorted = List.sort cmp jlist
      in let myPrint (i,j) = Io.printjob j.r j i oc
      in List.iter myPrint jlist_sorted;
      close_out oc
    with e ->
      begin
        close_out_noerr oc;
        raise e
      end
  end
