open System

let optionize_string = function
  |"" -> None
  |s  -> Some s

let rec l2s = function
    [] -> ""
  | e::l -> (Printf.sprintf "%s,%f" (l2s l)) e

type args_perturbator = {
  random_seed                :  int;
  input_filename             :  string option;
  output_channel             :  out_channel;
}

type args_cleaner = {
  input_filename             :  string option;
  output_channel             :  out_channel;
}

type args_subtrace = {
  input_filename             :  string option;
  output_channel             :  out_channel;
  wid                        :  int;
  weekspan                   :  int;
}

let parse_subtrace_args () =
  let input_filename= ref "";
  and wid= ref 0;
  and weekspan= ref 0;
  and output_filename= ref "";
  in let speclist = [
    ("-i"     , Arg.Set_string input_filename             , "input swf trace.");
    ("-o"     , Arg.Set_string output_filename            , "output swf trace.");
    ("-wk"    , Arg.Set_int    wid                        , "initial week index.");
    ("-span"  , Arg.Set_int    weekspan                   , "week span");
  ]
  in let usage_msg = "Trace shuffler."
  in let () = Arg.parse speclist print_endline usage_msg;
  in {
    input_filename             = optionize_string !input_filename;
    output_channel             = open_out !output_filename;
    wid                        = !wid;
    weekspan                   = !weekspan;
  }

let parse_perturbator_args () =
  let input_filename= ref "";
  and random_seed= ref 0;
  and output_filename= ref "";
  in let speclist = [
    ("-i"     , Arg.Set_string input_filename             , "input swf trace.");
    ("-o"     , Arg.Set_string output_filename            , "output swf trace.");
    ("-seed"  , Arg.Set_int    random_seed                , "random seed.");
  ]
  in let usage_msg = "Trace shuffler."
  in let () = Arg.parse speclist print_endline usage_msg;
  in {
    input_filename             = optionize_string !input_filename;
    output_channel             = open_out !output_filename;
    random_seed                = !random_seed
  }

let parse_cleaner_args () =
  let input_filename= ref "";
  and output_filename= ref "";
  in let speclist = [
    ("-i"     , Arg.Set_string input_filename             , "input swf trace.");
    ("-o"     , Arg.Set_string output_filename            , "output swf trace.");
  ]
  in let usage_msg = "Ocaml backfilling simulator for evaluating randomized and preference-learning based approaches."
  in let () = Arg.parse speclist print_endline usage_msg;
  in {
    input_filename             = optionize_string !input_filename;
    output_channel             = open_out !output_filename;
  }

let printjob now j id output_channel =
  let wait_time = now-j.r
  in  Printf.fprintf output_channel
        "%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d\n"
        id        (* 1  Job Number                     *)
        j.r       (* 2  Submit Time                    *)
        wait_time (* 3  Wait Time                      *)
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

let hist_to_swf jobs fn_o hist =
  let g fn =
    let c = open_out fn
    in let f (i, t) = printjob t (Hashtbl.find jobs i) i c
    in try
      List.iter f hist;
      close_out c
    with e->
      close_out_noerr c;
      raise e
  in BatOption.may g fn_o

let printjob_shift r j id output_channel =
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

let job_to_swf_row now jobs id output_channel =
  printjob now (Hashtbl.find jobs id) id output_channel

let parse_job (row:string) : int * job =
  let l=Str.split (Str.regexp "[ ]+") row
  in let j= { r     = int_of_string (List.nth l 1);
              p     = int_of_string (List.nth l 3);
              p_est = int_of_string (List.nth l 8);
              q     = int_of_string (List.nth l 7);
              u     = int_of_string (List.nth l 11)}
  and id = int_of_string (List.hd l)
  in begin
    assert (id>=0);
    assert (j.r>=0);
    assert (j.p>0);
    assert (j.p_est>0);
    assert (j.p_est>=j.p);
    assert (j.q>0);
    id,j
  end

let parse_jobs fn =
  let jobs: job_table = Hashtbl.create 1000
  in let parse_row maxprocs header jobs (row: string): unit =
    if (String.contains row ';') then
      begin
        header:=!header@[row];
        try
          let re = Str.regexp_string "MaxProcs:"
          in let () = ignore (Str.search_forward re row 0)
          in let l = String.length row
          in let s = String.sub row 12 (l-12)
          in maxprocs := int_of_string(s)
        with Not_found -> ();
      end
    else
      let id,j=parse_job row
      in Hashtbl.add jobs id j;
  in let ic=open_in fn
  in let maxprocs = ref 0
  in let () =
    try
      let header = ref []
      in let line_stream_of_channel channel =
        Stream.from
          (fun _ ->
             try Some (input_line channel) with End_of_file -> None)
      in Stream.iter
           (parse_row maxprocs header jobs)
           (line_stream_of_channel ic);
         close_in ic;
    with e ->
      close_in_noerr ic;
      raise e;
  in jobs, !maxprocs

let do_io_perturbator () =
  let args = parse_perturbator_args ()
  in let jobs = BatRefList.empty ()
  in let parse_row maxprocs header jobs (row: string): unit =
    if (String.contains row ';') then
      Printf.fprintf args.output_channel "%s\n" row
    else
      let id,j=parse_job row
      in BatRefList.push jobs j;
  in let ic=open_in (BatOption.get_exn args.input_filename (Invalid_argument "input swf file"))
  in let maxprocs = ref 0
  in let () =
    try
      let header = ref []
      in let line_stream_of_channel channel =
        Stream.from
          (fun _ ->
             try Some (input_line channel) with End_of_file -> None)
      in Stream.iter
           (parse_row maxprocs header jobs)
           (line_stream_of_channel ic);
         close_in ic;
    with e ->
      close_in_noerr ic;
      raise e;
  in BatRefList.to_list jobs, args

let do_io_cleaner () =
  let args = parse_cleaner_args ()
  in let jobs = BatRefList.empty ()
  in let parse_row maxprocs header jobs (row: string): unit =
    if (String.contains row ';') then
      Printf.fprintf args.output_channel "%s\n" row
    else
      let id,j=parse_job row
      in BatRefList.push jobs j;
  in let ic=open_in (BatOption.get_exn args.input_filename (Invalid_argument "input swf file"))
  in let maxprocs = ref 0
  in let () =
    try
      let header = ref []
      in let line_stream_of_channel channel =
        Stream.from
          (fun _ ->
             try Some (input_line channel) with End_of_file -> None)
      in Stream.iter
           (parse_row maxprocs header jobs)
           (line_stream_of_channel ic);
         close_in ic;
    with e ->
      close_in_noerr ic;
      raise e;
  in BatRefList.to_list jobs, args

let do_io_subtrace () =
  let args = parse_subtrace_args ()
  in let jobs = BatRefList.empty ()
  in let parse_row maxprocs header jobs (row: string): unit =
    if (String.contains row ';') then ()
    else
      let id,j=parse_job row
      in BatRefList.push jobs j;
  in let ic=open_in (BatOption.get_exn args.input_filename (Invalid_argument "input swf file"))
  in let maxprocs = ref 0
  in let () =
    try
      let header = ref []
      in let line_stream_of_channel channel =
        Stream.from
          (fun _ ->
             try Some (input_line channel) with End_of_file -> None)
      in Stream.iter
           (parse_row maxprocs header jobs)
           (line_stream_of_channel ic);
         close_in ic;
    with e ->
      close_in_noerr ic;
      raise e;
  in BatRefList.to_list jobs, args
