open Cmdliner
open Io
open Simulate

let copts_sect = "COMMON OPTIONS"
let help_secs = [
  `S copts_sect;
  `P "These options are common to all commands.";
                                  `S "MORE HELP";
                                  `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command.";`Noblank;]

let help man_format cmds topic = match topic with
  | None -> `Help (`Pager, None) (* help about the program. *)
  | Some topic ->
      let topics = "topics" :: "patterns" :: "environment" :: cmds in
      let conv, _ = Cmdliner.Arg.enum (List.rev_map (fun s -> (s, s)) topics) in
        match conv topic with
          | `Error e -> `Error (false, e)
          | `Ok t when t = "topics" -> List.iter print_endline topics; `Ok ()
          | `Ok t when List.mem t cmds -> `Help (man_format, Some t)
          | `Ok t ->
              let page = (topic, 7, "", "", ""), [`S topic; `P "Say something";] in
                `Ok (Cmdliner.Manpage.print man_format Format.std_formatter page)

let copts_t =
  let docs = copts_sect in
  let debug =
    let doc = "Give debug output." in
      Arg.(value & flag & info ["debug"] ~docs ~doc)
  in let seed =
    let doc = "Random seed value." in
      Arg.(value & opt int 0 & info ["seed"] ~docv:"SEED" ~docs ~doc)
  in let max_procs =
    let doc = "Enforce max_procs value." in
      Arg.(value & opt int 0 & info ["max_procs"] ~docv:"MAXPROCS" ~docs ~doc)
  in let initial_state =
    let doc = "Initial system state file" in
      Arg.(value & opt (some file) None & info ["initial_state"] ~docv:"INITSTATE" ~docs ~doc)
  in let additional_jobs =
    let doc = "Additional jobs." in
      Arg.(value & opt (some file) None & info ["additional_jobs"] ~docv:"ADDJOBS" ~docs ~doc)
  in let stats =
    let statdescs = String.concat ", " (List.map fst Statistics.allStats)
    in let doc = ("Specify statistics output. You may use a comma-separated list of arguments among: "^statdescs) in
      Arg.(value & opt (list ~sep:',' (enum Statistics.allStats)) [BatList.assoc "avgwait" Statistics.allStats] & info ["stat"] ~docv:"STAT" ~docs ~doc)
  in let swf_out =
    let doc = "Specify output swf file." in
      Arg.(value & opt (some string) None & info ["output"] ~docv:"OUTPUT" ~docs ~doc)
  in let swf_in =
    let doc = "Input swf file." in
      Arg.(required & pos 0 (some file) None & info [] ~docv:"SWFINPUT" ~doc)
  in Term.(const copts $ swf_in $ swf_out $ initial_state $ additional_jobs $ max_procs $ debug $ seed $ stats)

let help_cmd =
  let topic =
    let doc = "The topic to get help on. `topics' lists the topics." in
      Arg.(value & pos 0 (some string) None & info [] ~docv:"TOPIC" ~doc)
  in
  let doc = "display help about ocs and ocs commands" in
  let man =
    [`S "DESCRIPTION";
     `P "Prints help about ocs commands and other subjects..."] @ help_secs
  in
    Term.(ret
            (const help $ Term.man_format $ Term.choice_names $topic)),
    Term.info "help" ~doc ~man

let fixed_cmd =
  let docs = copts_sect
  and poldesc = String.concat ", " (List.map fst Metrics.criteriaList)
  in let reservation =
    let doc = ("Primary policy among: "^poldesc) in
      Arg.(value & opt (enum Metrics.criteriaList) (BatList.assoc "fcfs" Metrics.criteriaList) & info ["primary"] ~docv:"PRIMARY" ~doc)
  in let backfill =
    let doc = ("Backfilling policy among: "^poldesc) in
      Arg.(value & opt (enum Metrics.criteriaList) (BatList.assoc "fcfs" Metrics.criteriaList) & info ["backfill"] ~docv:"BACKFILL" ~doc)
  in
  let doc = "Simulates the run of a classic EASY backfilling scheduler using both static reservation and backfill policies." in
  let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
    Term.(const Simulate.fixed $ copts_t $ reservation $ backfill ),
    Term.info "fixed" ~doc ~sdocs:docs ~man

let mixed_cmd =
  let docs = copts_sect
  and poldesc = String.concat ", " (List.map fst Metrics.criteriaList)
   (*in let lalpha = *)
    (*let lcomma = Cmdliner.Arg.list ~sep:',' float*)
    (*in (some (t3 ~sep:';' lcomma lcomma lcomma))*)
    in let alpha =
      let doc =
        let d = List.length Metrics.features_job_plus
        in Printf.sprintf "Simple mixing parameters. Three comma-separated vectors of dimension %d, each separated by the character :" d
      in Arg.(value & opt (some (t3 ~sep:':' (list ~sep:',' float)(list ~sep:',' float)(list ~sep:',' float))) None & info ["alpha"] ~docv:"ALPHA" ~doc)
    in let alpha_threshold =
      let doc =
        let d = List.length Metrics.features_job_threshold
        in Printf.sprintf "Simple mixing parameters. Three comma-separated vectors of dimension %d, each separated by the character :" d
      in Arg.(value & opt (some (t3 ~sep:':' (list ~sep:',' float)(list ~sep:',' float)(list ~sep:',' float))) None & info ["alphathreshold"] ~docv:"ALPHA" ~doc)
    in let alpha_advanced =
      let doc =
        let d = List.length Metrics.features_job_advanced
        in Printf.sprintf "Advanced mixing parameters. Three comma-separated vectors of dimension %d, each separated by thecharacter :" d
      in Arg.(value & opt (some (t3 ~sep:':' (list ~sep:',' float)(list ~sep:',' float)(list ~sep:',' float))) None & info ["alphapoly"] ~docv:"ALPHAPOLY" ~doc)
    in let alpha_system =
      let doc =
        let d = List.length Metrics.features_system_job
        in Printf.sprintf "System mixing parameters. Three comma-separated vectors of dimension %d each, separated by thecharacter :" d
      in Arg.(value & opt (some (t3 ~sep:':' (list ~sep:',' float)(list ~sep:',' float)(list ~sep:',' float))) None & info ["alphasystem"] ~docv:"ALPHAPOLY" ~doc)
    in let feature_out =
      let doc = "Specify output feature file."
      in Arg.(value & opt (some string) None & info ["ft_out"] ~docv:"FTFILE" ~doc)
    in let backfill =
      let doc = ("Backfilling policy among: "^poldesc)
      in Arg.(value & opt (enum Metrics.criteriaList) (BatList.assoc "fcfs" Metrics.criteriaList) & info ["backfill"] ~docv:"BACKFILL" ~doc)
    in let proba =
      let doc = "Select a policy via sampling."
      in Arg.(value & flag & info ["proba"] ~docv:"PROBA" ~doc)
    in let sampling =
      let doc=
        let s = String.concat "," (List.map fst Easy.sampling_types)
        in Printf.sprintf "Sampling type. Available: %s" s
      in Arg.(value & opt (enum Easy.sampling_types) Easy.Softmax & info ["sampling"] ~docv:"SAMPLING" ~doc)
    in let doc = "Simulates the run of a classic EASY backfilling scheduler using a mixed reservation and a static backfill policy."
    in let man =
      [`S "DESCRIPTION";
       `P doc] @ help_secs
    in
      Term.(const Simulate.mixed $ copts_t $ backfill $ feature_out $ alpha $ alpha_threshold $ alpha_advanced $ alpha_system $ proba $sampling),
      Term.info "mixed" ~doc ~sdocs:docs ~man

let hysteresis_cmd =
  let docs = copts_sect
  in let thresholds =
      let doc = "Hysteresis thresholds"
      in Arg.(value & opt (pair float float) (1.,40.) & info ["thresholds"] ~docv:"THRESHALD" ~doc)
  in let policies =
    let default_pols = 
      let pl = List.map snd Metrics.criteriaList
      in ((List.nth pl 0),(List.nth pl 1))
    and doc = "Policies." 
    in Arg.(value & opt (pair (enum Metrics.criteriaList) (enum Metrics.criteriaList)) default_pols & info ["policies"] ~docv:"POLICIES" ~doc)
  in let doc = "Hysteresis primary policy choice."
    in let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
    Term.(const Simulate.hysteresis $ copts_t $ thresholds $ policies),
    Term.info "hysteresis" ~doc ~sdocs:docs ~man

let contextual_cmd =
  let docs = copts_sect
  in let period =
      let doc = "Period"
      in Arg.(value & opt int 86400 & info ["period"] ~docv:"PERIOD" ~doc)
  in let policies=
    let fullpol = List.map snd Metrics.criteriaList
    and doc = "Policies." 
    in Arg.(value & opt (list ~sep:',' (enum Metrics.criteriaList)) fullpol & info ["policies"] ~docv:"POLICIES" ~doc)
  in let ipc =
    let doc = "Ipc PAIR nanomsg channel for obtaining predictions." 
    in Arg.(value & opt string "ipc://ocs" & info ["ipc"] ~docv:"IPC" ~doc)
  in let doc = "Contextual policy choice."
    in let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
    Term.(const Simulate.contextual $ copts_t $ period $ policies $ ipc),
    Term.info "contextual" ~doc ~sdocs:docs ~man

let printstate_cmd =
  let docs = copts_sect
  in let state_out =
    let doc = "Specify output state files."
    in Arg.(value & opt (list ~sep:',' string) [] & info ["state_out"] ~docv:"OUTSTATE" ~doc)
  in let now_out =
    let doc = "Specify output timestamp files."
    in Arg.(value & opt (list ~sep:',' string) [] & info ["now_out"] ~docv:"OUTSTATE" ~doc)
  in let additional_out =
    let doc = "Specify output additional job files."
    in Arg.(value & opt (list ~sep:',' string) [] & info ["add_out"] ~docv:"OUTADD" ~doc)
  in let swfin_out =
    let doc = "Specify output swfin job files."
    in Arg.(value & opt (list ~sep:',' string) [] & info ["swfin_out"] ~docv:"SWFINOUT" ~doc)
  in let period =
    let doc = "Period"
    in Arg.(value & opt int 86400 & info ["period"] ~docv:"PERIOD" ~doc)
  in let policies=
    let doc = "Policies." 
    in Arg.(value & opt (list ~sep:',' (enum Metrics.criteriaList)) [BatList.assoc "fcfs" Metrics.criteriaList] & info ["policies"] ~docv:"POLICIES" ~doc)
  in let doc = "Simulates the run of a classic EASY backfilling with the FCFS primary/backfilling policy and prints periodical resimulation output."
    in let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
    Term.(const Simulate.printstate $ copts_t $ period $ state_out $ now_out $ additional_out $ swfin_out),
    Term.info "printstate" ~doc ~sdocs:docs ~man

let cmds = [fixed_cmd; mixed_cmd; contextual_cmd; printstate_cmd; hysteresis_cmd; help_cmd]

let default_cmd =
  let doc = "a backfilling simulator" in
  let man = help_secs in
    Term.(ret (const  (`Help (`Pager, None)) )),
    Term.info "ocs" ~version:"0.1" ~sdocs:copts_sect ~doc ~man

let () =
  match Term.eval_choice default_cmd cmds with
    | `Error _ -> exit 1 | _ -> exit 0
