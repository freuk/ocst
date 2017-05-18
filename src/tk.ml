open Cmdliner
open Tracejoin

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

type copts = {debug : bool}
let copts debug = {debug}

let copts_t =
  let docs = copts_sect in
  let debug =
    let doc = "Give debug output." in
    Arg.(value & flag & info ["debug"] ~docs ~doc)
  in Term.(const copts $ debug)

let help_cmd =
  let topic =
    let doc = "The topic to get help on. `topics' lists the topics." in
    Arg.(value & pos 0 (some string) None & info [] ~docv:"TOPIC" ~doc)
  in
  let doc = "display help about toolkit commands" in
  let man =
    [`S "DESCRIPTION";
     `P "Prints help about toolkit"] @ help_secs
  in
  Term.(ret
          (const help $ Term.man_format $ Term.choice_names $topic)),
  Term.info "help" ~doc ~man

let feature_cmd =
  let docs = copts_sect
  in let state =
    let doc = "Input state file." in
    Arg.(required & pos 0 (some file) None & info [] ~docv:"STATE" ~doc)
  in let now =
    let doc = "Input timestamp file." in
    Arg.(required & pos 1 (some file) None & info [] ~docv:"NOW" ~doc)
  in let additional =
    let doc = "Input job file." in
    Arg.(required & pos 2 (some file) None & info [] ~docv:"JOBS" ~doc)
  in let output =
    let doc = "Output feature file." in
    Arg.(required & pos 3 (some string) None & info [] ~docv:"OUTPUT" ~doc)
  in
  let doc = "Builds feature vectors" in
  let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
  Term.(const Metrics.state_features $ state $ now $ additional $ output),
  Term.info "statefeatures" ~doc ~sdocs:docs ~man

let cmds = [help_cmd; feature_cmd]

let default_cmd =
  let doc = "a ocs toolkit" in
  let man = help_secs in
  Term.(ret (const  (`Help (`Pager, None)) )),
  Term.info "ocskit" ~version:"0.1" ~sdocs:copts_sect ~doc ~man

let () = match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1 | _ -> exit 0
