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

let join_cmd =
  let docs = copts_sect
  in let swfOutput =
    let doc = "Output swf file." in
    Arg.(required & pos 0 (some string) None & info [] ~docv:"SWFOUTPUT" ~doc)
  in let swfList =
    let doc = "Input swf files." in
    Arg.(non_empty & pos_right 0 file [] & info [] ~docv:"SWFINPUT" ~doc)
  in
  let doc = "Joins traces" in
  let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
  Term.(const traceJoin $ swfOutput $ swfList),
  Term.info "join" ~doc ~sdocs:docs ~man

let sort_cmd =
  let docs = copts_sect
  in let swfOutput =
    let doc = "Output swf file." in
    Arg.(required & pos 1 (some string) None & info [] ~docv:"SWFOUTPUT" ~doc)
  in let swfList =
    let doc = "Input swf file." in
    Arg.(required & pos 0 (some file) None & info [] ~docv:"SWFINPUT" ~doc)
  in
  let doc = "Sorts trace according to submission time" in
  let man =
    [`S "DESCRIPTION";
     `P doc] @ help_secs
  in
  Term.(const Tracesort.trace_sort $ swfOutput $ swfList),
  Term.info "sort" ~doc ~sdocs:docs ~man

let cmds = [help_cmd; join_cmd; sort_cmd]

let default_cmd =
  let doc = "a swf toolkit" in
  let man = help_secs in
  Term.(ret (const  (`Help (`Pager, None)) )),
  Term.info "swfkit" ~version:"0.1" ~sdocs:copts_sect ~doc ~man

let () = match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1 | _ -> exit 0
