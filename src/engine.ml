open Events
open Easy
open Jobs
open Io
open Resources
open Binary_heap

module type SimulatorParamSig = sig
  val eventheap : event_heap
  val output_channel : out_channel option
  val output_channel_bf : out_channel option
end

module type SimulatorSig = sig
  type outputStat
  val simulate : unit -> unit
end

module type StatSig = sig
  type outputStat
  val incStat : int   (* time now*)
  -> int list         (*actual jobs to start now*)
  -> unit
  val getStat : unit -> outputStat
  val getN : unit -> int
  val reset : unit -> unit
end

module type HookSig = sig
  val hook : int -> unit
end

module NoHook = struct
  let hook _ = ()
end

module MakeSimulator
(St:StatSig)
(Sch:SchedulerSig)
(P:SimulatorParamSig)
(S:SystemParamSig)
(Hk:HookSig)
:SimulatorSig
with type outputStat = St.outputStat
= struct
  type outputStat = St.outputStat

  let execute_unload_events () =
    begin
      let e = pop_event P.eventheap
      in let event_list = ref [e]
      in while is_next_event_at_time e.time P.eventheap do
        event_list := !event_list@[pop_event P.eventheap]
      done;
      e.time, !event_list
    end

  let execute_events =
    let execute_event event =
      match event.event_type with
      | Submit -> enqueue_job event.id S.waitqueue
      | End -> free_resources S.jobs event.id S.resourcestate
    in List.iter execute_event

  let execute_orders now =
    let execute_order id =
      begin
        dequeue_job id S.waitqueue;
        use_resources S.jobs id now S.resourcestate;
        start_job S.jobs now id P.eventheap;
        BatOption.may (job_to_swf_row now S.jobs id) P.output_channel;
      end
    in List.iter execute_order

  let simulate () =
    while not (Events.is_empty P.eventheap) do
        let now,event_list = execute_unload_events ()
        in begin
          execute_events event_list;
          let decisions= Sch.schedule now
          in
            St.incStat now decisions;
            execute_orders now decisions;
            Hk.hook now;
            if (not (BatList.is_empty decisions)) 
            then BatOption.may (
              fun x-> begin
                Printf.fprintf x "%d" now;
                List.iter (fun i -> Printf.fprintf x " %d" i) decisions;
                Printf.fprintf x "%s" "\n";
              end
            ) P.output_channel_bf 
        end
    done
end
