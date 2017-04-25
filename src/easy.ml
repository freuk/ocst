open Metrics
open Jobs
open Events
open Resources
open System

module type SchedulerSig = sig
  val schedule : int -> int list
end

(*Job selector for easy algoritmm.*)
module type SelectorSig = sig
  val selector :
    int -> (*now*)
    int list -> (*backfillable jobs*)
    int -> (*time of the reservation*)
    int -> (*reservation job*)
    int -> (*free resources above the reservation job*)
    int
end

module type ReservationSelector = sig
  val reorder : int -> int list -> int list
end

module MakeReservationSelector
(P:SystemParamSig)
(C:CriteriaSig)
: ReservationSelector
= struct
  let comp now i1 i2 = compare (C.criteria P.jobs now i2) (C.criteria P.jobs now i1)
  let reorder now = List.sort (comp now)
end

module type BernouilliReservatorParam = sig
  val p : float
end

module MakeBernouilliReservationSelector
(BP:BernouilliReservatorParam)
(P:SystemParamSig)
(R1:ReservationSelector)
(R2:ReservationSelector)
: ReservationSelector
= struct
  let reorder now l =
    let r = Random.float 1.
    in if r> BP.p then R1.reorder now l else R2.reorder now l 
end

(*main functor for building easy backfilling algorithms*)
module MakeEasyScheduler
(SelectR:ReservationSelector)
(SelectB:SelectorSig)
(P:SystemParamSig)
: SchedulerSig
= struct

  let get_easy jobs free waitqueue =
    let rec easy decision free = function
      |[] -> (decision,free,None,[])
      | i :: is when free>=(find jobs i).q -> easy (decision@[i]) (free-(find jobs i).q) is
      | i :: is -> (decision,free,Some i,is)
    in easy  [] free waitqueue

  let get_backfillable jobs r r' t idlist =
    assert (t>0);
    let p i =
      let j = find jobs i
      in j.q <= (min r' r) || (j.q <= r && j.p_est <= t)
    in List.filter p idlist

  let reserve jobs now free id resourcestate decision=
    let job = find jobs id
    in let make_projected_end i = (now + (find jobs i).p_est ,i)
    in let projected_end_list = resourcestate.jobs_running_list @ (List.map make_projected_end decision)
    in let sorted_projected_end_list = List.sort (fun (t,_) (t',_) -> compare t t') projected_end_list
    in let rec fits f l = match l with
      | [] -> raise ( Failure "Internal error: resource usage data inconsistent." )
      | (t,i)::_ when f+(find jobs i).q >= job.q -> (t-now, f+(find jobs i).q-job.q)
      | (t,i)::tis -> fits (f+(find jobs i).q) tis
    in fits free sorted_projected_end_list

  let schedule now =
    let free = Resources.currently_free_resources P.resourcestate
    in let () = assert (free >= 0)
    in match get_easy P.jobs free (SelectR.reorder now !P.waitqueue) with
       | decision , _        , None     , _    ->  decision
       | decision , _        , sid      , []   ->  decision
       | decision , free'    , Some id  , rest ->
           let () = assert (free' >= 0);
           in let t, f_r = reserve P.jobs now free' id P.resourcestate decision
           in let () = assert (t>0)
           in let rec picknext decisionlist f f' idlist =
               assert (f >= 0);
               assert (f'>= 0);
               let bfable = get_backfillable P.jobs f f' t idlist
               in match bfable with
                 | [] -> decisionlist
                 | _::_ ->
                    let idpicked = SelectB.selector now bfable t id f_r
                    in let j = find P.jobs idpicked
                    in let res = j.q
                    in let runt = j.p_est
                    in let new_f  = f-res
                    in let new_f' = if (runt > t) then f'-res else f'
                    in let remaining_ids = List.filter (fun id -> id != idpicked) bfable
                    in picknext (decisionlist@[(idpicked)]) new_f new_f' remaining_ids
           in let backfill_decision= picknext [] free' f_r rest
           in (decision @ backfill_decision)
  end

module MakeGreedySelector(C:CriteriaSig)(P:SystemParamSig): SelectorSig
=struct

  let selector (now:int) (bfable:int list) (restime:int ) resjob resres =
    let id = List.hd bfable
    in let rec argmax v ip = function
    |i::is when v <= (C.criteria P.jobs now i) -> argmax (C.criteria P.jobs now i) i is
    |i::is -> argmax v ip is
    |[] -> ip
    in argmax (C.criteria P.jobs now id) id bfable
end

module MakeRandomizedSelector(C:CriteriaSig)(P:SystemParamSig)
:SelectorSig
=struct
  (* pick a random id from this list -- the non-emply list
    should be in format [(proba,id);..] and the probabilities
    should add to 1. *)
  let pick_random_weighted (l: (float*int) list) : int =
    let r = Random.float (float_of_int 1)
    in let rec nextrandom s i' = function
      |(p,i)::is when s +. p >= r -> i
      |(p,i)::is -> nextrandom (s +. p) i is
      |[]-> i'
    in nextrandom 0. (snd (List.hd l)) l

  let selector (now:int) (bfable:int list) (restime:int) resjob resres =
    let weights : float list = List.map (C.criteria P.jobs now) bfable
    in let minweight = BatList.min weights
    in let scaledweights = List.map (fun x-> x-.minweight) weights
    in let sum : float = List.fold_left (fun acc x -> acc +. x) 0. scaledweights
    in let nd = List.map2 (fun x y -> (x /. sum, y)) scaledweights bfable
    in pick_random_weighted nd
end

module type EpsGreedyParameterSig = sig
  val epsilon : float
end

module MakeEpsGreedySelector(E:EpsGreedyParameterSig)(C:CriteriaSig)(P:SystemParamSig)
:SelectorSig
= struct
  let selector (now:int) (bfable:int list) (restime:int) resjob resres =
    let id = List.hd bfable
    in let rec argmax v ip = function
    |i::is when v <= (C.criteria P.jobs now i) -> argmax (C.criteria P.jobs now i) i is
    |i::is -> argmax v ip is
    |[] -> ip
    in let r = Random.float (float_of_int 1)
    in if r<E.epsilon
    then
      List.nth bfable (Random.int (List.length bfable))
    else
      argmax (C.criteria P.jobs now id) id bfable
end

module RandomSelector : SelectorSig
= struct
  let selector _ bfable _ _ _ =
    let i =  Random.int (List.length bfable)
    in List.nth bfable i
end

module MakeRandomHeuristicSelector
(C1:CriteriaSig)
(C2:CriteriaSig)
(P:SystemParamSig)
: SelectorSig
=struct
  module H1 = MakeGreedySelector(C1)(P)
  module H2 = MakeGreedySelector(C2)(P)

  let selector now bfable restime resjob resres =
    let h = Random.bool ()
    in let sel = if h then
      H1.selector
    else
      H2.selector
    in sel now bfable restime resjob resres
end

module type BernouilliParameter = sig
  val p : float
end

module MakeBernouilliHeuristicSelector
(W:BernouilliParameter)
(C1:CriteriaSig)
(C2:CriteriaSig)
(P:SystemParamSig)
:SelectorSig = struct
  module H1 = MakeGreedySelector(C1)(P)
  module H2 = MakeGreedySelector(C2)(P)

  let selector now bfable restime resjob resres =
    let r = Random.float 1.
    in let sel = if r<W.p then
      H1.selector
    else
      H2.selector
    in sel now bfable restime resjob resres
end

(*examples:*)
module MakeEasyGreedy (CR:CriteriaSig)(CB:CriteriaSig)(P:SystemParamSig) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeGreedySelector(CB)(P))(P)

module MakeEasyRandomizedGreedy (CR:CriteriaSig)(CB:CriteriaSig)(P:SystemParamSig) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeRandomizedSelector(CB)(P))(P)

module MakeEasyEpsRandGreedy (E:EpsGreedyParameterSig)(CR:CriteriaSig)(CB:CriteriaSig)(P:SystemParamSig) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeEpsGreedySelector(E)(CB)(P))(P)

module MakeEasyRandom (CR:CriteriaSig)(P: SystemParamSig) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(RandomSelector)(P)

module MakeEasyBernouilli(C1:CriteriaSig)(C2:CriteriaSig)(B:BernouilliReservatorParam)(CB:CriteriaSig)(P: SystemParamSig) =
MakeEasyScheduler(MakeBernouilliReservationSelector(B)(P)(MakeReservationSelector(P)(C1))(MakeReservationSelector(P)(C2)))(MakeGreedySelector(CB)(P))(P)

module MakeEasyHRandom(C1:CriteriaSig)(C2:CriteriaSig)(CR:CriteriaSig)(P: SystemParamSig) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeRandomHeuristicSelector(C1)(C2)(P))(P)
