open Metrics

(***************************Primary and backfilling selectors***********************)

module type Selector = sig
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
(P:SystemParam)
(C:Criteria)
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
(P:SystemParam)
(R1:ReservationSelector)
(R2:ReservationSelector)
: ReservationSelector
= struct
  let reorder now l =
    let r = Random.float 1.
    in if r> BP.p then R1.reorder now l else R2.reorder now l 
end

module MakeGreedySelector(C:Criteria)(P:SystemParam): Selector
=struct

  let selector (now:int) (bfable:int list) (restime:int ) resjob resres =
    let id = List.hd bfable
    in let rec argmax v ip = function
    |i::is when v <= (C.criteria P.jobs now i) -> argmax (C.criteria P.jobs now i) i is
    |i::is -> argmax v ip is
    |[] -> ip
    in argmax (C.criteria P.jobs now id) id bfable
end

module MakeRandomizedSelector(C:Criteria)(P:SystemParam)
:Selector
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

module type EpsGreedyParameter = sig
  val epsilon : float
end

module MakeEpsGreedySelector(E:EpsGreedyParameter)(C:Criteria)(P:SystemParam)
:Selector
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

module RandomSelector : Selector
= struct
  let selector _ bfable _ _ _ =
    let i =  Random.int (List.length bfable)
    in List.nth bfable i
end

module MakeRandomHeuristicSelector
(C1:Criteria)
(C2:Criteria)
(P:SystemParam)
: Selector
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
(C1:Criteria)
(C2:Criteria)
(P:SystemParam)
:Selector = struct
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

(************************************ Scheduler ***************************************)

module type SchedulerParam = sig
  val jobs : jobTable
end

module type Scheduler = sig
  val schedule : int -> Engine.system -> int list
end

module MakeEasyScheduler
(SelectR:ReservationSelector)
(SelectB:Selector)
(P:SchedulerParam)
: Scheduler
= struct
  let get_easy free waitqueue =
    let rec easy decision free = function
      |[] -> (decision,free,None,[])
      | i :: is when free>=(find P.jobs i).q -> easy (decision@[i]) (free-(find jobs i).q) is
      | i :: is -> (decision,free,Some i,is)
    in easy  [] free waitqueue

  let get_backfillable r r' t idlist =
    assert (t>0);
    let p i =
      let j = find P.jobs i
      in j.q <= (min r' r) || (j.q <= r && j.p_est <= t)
    in List.filter p idlist

  let reserve now free id resourcestate decision=
    let job = find P.jobs id
    in let make_projected_end i = (now + (find P.jobs i).p_est ,i)
    in let projected_end_list = resourcestate.jobs_running_list @ (List.map make_projected_end decision)
    in let sorted_projected_end_list = List.sort (fun (t,_) (t',_) -> compare t t') projected_end_list
    in let rec fits f l = match l with
      | [] -> raise ( Failure "Internal error: resource usage data inconsistent." )
      | (t,i)::_ when f+(find P.jobs i).q >= job.q -> (t-now, f+(find P.jobs i).q-job.q)
      | (t,i)::tis -> fits (f+(find P.jobs i).q) tis
    in fits free sorted_projected_end_list

  let schedule now s =
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

(*examples:*)
module MakeEasyGreedy (CR:Criteria)(CB:Criteria)(P:SystemParam) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeGreedySelector(CB)(P))(P)

module MakeEasyRandomizedGreedy (CR:Criteria)(CB:Criteria)(P:SystemParam) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeRandomizedSelector(CB)(P))(P)

module MakeEasyEpsRandGreedy (E:EpsGreedyParameter)(CR:Criteria)(CB:Criteria)(P:SystemParam) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeEpsGreedySelector(E)(CB)(P))(P)

module MakeEasyRandom (CR:Criteria)(P: SystemParam) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(RandomSelector)(P)

module MakeEasyBernouilli(C1:Criteria)(C2:Criteria)(B:BernouilliReservatorParam)(CB:Criteria)(P: SystemParam) =
MakeEasyScheduler(MakeBernouilliReservationSelector(B)(P)(MakeReservationSelector(P)(C1))(MakeReservationSelector(P)(C2)))(MakeGreedySelector(CB)(P))(P)

module MakeEasyHRandom(C1:Criteria)(C2:Criteria)(CR:Criteria)(P: SystemParam) =
MakeEasyScheduler(MakeReservationSelector(P)(CR))(MakeRandomHeuristicSelector(C1)(C2)(P))(P)
