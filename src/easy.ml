(***************************Primary and backfilling selectors***********************)

(**** Primary Selector ****)
module type Primary = sig
  val reorder :
    Engine.system ->                (*system state before primary jobs are started*)
    now:int ->                      (*now*)
    int list
end

module MakeGreedyPrimary
  (C:Metrics.Criteria)
  : Primary =
struct
  let reorder s now =
    let comp i1 i2 = compare (C.criteria s now i2) (C.criteria s now i1)
    in List.sort comp
end

(**** Backfilling Selector ****)
module type Secondary = sig
  val selector :
    system:system ->                (*full state of the system*)
    now:int ->                      (*now*)
    backfillable:int list ->        (*backfillable jobs*)
    reservationT:int ->             (*time of the reservation*)
    reservationID:int ->            (*reservation job*)
    reservationFree:int ->          (*free resources above the reservation job*)
    int
end

module MakeGreedySecondary
  (C:Metrics.Criteria)
  : Secondary =
struct
  let selector
        ~system:s
        ~now:now
        ~backfillable:bfable
        ~reservationT:restime
        ~reservationID:resjob
        ~reservationFree:resres =
    let rec argmax v ip = function
      |i::is when v <= (C.criteria s now i) -> argmax (C.criteria s now i) i is
      |i::is -> argmax v ip is
      |[] -> ip
    and id = List.hd bfable
    in argmax (C.criteria s now id) id bfable
end

(************************************ Scheduler ***************************************)

module type SchedulerParam = sig
  val jobs : jobTable
end

module type Scheduler = sig
  val schedule : int -> Engine.system -> int list
end

module MakeEasyScheduler
  (Primary:Primary)
  (Secondary:Secondary)
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

       let reserve now free id running decision=
         let job = find P.jobs id
         in let make_projected_end i = (now + (find P.jobs i).p_est ,i)
         in let projectedEndList = List.map make_projected_end ((List.map snd running) @ decision)
         in let sortedProjectedEndList = List.sort (fun (t,_) (t',_) -> compare t t') projectedEndList
         in let rec fits f l = match l with
           | [] -> failwith "Internal error: resource usage data inconsistent."
           | (t,i)::_ when f+(find P.jobs i).q >= job.q -> (t-now, f+(find P.jobs i).q-job.q)
           | (t,i)::tis -> fits (f+(find P.jobs i).q) tis
         in fits free sortedProjectedEndList

       let schedule now s =
         assert (free >= 0);
         match get_easy s.free (Primary.reorder s now) with
           | decision , _        , None     , _    ->  decision
           | decision , _        , sid      , []   ->  decision
           | decision , free'    , Some id  , rest ->
               let () = assert (free' >= 0);
               in let t, f_r = reserve now free' id system.running decision
               in let () = assert (t>0)
               in let rec picknext decisionlist f f' idlist =
                 assert (f >= 0);
                 assert (f'>= 0);
                 let bfable = get_backfillable  f f' t idlist
                 in match bfable with
                   | [] -> decisionlist
                   | _::_ ->
                       let idpicked = Secondary.selector s now bfable t id f_r
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
module MakeEasyGreedy(CR:Criteria)(CB:Criteria)(P:SchedulerParam) =
  MakeEasyScheduler(MakeGreedyPrimary(P)(CR))(MakeGreedySecondary(CB)(P))(P)

(*module MakeEasyEpsRandGreedy (E:EpsGreedyParameter)(CR:Criteria)(CB:Criteria)(P:SchedulerParam) =*)
(*MakeEasyScheduler(MakeGreedyPrimary(P)(CR))(MakeEpsGreedySecondary(E)(CB)(P))(P)*)

(*module MakeEasyRandom (CR:Criteria)(P: SchedulerParam) =*)
(*MakeEasyScheduler(MakeGreedyPrimary(P)(CR))(RandomSecondary)(P)*)

(*module MakeEasyHRandom(C1:Criteria)(C2:Criteria)(CR:Criteria)(P: SchedulerParam) =*)
(*MakeEasyScheduler(MakeGreedyPrimary(P)(CR))(MakeRandomHeuristicSecondary(C1)(C2)(P))(P)*)


(*module MakeBernouilliPrimary*)
(*(BP:BernouilliReservatorParam)*)
(*(P:SchedulerParam)*)
(*(R1:Primary)*)
(*(R2:Primary)*)
(*: Primary*)
(*= struct*)
  (*let reorder now l =*)
    (*let r = Random.float 1.*)
    (*in if r> BP.p then R1.reorder now l else R2.reorder now l *)
(*end*)


(*module type BernouilliParameter = sig*)
  (*val p : float*)
(*end*)

(*module MakeBernouilliHeuristicSecondary*)
(*(W:BernouilliParameter)*)
(*(C1:Criteria)*)
(*(C2:Criteria)*)
(*(P:SchedulerParam)*)
(*:Secondary = struct*)
  (*module H1 = MakeGreedySecondary(C1)(P)*)
  (*module H2 = MakeGreedySecondary(C2)(P)*)

  (*let selector now bfable restime resjob resres =*)
    (*let r = Random.float 1.*)
    (*in let sel = if r<W.p then*)
      (*H1.selector*)
    (*else*)
      (*H2.selector*)
    (*in sel now bfable restime resjob resres*)
(*end*)


(*module MakeEasyBernouilli(C1:Criteria)(C2:Criteria)(B:BernouilliReservatorParam)(CB:Criteria)(P: SchedulerParam) =*)
(*MakeEasyScheduler(MakeBernouilliPrimary(B)(P)(MakeGreedyPrimary(P)(C1))(MakeGreedyPrimary(P)(C2)))(MakeGreedySecondary(CB)(P))(P)*)


(*module type BernouilliReservatorParam = sig*)
  (*val p : float*)
(*end*)


(*module MakeRandomizedSecondary(C:Criteria)(P:SchedulerParam)*)
(*:Secondary*)
(*=struct*)
  (*(* pick a random id from this list -- the non-emply list*)
    (*should be in format [(proba,id);..] and the probabilities*)
    (*should add to 1. *)*)
  (*let pick_random_weighted (l: (float*int) list) : int =*)
    (*let r = Random.float (float_of_int 1)*)
    (*in let rec nextrandom s i' = function*)
      (*|(p,i)::is when s +. p >= r -> i*)
      (*|(p,i)::is -> nextrandom (s +. p) i is*)
      (*|[]-> i'*)
    (*in nextrandom 0. (snd (List.hd l)) l*)

  (*let selector (now:int) (bfable:int list) (restime:int) resjob resres =*)
    (*let weights : float list = List.map (C.criteria P.jobs now) bfable*)
    (*in let minweight = BatList.min weights*)
    (*in let scaledweights = List.map (fun x-> x-.minweight) weights*)
    (*in let sum : float = List.fold_left (fun acc x -> acc +. x) 0. scaledweights*)
    (*in let nd = List.map2 (fun x y -> (x /. sum, y)) scaledweights bfable*)
    (*in pick_random_weighted nd*)
(*end*)

(*module MakeEasyRandomizedGreedy (CR:Criteria)(CB:Criteria)(P:SchedulerParam) =*)
(*MakeEasyScheduler(MakeGreedyPrimary(P)(CR))(MakeRandomizedSecondary(CB)(P))(P)*)

(*module MakeEpsGreedySecondary(E:EpsGreedyParameter)(C:Criteria)(P:SchedulerParam)*)
(*:Secondary*)
(*= struct*)
  (*let selector (now:int) (bfable:int list) (restime:int) resjob resres =*)
    (*let id = List.hd bfable*)
    (*in let rec argmax v ip = function*)
    (*|i::is when v <= (C.criteria P.jobs now i) -> argmax (C.criteria P.jobs now i) i is*)
    (*|i::is -> argmax v ip is*)
    (*|[] -> ip*)
    (*in let r = Random.float (float_of_int 1)*)
    (*in if r<E.epsilon*)
    (*then*)
      (*List.nth bfable (Random.int (List.length bfable))*)
    (*else*)
      (*argmax (C.criteria P.jobs now id) id bfable*)
(*end*)

(*module type EpsGreedyParameter = sig*)
  (*val epsilon : float*)
(*end*)

(*module RandomSecondary : Secondary*)
(*= struct*)
  (*let selector _ bfable _ _ _ =*)
    (*let i =  Random.int (List.length bfable)*)
    (*in List.nth bfable i*)
(*end*)

(*module MakeRandomHeuristicSecondary*)
(*(C1:Criteria)*)
(*(C2:Criteria)*)
(*(P:SchedulerParam)*)
(*: Secondary*)
(*=struct*)
  (*module H1 = MakeGreedySecondary(C1)(P)*)
  (*module H2 = MakeGreedySecondary(C2)(P)*)

  (*let selector now bfable restime resjob resres =*)
    (*let h = Random.bool ()*)
    (*in let sel = if h then*)
      (*H1.selector*)
    (*else*)
      (*H2.selector*)
    (*in sel now bfable restime resjob resres*)
(*end*)
