open Jobs
open Resources

(*mutable references to system datastructures*)
module type SystemParamSig  =sig
  val jobs : job_table
  val resourcestate : system_state
  val waitqueue : wait_queue
end
