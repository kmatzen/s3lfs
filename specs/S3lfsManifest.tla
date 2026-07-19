-------------------------- MODULE S3lfsManifest --------------------------
(***************************************************************************)
(* Models concurrent read-modify-write of .s3_manifest.yaml by N s3lfs     *)
(* processes.                                                              *)
(*                                                                         *)
(* Each process mirrors the real lifecycle:                                *)
(*                                                                         *)
(*   1. __init__ loads the manifest into memory with NO lock held          *)
(*      (core.py:188).                                                     *)
(*   2. The process later acquires the portalocker lock (core.py:252).     *)
(*   3. Some code paths re-read the manifest under the lock                *)
(*      (parallel_upload_chunked:1525, upload:1258, track_interleaved:2480) *)
(*      and some do not (remove_file:1284, remove_subtree:1776,            *)
(*      track_modified_files_cached:830, track_modified_files:1381).       *)
(*   4. The process writes back its own snapshot plus its mutation and     *)
(*      releases the lock.                                                 *)
(*                                                                         *)
(* Two independent knobs, giving a 2x2:                                    *)
(*                                                                         *)
(*   RELOAD      -- TRUE  models the code paths that re-read under lock.   *)
(*   SHARED_LOCK -- TRUE  models all processes agreeing on one lock file.  *)
(*                  FALSE models the CWD-relative temp_dir defect at       *)
(*                  core.py:158, where processes started from different    *)
(*                  directories guard one manifest with different locks,   *)
(*                  so mutual exclusion silently does not apply.           *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS Procs,        \* set of concurrent s3lfs processes
          Paths,        \* set of tracked file paths
          RELOAD,
          SHARED_LOCK,
          NoProc

ASSUME NoProc \notin Procs

VARIABLES manifest,    \* set of paths recorded in .s3_manifest.yaml
          expected,    \* ghost: manifest under an equivalent serial execution
          pstate,      \* per-process control state
          snapshot,    \* per-process in-memory copy of the manifest
          op,          \* per-process pending mutation
          lockHolder   \* lock name -> owning process (or NoProc)

vars == <<manifest, expected, pstate, snapshot, op, lockHolder>>

Kinds    == {"add", "remove"}
Ops      == Kinds \X Paths
NoOp     == <<"add", CHOOSE p \in Paths : TRUE>>

\* Every process shares one lock, or each guards its own -- the defect.
LockNames == {"global"} \cup Procs
LockOf(p) == IF SHARED_LOCK THEN "global" ELSE p

Apply(m, o) ==
    IF o[1] = "add" THEN m \cup {o[2]} ELSE m \ {o[2]}

TypeOK ==
    /\ manifest \subseteq Paths
    /\ expected \subseteq Paths
    /\ pstate \in [Procs -> {"init", "ready", "acquired", "mutated", "done"}]
    /\ snapshot \in [Procs -> SUBSET Paths]
    /\ op \in [Procs -> Ops]
    /\ lockHolder \in [LockNames -> Procs \cup {NoProc}]

Init ==
    /\ manifest = {}
    /\ expected = {}
    /\ pstate = [p \in Procs |-> "init"]
    /\ snapshot = [p \in Procs |-> {}]
    /\ op = [p \in Procs |-> NoOp]
    /\ lockHolder = [l \in LockNames |-> NoProc]

(***************************************************************************)
(* 1. Unlocked load at construction time (core.py:188).                    *)
(***************************************************************************)
PLoad(p) ==
    /\ pstate[p] = "init"
    /\ snapshot' = [snapshot EXCEPT ![p] = manifest]
    /\ \E o \in Ops : op' = [op EXCEPT ![p] = o]
    /\ pstate' = [pstate EXCEPT ![p] = "ready"]
    /\ UNCHANGED <<manifest, expected, lockHolder>>

(***************************************************************************)
(* 2. Acquire the lock this process believes guards the manifest.          *)
(***************************************************************************)
PAcquire(p) ==
    /\ pstate[p] = "ready"
    /\ lockHolder[LockOf(p)] = NoProc
    /\ lockHolder' = [lockHolder EXCEPT ![LockOf(p)] = p]
    /\ pstate' = [pstate EXCEPT ![p] = "acquired"]
    /\ UNCHANGED <<manifest, expected, snapshot, op>>

(***************************************************************************)
(* 3. Optionally re-read under the lock.  Kept as its own step so that     *)
(*    without real mutual exclusion two processes can both reload before   *)
(*    either saves -- which is exactly how the lock defect defeats the     *)
(*    reload discipline.                                                   *)
(***************************************************************************)
PReload(p) ==
    /\ pstate[p] = "acquired"
    /\ snapshot' = [snapshot EXCEPT ![p] = IF RELOAD THEN manifest ELSE snapshot[p]]
    /\ pstate' = [pstate EXCEPT ![p] = "mutated"]
    /\ UNCHANGED <<manifest, expected, op, lockHolder>>

(***************************************************************************)
(* 4. save_manifest: write back this process's snapshot plus its mutation. *)
(*    The ghost variable records what a serial execution would have got.   *)
(***************************************************************************)
PSave(p) ==
    /\ pstate[p] = "mutated"
    /\ manifest' = Apply(snapshot[p], op[p])
    /\ expected' = Apply(expected, op[p])
    /\ lockHolder' = [lockHolder EXCEPT ![LockOf(p)] = NoProc]
    /\ pstate' = [pstate EXCEPT ![p] = "done"]
    /\ UNCHANGED <<snapshot, op>>

(***************************************************************************)
(* Every process has finished. Modelled as an explicit stuttering step so   *)
(* that TLC's deadlock check stays ON: without it, normal termination is    *)
(* indistinguishable from a genuine deadlock, and switching the check off   *)
(* would also hide one.                                                     *)
(***************************************************************************)
Terminating ==
    /\ \A p \in Procs : pstate[p] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E p \in Procs : PLoad(p) \/ PAcquire(p) \/ PReload(p) \/ PSave(p)
    \/ Terminating

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* Every committed mutation survives.  A violation is a lost update: one   *)
(* process silently erasing another's committed work from the manifest,    *)
(* orphaning its S3 objects.                                               *)
(***************************************************************************)
NoLostUpdate == manifest = expected

MutualExclusion ==
    \A p1, p2 \in Procs :
        (p1 # p2 /\ pstate[p1] \in {"acquired", "mutated"}
                 /\ pstate[p2] \in {"acquired", "mutated"})
            => FALSE

=============================================================================
