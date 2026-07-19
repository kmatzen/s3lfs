------------------------- MODULE S3lfsNamespace -------------------------
(***************************************************************************)
(* A replacement for the SHARED_LOCK knob in S3lfsManifest.                *)
(*                                                                         *)
(* S3lfsManifest took mutual exclusion as a given (SHARED_LOCK \in BOOLEAN) *)
(* and could therefore only score fixes that were already thought of. Here *)
(* the lock's identity is DERIVED by resolving a (base, name) pair, the    *)
(* way the code actually computes it, and mutual exclusion falls out of    *)
(* whether two processes resolve to the same file.                         *)
(*                                                                         *)
(* The model also carries the filesystem namespace, because s3lfs's own    *)
(* metadata lives in the tree that s3lfs enumerates. _resolve_filesystem_  *)
(* paths uses rglob("*") with no exclusion list (core.py:1878), so any     *)
(* internal file inside the enumerated subtree is picked up as a user file.*)
(*                                                                         *)
(* Directory layout:                                                       *)
(*                                                                         *)
(*     R                 repository root, holds .s3_manifest.yaml          *)
(*     |-- R_temp        R/.s3lfs_temp                                     *)
(*     +-- S             a subdirectory a process may be started from      *)
(*         +-- S_temp    S/.s3lfs_temp                                     *)
(*                                                                         *)
(* LOCK_POLICY selects where the lock is resolved, matching the three      *)
(* placements actually considered:                                         *)
(*                                                                         *)
(*   "cwd_temp"       <cwd>/.s3lfs_temp/.s3lfs.lock   -- original defect   *)
(*   "manifest_root"  <manifest dir>/.s3lfs.lock      -- first attempt     *)
(*   "manifest_temp"  <manifest dir>/.s3lfs_temp/...  -- shipped           *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS Procs, Paths, LOCK_POLICY, TRACK_TARGET, RELOAD, NoProc

ASSUME NoProc \notin Procs
ASSUME LOCK_POLICY \in {"cwd_temp", "manifest_root", "manifest_temp"}

Dirs        == {"R", "S", "R_temp", "S_temp"}
WorkDirs    == {"R", "S"}          \* directories a process can be started from
ManifestDir == "R"

ASSUME TRACK_TARGET \in Dirs

\* Everything at or beneath a directory -- what rglob("*") reaches.
Under(d) ==
    CASE d = "R"      -> {"R", "S", "R_temp", "S_temp"}
      [] d = "S"      -> {"S", "S_temp"}
      [] d = "R_temp" -> {"R_temp"}
      [] d = "S_temp" -> {"S_temp"}

TempOf(d) == IF d = "R" THEN "R_temp" ELSE "S_temp"

VARIABLES manifest, expected, pstate, snapshot, op, lockHolder, cwd

vars == <<manifest, expected, pstate, snapshot, op, lockHolder, cwd>>

Kinds == {"add", "remove"}
Ops   == Kinds \X Paths
NoOp  == <<"add", CHOOSE p \in Paths : TRUE>>

(***************************************************************************)
(* Lock identity, derived rather than assumed.  Two processes exclude one  *)
(* another exactly when these resolve to the same file.                    *)
(***************************************************************************)
LockDir(p) ==
    CASE LOCK_POLICY = "cwd_temp"      -> TempOf(cwd[p])
      [] LOCK_POLICY = "manifest_root" -> ManifestDir
      [] LOCK_POLICY = "manifest_temp" -> TempOf(ManifestDir)

LockFile(p) == <<LockDir(p), "lock">>
LockNames   == {<<d, "lock">> : d \in Dirs}

Apply(m, o) == IF o[1] = "add" THEN m \cup {o[2]} ELSE m \ {o[2]}

TypeOK ==
    /\ manifest \subseteq Paths
    /\ expected \subseteq Paths
    /\ pstate \in [Procs -> {"init", "ready", "acquired", "mutated", "done"}]
    /\ snapshot \in [Procs -> SUBSET Paths]
    /\ op \in [Procs -> Ops]
    /\ lockHolder \in [LockNames -> Procs \cup {NoProc}]
    /\ cwd \in [Procs -> WorkDirs]

Init ==
    /\ manifest = {}
    /\ expected = {}
    /\ pstate = [p \in Procs |-> "init"]
    /\ snapshot = [p \in Procs |-> {}]
    /\ op = [p \in Procs |-> NoOp]
    /\ lockHolder = [l \in LockNames |-> NoProc]
    \* Processes may be started from any working directory.
    /\ cwd \in [Procs -> WorkDirs]

PLoad(p) ==
    /\ pstate[p] = "init"
    /\ snapshot' = [snapshot EXCEPT ![p] = manifest]
    /\ \E o \in Ops : op' = [op EXCEPT ![p] = o]
    /\ pstate' = [pstate EXCEPT ![p] = "ready"]
    /\ UNCHANGED <<manifest, expected, lockHolder, cwd>>

PAcquire(p) ==
    /\ pstate[p] = "ready"
    /\ lockHolder[LockFile(p)] = NoProc
    /\ lockHolder' = [lockHolder EXCEPT ![LockFile(p)] = p]
    /\ pstate' = [pstate EXCEPT ![p] = "acquired"]
    /\ UNCHANGED <<manifest, expected, snapshot, op, cwd>>

PReload(p) ==
    /\ pstate[p] = "acquired"
    /\ snapshot' = [snapshot EXCEPT ![p] = IF RELOAD THEN manifest ELSE snapshot[p]]
    /\ pstate' = [pstate EXCEPT ![p] = "mutated"]
    /\ UNCHANGED <<manifest, expected, op, lockHolder, cwd>>

PSave(p) ==
    /\ pstate[p] = "mutated"
    /\ manifest' = Apply(snapshot[p], op[p])
    /\ expected' = Apply(expected, op[p])
    /\ lockHolder' = [lockHolder EXCEPT ![LockFile(p)] = NoProc]
    /\ pstate' = [pstate EXCEPT ![p] = "done"]
    /\ UNCHANGED <<snapshot, op, cwd>>

(***************************************************************************)
(* Explicit terminal stutter, so TLC's deadlock check can stay ON.          *)
(***************************************************************************)
Terminating ==
    /\ \A p \in Procs : pstate[p] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E p \in Procs : PLoad(p) \/ PAcquire(p) \/ PReload(p) \/ PSave(p)
    \/ Terminating

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* Safety 1: no committed mutation is lost.  Unlike S3lfsManifest this is  *)
(* now a CONSEQUENCE of where the lock resolves, not of a boolean.         *)
(***************************************************************************)
NoLostUpdate == manifest = expected

(***************************************************************************)
(* Safety 2: s3lfs must not enumerate its own metadata as user data.       *)
(* Violated whenever the lock lands inside the subtree being tracked.      *)
(***************************************************************************)
NoInternalFileTracked ==
    /\ \A p \in Procs : LockDir(p) \notin Under(TRACK_TARGET)
    /\ ManifestDir \notin Under(TRACK_TARGET)

(***************************************************************************)
(* Mutual exclusion, stated directly, so the counterexample names the two  *)
(* processes rather than only showing a lost update downstream.            *)
(***************************************************************************)
MutualExclusion ==
    \A p1, p2 \in Procs :
        (p1 # p2 /\ pstate[p1] \in {"acquired", "mutated"}
                 /\ pstate[p2] \in {"acquired", "mutated"})
            => FALSE

=============================================================================
