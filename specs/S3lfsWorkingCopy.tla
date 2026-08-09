------------------------- MODULE S3lfsWorkingCopy -------------------------
(***************************************************************************)
(* Models the working-copy lifecycle: track, remove, branch switch, sync,  *)
(* and garbage collection, against a hostile user who edits tracked files  *)
(* without uploading them.                                                 *)
(*                                                                         *)
(* The existing specs model the storage layer.  This one models the layer  *)
(* above it -- the one that decides which bytes on disk get replaced or    *)
(* deleted -- and checks three things a large-asset store must never get   *)
(* wrong:                                                                  *)
(*                                                                         *)
(*   NoDataLoss             content that exists only on disk is never      *)
(*                          destroyed by an automatic operation            *)
(*   NoDanglingReference    every manifest entry has an object behind it   *)
(*   NoCollateralDeletion   untracking one path never destroys the bytes   *)
(*                          another path still refers to                   *)
(*                                                                         *)
(* Two constants isolate the design decisions those properties depend on,  *)
(* so TLC can show each one is load-bearing rather than incidental:        *)
(*                                                                         *)
(*   CLOBBER_GUARD    TRUE  -- sync refuses to replace or delete on-disk    *)
(*                             content that differs from what the manifest *)
(*                             recorded (S3LFS.compare_to_hashes drives    *)
(*                             this in `sync`)                             *)
(*                    FALSE -- sync replaces whatever is in the way, which *)
(*                             is what the code did before this was fixed  *)
(*                                                                         *)
(*   PATH_AWARE_KEYS  TRUE  -- an object's key is derived from path AND    *)
(*                             content hash (_asset_base_key)              *)
(*                    FALSE -- keyed by content hash alone, so two paths   *)
(*                             holding identical bytes share one object    *)
(***************************************************************************)
EXTENDS FiniteSets, Naturals

CONSTANTS
    Paths,            \* tracked path names, e.g. {p1, p2}
    Contents,         \* content values; a content is its own hash here
    Absent,           \* marker for "no file on disk" / "no manifest entry"
    CLOBBER_GUARD,
    PATH_AWARE_KEYS

VARIABLES
    disk,             \* Paths -> Contents \cup {Absent}
    manifest,         \* Paths -> Contents \cup {Absent}: the working manifest
    baseline,         \* Paths -> Contents \cup {Absent}: manifest at the
                      \* revision sync diffs against (the previous HEAD)
    s3,               \* set of storage keys currently present
    lost              \* history: contents destroyed while stored nowhere

vars == <<disk, manifest, baseline, s3, lost>>

ASSUME Absent \notin Contents

Values == Contents \cup {Absent}

(***************************************************************************)
(* Storage keys.  With PATH_AWARE_KEYS the key carries the path, so the    *)
(* same bytes tracked at two paths occupy two objects and removing one     *)
(* cannot affect the other.  Without it, identical content aliases.        *)
(***************************************************************************)
Key(p, c) == IF PATH_AWARE_KEYS THEN <<p, c>> ELSE <<"shared", c>>

\* Keys reachable from the manifest: what garbage collection must keep.
LiveKeys == {Key(p, manifest[p]) : p \in {q \in Paths : manifest[q] # Absent}}

Stored(p, c) == c # Absent /\ Key(p, c) \in s3

\* Content that exists on disk and nowhere else is irreplaceable: if an
\* operation removes it from disk, it is gone for good.
Irreplaceable(p) == disk[p] # Absent /\ ~Stored(p, disk[p])

\* Record destruction of whatever p currently holds, if it was the only copy.
Destroyed(p) == IF Irreplaceable(p) THEN {disk[p]} ELSE {}

TypeOK ==
    /\ disk     \in [Paths -> Values]
    /\ manifest \in [Paths -> Values]
    /\ baseline \in [Paths -> Values]
    /\ s3       \subseteq ({"shared"} \cup Paths) \X Contents
    /\ lost     \subseteq Contents

Init ==
    /\ disk     = [p \in Paths |-> Absent]
    /\ manifest = [p \in Paths |-> Absent]
    /\ baseline = [p \in Paths |-> Absent]
    /\ s3       = {}
    /\ lost     = {}

(***************************************************************************)
(* The user                                                                *)
(***************************************************************************)

\* Create or edit a file without tracking it.  This is the state that makes
\* clobbering possible: the bytes exist only here.  Tracked files are
\* gitignored, so git cannot warn about this either.
Edit ==
    /\ \E p \in Paths, c \in Contents :
        /\ disk[p] # c
        /\ disk' = [disk EXCEPT ![p] = c]
    /\ UNCHANGED <<manifest, baseline, s3, lost>>

\* Delete a file yourself.  Deliberate, so it does not count as loss.
UserDelete ==
    /\ \E p \in Paths :
        /\ disk[p] # Absent
        /\ disk' = [disk EXCEPT ![p] = Absent]
    /\ UNCHANGED <<manifest, baseline, s3, lost>>

(***************************************************************************)
(* s3lfs track: upload the bytes, then record them.                        *)
(*                                                                         *)
(* The object is PUT before the manifest names it, which is what keeps     *)
(* NoDanglingReference true at every intermediate state.                   *)
(***************************************************************************)
Track ==
    /\ \E p \in Paths :
        /\ disk[p] # Absent
        /\ manifest[p] # disk[p]
        /\ s3' = s3 \cup {Key(p, disk[p])}
        /\ manifest' = [manifest EXCEPT ![p] = disk[p]]
    /\ UNCHANGED <<disk, baseline, lost>>

\* s3lfs remove: drop the manifest entry.  The object stays until cleanup.
Untrack ==
    /\ \E p \in Paths :
        /\ manifest[p] # Absent
        /\ manifest' = [manifest EXCEPT ![p] = Absent]
    /\ UNCHANGED <<disk, baseline, s3, lost>>

(***************************************************************************)
(* Committing and switching branches                                       *)
(*                                                                         *)
(* Commit makes the working manifest the baseline sync will diff against.  *)
(* SwitchBranch moves the working manifest to some other committed state   *)
(* while leaving the baseline at the old one -- exactly the situation      *)
(* post-checkout hands to `s3lfs sync --from $1`.                          *)
(***************************************************************************)
Commit ==
    /\ baseline' = manifest
    /\ UNCHANGED <<disk, manifest, s3, lost>>

SwitchBranch ==
    /\ \E m \in [Paths -> Values] :
        \* Only to a manifest whose entries are actually stored: a branch
        \* referring to content nobody uploaded is the separate failure
        \* NoDanglingReference already covers.
        /\ \A p \in Paths : m[p] # Absent => Key(p, m[p]) \in s3
        /\ manifest' = m
    /\ UNCHANGED <<disk, baseline, s3, lost>>

(***************************************************************************)
(* s3lfs sync                                                              *)
(*                                                                         *)
(* Download: an entry whose manifest hash differs from what is on disk.    *)
(* With the guard, a file is only replaced when what it holds is the       *)
(* content the baseline recorded -- i.e. it is clean, so replacing it      *)
(* loses nothing.  Without the guard, anything in the way is replaced.     *)
(***************************************************************************)
(***************************************************************************)
(* The guard.  Matching a recorded hash is NOT sufficient: the object      *)
(* behind that hash may since have been garbage-collected, in which case   *)
(* the copy on disk is the last one.  TLC found exactly that trace --      *)
(* track, commit, remove, cleanup, sync -- against the weaker rule.  The   *)
(* condition that actually holds is: only take bytes off disk when those   *)
(* bytes can be fetched back.                                              *)
(***************************************************************************)
SafeToReplace(p) ==
    \/ disk[p] = Absent            \* nothing to lose
    \/ Stored(p, disk[p])          \* retrievable, so removing it loses nothing

SyncDownload ==
    /\ \E p \in Paths :
        /\ manifest[p] # Absent
        /\ disk[p] # manifest[p]
        /\ CLOBBER_GUARD => SafeToReplace(p)
        /\ disk' = [disk EXCEPT ![p] = manifest[p]]
        /\ lost' = lost \cup Destroyed(p)
    /\ UNCHANGED <<manifest, baseline, s3>>

\* Prune: a path the manifest no longer lists is removed from disk.
SyncPrune ==
    /\ \E p \in Paths :
        /\ manifest[p] = Absent
        /\ disk[p] # Absent
        /\ CLOBBER_GUARD => SafeToReplace(p)
        /\ disk' = [disk EXCEPT ![p] = Absent]
        /\ lost' = lost \cup Destroyed(p)
    /\ UNCHANGED <<manifest, baseline, s3>>

(***************************************************************************)
(* s3lfs cleanup: delete every object the manifest does not reach.         *)
(*                                                                         *)
(* Reachability is computed on the same key function the writer uses, so   *)
(* whether this is safe depends entirely on PATH_AWARE_KEYS.               *)
(***************************************************************************)
Cleanup ==
    /\ s3' = s3 \cap LiveKeys
    /\ UNCHANGED <<disk, manifest, baseline, lost>>

Next ==
    \/ Edit \/ UserDelete
    \/ Track \/ Untrack
    \/ Commit \/ SwitchBranch
    \/ SyncDownload \/ SyncPrune
    \/ Cleanup

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* No automatic operation destroyed the only copy of anything.
NoDataLoss == lost = {}

\* Every manifest entry has an object behind it, so checkout cannot 404.
NoDanglingReference ==
    \A p \in Paths : manifest[p] # Absent => Key(p, manifest[p]) \in s3

\* Untracking or cleaning up on one path never removes the bytes another
\* path still refers to.  Distinct live paths must occupy distinct keys.
NoCollateralDeletion ==
    \A p, q \in Paths :
        (p # q /\ manifest[p] # Absent /\ manifest[q] # Absent)
            => (Key(p, manifest[p]) # Key(q, manifest[q]))

=============================================================================
