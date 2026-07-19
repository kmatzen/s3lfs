-------------------------- MODULE S3lfsCombined --------------------------
(***************************************************************************)
(* Manifest read-modify-write and garbage collection in ONE model.         *)
(*                                                                         *)
(* The earlier specs are per-protocol silos, and each assumed the others'  *)
(* properties held:                                                        *)
(*                                                                         *)
(*   S3lfsGC       assumed mutual exclusion, which was false until the     *)
(*                 lock path was fixed.                                    *)
(*   S3lfsManifest assumed paths did not matter, which hid the namespace   *)
(*                 defect.                                                 *)
(*                                                                         *)
(* Cross-cutting defects live in exactly those seams. This model puts an   *)
(* uploader, a remover and a collector against one manifest, so the two    *)
(* safety properties are checked against the SAME behaviours rather than   *)
(* against two separate idealisations.                                     *)
(*                                                                         *)
(* Reachability is keyed on hash AND path, matching the storage layout     *)
(* assets/{hash}/{manifest_key}.gz, so this also tracks the path-aware GC  *)
(* the code now implements.                                                *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS Paths,          \* file paths that may be tracked
          Hashes,         \* content hashes
          NoItem,         \* sentinel for "nothing selected"
          RELOAD,         \* do writers re-read under the lock?
          INFLIGHT,       \* do uploaders claim before uploading?
          GC_REVALIDATE   \* does the collector re-check before deleting?

VARIABLES manifest,   \* path -> hash, as a set of <<path, hash>> pairs
          s3,         \* set of <<path, hash>> objects present in S3
          inflight,   \* claimed but not yet published <<path, hash>>
          lock,       \* "free" or the holder's name
          up,         \* uploader control state
          upItem,     \* what the uploader is publishing
          upSnap,     \* uploader's in-memory manifest copy
          rm,         \* remover control state
          rmItem,
          rmSnap,
          gc,         \* collector control state
          gcDoomed    \* objects the collector has marked

vars == <<manifest, s3, inflight, lock, up, upItem, upSnap,
          rm, rmItem, rmSnap, gc, gcDoomed>>

Assets == Paths \X Hashes

ASSUME NoItem \notin Assets
ASSUME NoItem \notin Paths

\* The manifest maps each path to at most one hash.
PathsOf(m) == {a[1] : a \in m}
Replace(m, item) == {a \in m : a[1] # item[1]} \cup {item}
Without(m, path) == {a \in m : a[1] # path}

TypeOK ==
    /\ manifest \subseteq Assets
    /\ s3 \subseteq Assets
    /\ inflight \subseteq Assets
    /\ lock \in {"free", "up", "rm", "gc"}
    /\ up \in {"idle", "claimed", "stored", "done"}
    /\ rm \in {"idle", "loaded", "done"}
    /\ gc \in {"idle", "marked", "swept"}
    /\ upItem \in Assets \cup {NoItem}
    /\ rmItem \in Paths \cup {NoItem}
    /\ upSnap \subseteq Assets
    /\ rmSnap \subseteq Assets
    /\ gcDoomed \subseteq Assets

Init ==
    /\ manifest = {}
    /\ s3 = {}
    /\ inflight = {}
    /\ lock = "free"
    /\ up = "idle"
    /\ rm = "idle"
    /\ gc = "idle"
    /\ upItem = NoItem
    /\ rmItem = NoItem
    /\ upSnap = {}
    /\ rmSnap = {}
    /\ gcDoomed = {}

(***************************************************************************)
(* Uploader: claim, PUT, then publish the manifest entry.                  *)
(***************************************************************************)
UpClaim ==
    /\ up = "idle"
    /\ lock = "free"
    /\ \E a \in Assets :
        /\ a \notin manifest
        /\ upItem' = a
        /\ inflight' = IF INFLIGHT THEN inflight \cup {a} ELSE inflight
        /\ upSnap' = manifest
    /\ up' = "claimed"
    /\ UNCHANGED <<manifest, s3, lock, rm, rmItem, rmSnap, gc, gcDoomed>>

UpStore ==
    /\ up = "claimed"
    /\ s3' = s3 \cup {upItem}
    /\ up' = "stored"
    /\ UNCHANGED <<manifest, inflight, lock, upItem, upSnap,
                   rm, rmItem, rmSnap, gc, gcDoomed>>

UpPublish ==
    /\ up = "stored"
    /\ lock = "free"
    /\ LET base == IF RELOAD THEN manifest ELSE upSnap
       IN manifest' = Replace(base, upItem)
    \* The claim is released only after the reference is published.
    /\ inflight' = inflight \ {upItem}
    /\ up' = "done"
    /\ UNCHANGED <<s3, lock, upItem, upSnap, rm, rmItem, rmSnap, gc, gcDoomed>>

(***************************************************************************)
(* Remover: load, then save without the entry.                             *)
(***************************************************************************)
RmLoad ==
    /\ rm = "idle"
    /\ \E p \in Paths :
        /\ rmItem' = p
        /\ rmSnap' = manifest
    /\ rm' = "loaded"
    /\ UNCHANGED <<manifest, s3, inflight, lock, up, upItem, upSnap, gc, gcDoomed>>

RmSave ==
    /\ rm = "loaded"
    /\ lock = "free"
    /\ LET base == IF RELOAD THEN manifest ELSE rmSnap
       IN manifest' = Without(base, rmItem)
    /\ rm' = "done"
    /\ UNCHANGED <<s3, inflight, lock, up, upItem, upSnap, rmItem, rmSnap,
                   gc, gcDoomed>>

(***************************************************************************)
(* Collector: mark under the lock, sweep later.                            *)
(***************************************************************************)
GcMark ==
    /\ gc = "idle"
    /\ lock = "free"
    /\ LET live == manifest \cup (IF INFLIGHT THEN inflight ELSE {})
       IN gcDoomed' = s3 \ live
    /\ gc' = "marked"
    /\ UNCHANGED <<manifest, s3, inflight, lock, up, upItem, upSnap,
                   rm, rmItem, rmSnap>>

GcSweep ==
    /\ gc = "marked"
    /\ LET live == manifest \cup (IF INFLIGHT THEN inflight ELSE {})
           doomed == IF GC_REVALIDATE THEN gcDoomed \ live ELSE gcDoomed
       IN s3' = s3 \ doomed
    /\ gc' = "swept"
    /\ gcDoomed' = {}
    /\ UNCHANGED <<manifest, inflight, lock, up, upItem, upSnap,
                   rm, rmItem, rmSnap>>

Terminating ==
    /\ up = "done" /\ rm = "done" /\ gc = "swept"
    /\ UNCHANGED vars

Next ==
    \/ UpClaim \/ UpStore \/ UpPublish
    \/ RmLoad \/ RmSave
    \/ GcMark \/ GcSweep
    \/ Terminating

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* Safety 1 (from S3lfsGC): every tracked file is downloadable.            *)
(***************************************************************************)
NoDanglingReference == manifest \subseteq s3

(***************************************************************************)
(* Safety 2 (from S3lfsManifest): a published entry is not silently lost.  *)
(*                                                                         *)
(* Stated as: once the uploader has published and the remover has finished *)
(* removing some OTHER path, the uploaded entry is still there.            *)
(***************************************************************************)
NoLostUpdate ==
    (up = "done" /\ rm = "done" /\ rmItem # upItem[1]) => upItem \in manifest

(***************************************************************************)
(* The seam. Neither single-protocol spec can state this: it needs the     *)
(* remover (manifest spec) and the collector (GC spec) in one behaviour.   *)
(* A lost update orphans objects, and the collector then deletes them --   *)
(* so a manifest bug becomes data loss only when GC is also in play.       *)
(***************************************************************************)
NoOrphanedObjectSurvivesGC ==
    (up = "done" /\ gc = "swept" /\ upItem \in manifest) => upItem \in s3

=============================================================================
