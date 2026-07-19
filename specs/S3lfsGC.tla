---------------------------- MODULE S3lfsGC ----------------------------
(***************************************************************************)
(* Models s3lfs cleanup_s3 (core.py:1298-1346) racing a concurrent         *)
(* uploader (parallel_upload_chunked, core.py:1456-1535).                  *)
(*                                                                         *)
(* The writer's real ordering is: PUT chunks to S3, THEN update the        *)
(* manifest under the lock.  The GC's real ordering is: snapshot the       *)
(* manifest under the lock, RELEASE the lock, list S3, (pause for human    *)
(* confirmation), delete anything not in the snapshot.                     *)
(*                                                                         *)
(* REVALIDATE is the candidate fix: re-read the manifest under the lock    *)
(* immediately before deleting, and drop any object that became            *)
(* referenced in the meantime.                                             *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS Hashes, REVALIDATE, INFLIGHT, NoHash

VARIABLES s3,        \* set of content hashes present as S3 objects
          manifest,  \* set of content hashes referenced by .s3_manifest.yaml
          inflight,  \* hashes an uploader has claimed but not yet committed
          wPhase, wHash,
          gPhase, gSnap, gDoomed

vars == <<s3, manifest, inflight, wPhase, wHash, gPhase, gSnap, gDoomed>>

ASSUME NoHash \notin Hashes

TypeOK ==
    /\ s3       \subseteq Hashes
    /\ manifest \subseteq Hashes
    /\ inflight \subseteq Hashes
    /\ wPhase \in {"idle", "uploading", "committing"}
    /\ wHash  \in Hashes \cup {NoHash}
    /\ gPhase \in {"idle", "listing", "sweeping"}
    /\ gSnap   \subseteq Hashes
    /\ gDoomed \subseteq Hashes

Init ==
    /\ s3 = {}
    /\ manifest = {}
    /\ inflight = {}
    /\ wPhase = "idle"
    /\ wHash = NoHash
    /\ gPhase = "idle"
    /\ gSnap = {}
    /\ gDoomed = {}

(***************************************************************************)
(* Writer: track / parallel_upload_chunked                                 *)
(***************************************************************************)

\* Pick a file to upload and compute its hash.  Under INFLIGHT the uploader
\* claims the hash under the lock BEFORE any bytes reach S3.
WStart ==
    /\ wPhase = "idle"
    /\ \E h \in Hashes :
        /\ h \notin manifest          \* nothing to do if already tracked
        /\ wHash' = h
        /\ inflight' = IF INFLIGHT THEN inflight \cup {h} ELSE inflight
    /\ wPhase' = "uploading"
    /\ UNCHANGED <<s3, manifest, gPhase, gSnap, gDoomed>>

\* _upload_chunk: the object lands in S3 while still unreferenced.
WUpload ==
    /\ wPhase = "uploading"
    /\ s3' = s3 \cup {wHash}
    /\ wPhase' = "committing"
    /\ UNCHANGED <<manifest, inflight, wHash, gPhase, gSnap, gDoomed>>

\* core.py:1525 -- lock, load_manifest, update, save_manifest.  The claim is
\* released in the same critical section that publishes the reference.
WCommit ==
    /\ wPhase = "committing"
    /\ manifest' = manifest \cup {wHash}
    /\ inflight' = inflight \ {wHash}
    /\ wPhase' = "idle"
    /\ wHash' = NoHash
    /\ UNCHANGED <<s3, gPhase, gSnap, gDoomed>>

(***************************************************************************)
(* Collector: cleanup_s3                                                   *)
(***************************************************************************)

\* core.py:1304-1305 -- snapshot under the lock, then release it.
GMark ==
    /\ gPhase = "idle"
    /\ gSnap' = manifest \cup (IF INFLIGHT THEN inflight ELSE {})
    /\ gPhase' = "listing"
    /\ UNCHANGED <<s3, manifest, inflight, wPhase, wHash, gDoomed>>

\* core.py:1318-1326 -- paginate S3, mark anything outside the stale snapshot.
GList ==
    /\ gPhase = "listing"
    /\ gDoomed' = s3 \ gSnap
    /\ gPhase' = "sweeping"
    /\ UNCHANGED <<s3, manifest, inflight, wPhase, wHash, gSnap>>

\* core.py:1336-1343 -- input() blocks here for an unbounded time, then delete.
GSweep ==
    /\ gPhase = "sweeping"
    /\ LET live   == manifest \cup (IF INFLIGHT THEN inflight ELSE {})
           doomed == IF REVALIDATE \/ INFLIGHT THEN gDoomed \ live ELSE gDoomed
       IN s3' = s3 \ doomed
    /\ gPhase' = "idle"
    /\ gDoomed' = {}
    /\ gSnap' = {}
    /\ UNCHANGED <<manifest, inflight, wPhase, wHash>>

Next == WStart \/ WUpload \/ WCommit \/ GMark \/ GList \/ GSweep

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* The property that matters: every tracked file is downloadable.          *)
(* Violation == checkout 404s.                                             *)
(***************************************************************************)
NoDanglingReference == manifest \subseteq s3

=============================================================================
