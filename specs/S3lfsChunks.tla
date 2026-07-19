--------------------------- MODULE S3lfsChunks ---------------------------
(***************************************************************************)
(* Models chunked upload and the checkout that later reassembles the file. *)
(*                                                                         *)
(* Upload (parallel_upload_chunked, core.py:1456-1535):                    *)
(*   - the manifest entry is recorded at PREP time (core.py:1498), before  *)
(*     any chunk has been PUT;                                             *)
(*   - individual chunk upload failures are caught, printed, and skipped   *)
(*     (core.py:1513-1516);                                                *)
(*   - the finally block at core.py:1524 writes the manifest regardless.   *)
(*                                                                         *)
(* Checkout (_discover_chunks_for_file, core.py:1565-1584):                *)
(*   - lists the chunk objects that exist, counts them, and then reads     *)
(*     indices 0..n-1 via range(len(chunk_keys)) -- assuming the surviving *)
(*     chunks are exactly a contiguous prefix;                             *)
(*   - _finalize_file (core.py:1615-1637) concatenates them with no hash   *)
(*     verification.                                                       *)
(*                                                                         *)
(* The dangerous case is a TRAILING gap.  If chunks 3 and 4 of 5 fail, the *)
(* count is 3, indices 0..2 all exist, and checkout reports success while  *)
(* silently producing a TRUNCATED file.  An interior gap ({0,1,2,4}) is    *)
(* comparatively benign: the count is 4, index 3 is missing, and the       *)
(* download fails loudly.                                                  *)
(*                                                                         *)
(* Three candidate fixes, as independent knobs:                            *)
(*   COMMIT_AFTER_UPLOAD -- write the manifest entry only once every chunk *)
(*                          has landed, instead of at prep time.           *)
(*   STORE_COUNT         -- record the chunk count in the manifest rather  *)
(*                          than inferring it from len(chunk_keys).        *)
(*   VERIFY_HASH         -- hash the reassembled file and compare against  *)
(*                          the manifest before declaring success.         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS NumChunks,
          COMMIT_AFTER_UPLOAD,
          STORE_COUNT,
          VERIFY_HASH

Chunks == 0 .. (NumChunks - 1)

VARIABLES uploaded,    \* set of chunk indices actually present in S3
          uPhase,
          committed,   \* is there a manifest entry for this file?
          mCount,      \* chunk count recorded in the manifest (if STORE_COUNT)
          dPhase,
          outcome,     \* result of checkout
          content      \* set of chunk indices the checkout handed to the user

vars == <<uploaded, uPhase, committed, mCount, dPhase, outcome, content>>

TypeOK ==
    /\ uploaded \subseteq Chunks
    /\ uPhase \in {"idle", "uploading", "committing", "done"}
    /\ committed \in BOOLEAN
    /\ mCount \in 0 .. NumChunks
    /\ dPhase \in {"idle", "done"}
    /\ outcome \in {"none", "ok", "failed", "detected", "aborted"}
    /\ content \subseteq Chunks

Init ==
    /\ uploaded = {}
    /\ uPhase = "idle"
    /\ committed = FALSE
    /\ mCount = 0
    /\ dPhase = "idle"
    /\ outcome = "none"
    /\ content = {}

(***************************************************************************)
(* Upload.  Any subset of chunks may survive: S3 errors are swallowed per  *)
(* chunk, so an arbitrary set of failures is reachable.                    *)
(***************************************************************************)
UStart ==
    /\ uPhase = "idle"
    /\ uPhase' = "uploading"
    /\ UNCHANGED <<uploaded, committed, mCount, dPhase, outcome, content>>

UUpload ==
    /\ uPhase = "uploading"
    /\ \E S \in SUBSET Chunks : uploaded' = S
    /\ uPhase' = "committing"
    /\ UNCHANGED <<committed, mCount, dPhase, outcome, content>>

\* core.py:1524 -- the finally block writes the manifest even when chunks failed.
UCommit ==
    /\ uPhase = "committing"
    /\ IF COMMIT_AFTER_UPLOAD /\ uploaded # Chunks
         THEN /\ committed' = FALSE
              /\ mCount' = 0
              /\ outcome' = "aborted"
         ELSE /\ committed' = TRUE
              /\ mCount' = IF STORE_COUNT THEN NumChunks ELSE 0
              /\ outcome' = outcome
    /\ uPhase' = "done"
    /\ UNCHANGED <<uploaded, dPhase, content>>

(***************************************************************************)
(* Checkout.  Infer the chunk count the way the code does, read that many  *)
(* contiguous indices, and reassemble.                                     *)
(***************************************************************************)
DCheckout ==
    /\ dPhase = "idle"
    /\ uPhase = "done"
    /\ committed
    /\ LET expected == IF STORE_COUNT THEN mCount ELSE Cardinality(uploaded)
           needed   == 0 .. (expected - 1)
       IN IF needed \subseteq uploaded
            THEN IF VERIFY_HASH /\ needed # Chunks
                   THEN /\ outcome' = "detected"   \* loud failure: hash mismatch
                        /\ content' = {}
                   ELSE /\ outcome' = "ok"
                        /\ content' = needed
            ELSE /\ outcome' = "failed"            \* loud failure: 404 on a chunk
                 /\ content' = {}
    /\ dPhase' = "done"
    /\ UNCHANGED <<uploaded, uPhase, committed, mCount>>

(***************************************************************************)
(* Explicit terminal stutter, so TLC's deadlock check can stay ON.          *)
(***************************************************************************)
Terminating ==
    /\ uPhase = "done"
    /\ (dPhase = "done" \/ ~committed)
    /\ UNCHANGED vars

Next == UStart \/ UUpload \/ UCommit \/ DCheckout \/ Terminating

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* THE property.  A checkout that reports success must have produced the   *)
(* whole file.  Violating this is silent data corruption -- the user gets  *)
(* a truncated file and no error anywhere.                                 *)
(***************************************************************************)
NoSilentCorruption == (outcome = "ok") => (content = Chunks)

\* Same property restricted to non-degenerate uploads, so TLC reports the
\* realistic truncation trace rather than the shortest one (zero chunks land,
\* the inferred count is 0, and an empty file "succeeds").
NoSilentTruncation ==
    (outcome = "ok" /\ uploaded # {}) => (content = Chunks)

(***************************************************************************)
(* A manifest entry must imply every one of its chunks exists.  This is    *)
(* the stronger, upload-side property.                                     *)
(***************************************************************************)
ManifestImpliesChunks ==
    (committed /\ uPhase = "done") => (uploaded = Chunks)

=============================================================================
