-------------------------- MODULE S3lfsOwnership --------------------------
(***************************************************************************)
(* Models which system owns each file: git, s3lfs, or neither.             *)
(*                                                                         *)
(* s3lfs keeps its files out of git by writing entries into a marked block *)
(* in .gitignore and removing them from the index.  That makes ownership a *)
(* shared, mutable thing between two systems, and there are two ways to    *)
(* get it wrong:                                                           *)
(*                                                                         *)
(*   NoDualOwnership   a path is never tracked by git and s3lfs at once.   *)
(*                     Both would version the same bytes, and the large    *)
(*                     file would enter git history -- the outcome s3lfs   *)
(*                     exists to prevent.                                  *)
(*                                                                         *)
(*   NoOrphanedFile    a file on disk that git ignores is tracked by       *)
(*                     s3lfs.  Otherwise it is invisible to both: git will *)
(*                     not stage it and s3lfs will not upload it, so it    *)
(*                     exists on one machine and is lost on a fresh clone. *)
(*                                                                         *)
(* IGNORE_SCOPE isolates the decision that NoOrphanedFile rests on:        *)
(*                                                                         *)
(*   "perFile"    -- tracking expands to one .gitignore entry per tracked  *)
(*                   file (_gitignore_entries_for)                         *)
(*   "directory"  -- tracking a directory writes a single `/dir/` pattern, *)
(*                   which is what the code did before #108                *)
(***************************************************************************)
EXTENDS FiniteSets

CONSTANTS
    Files,          \* every path that could exist, e.g. {a1, a2}
    IGNORE_SCOPE    \* "perFile" or "directory"

VARIABLES
    onDisk,         \* files that exist in the working tree
    manifest,       \* files s3lfs tracks
    gitIndex,       \* files git tracks
    ignoreEntries   \* entries in the s3lfs .gitignore block

vars == <<onDisk, manifest, gitIndex, ignoreEntries>>

(***************************************************************************)
(* Every file here lives in one directory, which is the case that matters: *)
(* a single `/dir/` pattern covers all of them, a per-file expansion       *)
(* covers only the ones actually tracked.                                  *)
(***************************************************************************)
DirPattern == "dir"

\* Which files a set of .gitignore entries actually hides.
Ignored(entries) ==
    IF DirPattern \in entries
        THEN Files                          \* `/dir/` hides everything under it
        ELSE {f \in Files : f \in entries}   \* a literal entry hides one file

\* What tracking a file adds to the block.
EntriesFor(f) ==
    IF IGNORE_SCOPE = "directory" THEN {DirPattern} ELSE {f}

TypeOK ==
    /\ onDisk        \subseteq Files
    /\ manifest      \subseteq Files
    /\ gitIndex      \subseteq Files
    /\ ignoreEntries \subseteq Files \cup {DirPattern}

Init ==
    /\ onDisk = {}
    /\ manifest = {}
    /\ gitIndex = {}
    /\ ignoreEntries = {}

(***************************************************************************)
(* The user                                                                *)
(***************************************************************************)

\* Create a file: a new asset, or a source file a teammate adds later.
Create ==
    /\ \E f \in Files :
        /\ f \notin onDisk
        /\ onDisk' = onDisk \cup {f}
    /\ UNCHANGED <<manifest, gitIndex, ignoreEntries>>

\* `git add`.  Git refuses paths its ignore rules cover, which is the whole
\* mechanism s3lfs relies on to keep large files out of history.
GitAdd ==
    /\ \E f \in Files :
        /\ f \in onDisk
        /\ f \notin Ignored(ignoreEntries)
        /\ gitIndex' = gitIndex \cup {f}
    /\ UNCHANGED <<onDisk, manifest, ignoreEntries>>

GitRemove ==
    /\ \E f \in gitIndex : gitIndex' = gitIndex \ {f}
    /\ UNCHANGED <<onDisk, manifest, ignoreEntries>>

(***************************************************************************)
(* s3lfs track: upload, record, ignore, and de-index.                      *)
(*                                                                         *)
(* The de-index step matters because .gitignore has no effect on files git *)
(* already tracks -- without it git keeps versioning the large file        *)
(* alongside S3.                                                           *)
(***************************************************************************)
Track ==
    /\ \E f \in Files :
        /\ f \in onDisk
        /\ f \notin manifest
        /\ manifest' = manifest \cup {f}
        /\ ignoreEntries' = ignoreEntries \cup EntriesFor(f)
        /\ gitIndex' = gitIndex \ {f}
    /\ UNCHANGED onDisk

\* s3lfs remove: stop tracking, and stop ignoring.
Untrack ==
    /\ \E f \in manifest :
        /\ manifest' = manifest \ {f}
        /\ ignoreEntries' = ignoreEntries \ EntriesFor(f)
        /\ UNCHANGED <<onDisk, gitIndex>>

Next == Create \/ GitAdd \/ GitRemove \/ Track \/ Untrack

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* Never versioned by both systems at once.
NoDualOwnership == manifest \cap gitIndex = {}

\* A file hidden from git is tracked by s3lfs.  A file hidden from git and
\* absent from the manifest is owned by nobody: it exists on one disk and
\* disappears on the next clone.
NoOrphanedFile ==
    \A f \in onDisk : f \in Ignored(ignoreEntries) => f \in manifest

=============================================================================
