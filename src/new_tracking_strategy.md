
Indexing strategy concerns:


Can we remove messages that don't affect the state anymore, *WITHOUT* time-traveling back?

Requirements
- Only forward playback, no rewinding
- Advance_cutoff without rewinding
- Requires updating message structure for past messages while in the future
- Timetravel at any point in time must still be supported
  (probably can't carry state in RAM between message applications)



Concept:

- Outgoing read dependency. Mapping from X to Y, such that Y observed data written by X.
- Incoming read dependency: Mapping from Y to X, such that X was observed by Y.
- Last overwriter of: Mapping from X to Ys, such that each of the Ys had its last fragment overwritten by X
  (this mapping is used during advance_cutoff)

State:
- Outgoing read dependencies (Vec<Vec<SequenceNr>>)
- Incoming read dependencies (Vec<Vec<SequenceNr>>)
- Last overwriter of (Vec<Vec<SequenceNr>>)
- Staleness (no uses)
- Deleted (simply "deleted" in main Index)

Invariants:
- Being observed means you may never be deleted.

// M = message to try and delete
// O = message that overwrote last fragment of M
Function "TryDelete(M,O)":
- Check if anything depends on the other message M (by read-dep)
    - If any message does, unconditionally don't delete M.
    - Else, continue next step below:
- Check if deletion possible because O (the last overwriter) is before cutoff
    - Mark for deletion
- Check if early deletion possible bc opaque and not tombstone.
    - Mark for deletion
- Check if early deletion possible bc non-transmitted, and not tombstone
    - Mark for deletion
* If marked for deletion, iterate over X in (Last overwriter of)[M]
    - Recurse by calling TryDelete(X, M)


Events:
* Play each message X
- Whenever other message Y observed:
    - Record read-dep from Y to X
    - Record reverse read-dep from X to Y
- Whenever other message's M use-count reaches 0:
    - Record that X was last overwriter of Y     
    - TryDelete(M)

- Play complete for one message:
    - Determine if message itself is unused.
        - Mark for deletion.
- Whenever a message D is marked for deletion, also:
    - Iterate over its revdeps R. If no R has any read dependencies except D:
        - Mark D for deletion.  Zero out deps.
- Finally, signal caller to rewind to time of earliest delete, and reproject.
  Do this in a loop until no more messages to delete are found.

* Advance cutoff
- Scan cutoff interval
    - If any message satisfies:
        - Overwritten (no uses)
        - No read-deps
    - Then just replay from start of cutoff-window.

* Timetravel back
- Just replay the events

