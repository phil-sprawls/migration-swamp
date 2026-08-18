# migration-swamp — Now / Next / Later

For the analysts and teams who will use this, and the leaders who need to
know what to plan around.

**Now = what works the day it launches.** Nothing here is live yet.
Now / Next / Later describe **order, not dates.** A "?" means we have heard
the demand but have not committed to it — plan as if it will not happen.

---

| | **Now** (at launch) | **Next** | **Later** |
|---|---|---|---|
| **Access** | You get access to the data you requested, and only you. You get it because you proved you can already read it at the source — so this never gives you data you couldn't already get. | No change. | No change. |
| **Source Systems** | SQL Server, Oracle, and SAS. | No change. | Network drive? |
| **Redundancy Management** | Before copying, we check whether the same table is already in the swamp. If it is, you get access to the existing copy — we don't make a second one. | Check against EDW-S? | To be determined. |
| **Refresh** | Manual. A copy is a snapshot from the moment you requested it. It does not update on its own; refreshing is something you ask for. | Still manual. | Automated? |

---

## What this means in practice

**Nothing in Next is committed.** Every row either holds steady or carries
a question mark. That is deliberate: the goal at launch is a narrow thing
that works and is properly governed, not a broad thing that mostly works.
Plan on what launches staying the shape of the service for a while.

**You will not get a second copy of something we already have.** If a
colleague already brought in the table you need, you get access to their
copy rather than a duplicate — same data, one governed copy, one place to
look. This is the main thing that keeps the environment from filling up
with near-identical tables.

**The EDW-S check would extend that beyond the swamp.** If we build it, a
request for data that already lives in the enterprise warehouse would point
you there and make no copy at all. It is not committed.

**A copy is a snapshot, not a feed.** This is the most common wrong
expectation, so it is worth being blunt: the data is current as of when it
was pulled and stays that way. If your work depends on data that moves,
plan for a manual refresh, and check the age of a copy before you report
off it.

**SQL Server needs your server switched on first.** Each SQL Server host
has to be individually enabled before we can reach it. If yours isn't, the
tool will tell you so and point you to `go/udapintake` to request it — it
will say so immediately rather than failing in a way that looks like a
password problem.

---

## Open decision that could change this

**Whether this becomes the only way to bring data in, or one option among
several, is not yet decided.** Today people pull on-prem data themselves,
and nothing here stops that. If that direct route is eventually restricted
so this becomes the required path, the timing and the support that comes
with it will be communicated well in advance — no one will find out by
being blocked.

This decision is being made before launch. It does not change any cell in
the table above; it changes how much of the environment the table applies
to.

---

*Technical detail, design rationale, and the full list of open questions:
`docs/WHITEPAPER.md`.*
