# Load test report

Every number here was produced by `loadtest/ingest_load.py` against the real
`docker compose` stack (Postgres 16, Redis 7, the API and one worker running all
four roles), not estimated. The raw output of each run is in
[`evidence/`](evidence/), next to this file.

**Environment.** One Windows 11 laptop: 8 logical CPUs, 7.9 GB RAM, Python
3.12.6, Docker Desktop with the databases in Linux containers. Producer,
consumers and databases share the machine, and it was also running an editor
and other containers -- free memory dropped below 200 MB during the session.
Read every timing as "what this hardware did under contention", not as the
design's ceiling.

Reproduce:

```bash
docker compose up -d
python loadtest/ingest_load.py --traces 1500 --spans 6 --concurrency 30 --rate 4   # paced
python loadtest/ingest_load.py --traces 2000 --spans 6 --concurrency 200 --rate 0  # burst
```

Each event is a real `ObsEvent` shaped like an agent run (a trace start, six
spans, a trace end); the mean encoded size was **609.6 bytes**.

---

## 1. The number that matters: SDK overhead in the agent's request path

The claim the whole project rests on is that *watching SupportPilot must not
slow SupportPilot down*. This is the wall time the agent's own thread spends
inside `publish()` -- not end-to-end delivery, which happens on a background
thread the request never waits for.

| percentile | paced run (30 producers) | burst run (200 producers) |
|---|---|---|
| p50 | **2.1 µs** | 1.3 µs |
| p95 | **9.6 µs** | 2.6 µs |
| p99 | **44.0 µs** | 6.6 µs |
| max | 341.5 µs | 5,971 µs |

The paced p99 moves with machine load: the same command gave 35.5 µs one run
earlier on the same day ([`evidence/loadtest_paced_before_lock_fix.txt`](evidence/loadtest_paced_before_lock_fix.txt)),
and an earlier session on a quieter machine measured 29 µs. Quote it as "tens of
microseconds at p99", not as one exact figure. The burst's lower percentiles are
the tight loop with no sleeps between publishes; its max is a GC pause or a
scheduler hiccup on a contended machine.

For scale: a SupportPilot agent run takes 1–3 seconds and emits roughly 8–25
events. Even at the worst p99 above that is about a millisecond per run, well
under 0.1% of it.

`publish()` is this cheap because it does two things: build the event and
`put_nowait` it onto a bounded queue. Every socket write happens on a daemon
thread.

---

## 2. A paced burst, reconciled end to end

1,500 traces × 8 events = 12,000 events, 30 concurrent agent runs, paced to model
real ticket handling rather than a synthetic flood
([`evidence/loadtest_paced.txt`](evidence/loadtest_paced.txt)).

| metric | value |
|---|---|
| events published / dropped by the SDK | 12,000 / **0** |
| ingest rate while producing | 785 events/s, for 15.3 s |
| spans expected / **written to Postgres** | 9,000 / **9,000** |
| spans missing | **0** |
| peak consumer lag | 9,249 messages |
| drain after producers stopped: `obs-storage` / `obs-guardrail` / `obs-eval` | 26.9 s / 26.7 s / 29.5 s |
| durable write throughput, whole window | **213 rows/s** |

"Zero loss" here means something checkable: the harness counts span rows in
Postgres before and after, and every one of the 9,000 spans sent arrived. An
earlier version of this report only counted what the SDK dropped, which cannot
see a loss inside the pipeline.

**What 785 events/s is, and is not.** It is the rate the load generator was paced
at for 15 seconds, not a sustained ceiling. The ceiling on this machine is the
single worker's durable path: it took 42.2 s from the first event to the storage
group fully draining 12,000 events, so it absorbs about **285 events/s**,
roughly 36 agent runs per second or 3 million a day. Traffic above that for long
grows the backlog; the stream is capped at 100,000 entries (`MAXLEN ~`), so a
backlog that outgrows the cap loses its oldest unread events. Finding the real
limit means a producer on a separate machine, a rate stepped up until lag stops
returning to zero, more storage replicas, and an hour-long soak -- none of which
this report did.

The write throughput is measured over the window the writer was actually busy
(producing plus draining). An earlier version divided all rows by the drain
after producers stopped, which counted rows written during production against a
window that excluded it and overstated the rate.

## 3. What the reconciliation caught: the consumers deadlocked each other

The first run of this test, on the code as it was, reported **245 of 9,000 spans
missing** and a storage group that had not drained after 180 s
([`evidence/loadtest_paced_before_lock_fix.txt`](evidence/loadtest_paced_before_lock_fix.txt)).

The worker log said why: `DeadlockDetectedError: deadlock detected`, eight times.
The storage writer and the guardrail scanner read the same stream in different
consumer groups, so they are always writing *different* batches that mention the
*same* traces. Both upsert those `traces` rows, each in its own batch's arrival
order. Two transactions locking the same rows in opposite orders is a lock cycle;
Postgres resolves it by killing one.

Nothing was lost -- the victim batch stays unacked, and the reclaim loop retried
it; all 9,000 spans were in Postgres a few minutes later, with zero dead letters.
But "a few minutes" was the problem: a deadlocked batch waited the 60 s claim idle
time, then a 30 s reclaim interval, and only 100 messages are reclaimed per
interval.

Two fixes, each tested:

* **One lock order.** Every multi-row write to `traces` and `spans` goes in
  primary-key order, in all three writers. A single global order cannot form a
  cycle. `tests/integration/test_consumer_lock_order.py` runs both consumers
  concurrently over the same 60 traces in opposite orders against real Postgres:
  the old code deadlocks in the first round, the fixed code never does.
* **Retry a deadlock victim immediately.** A deadlock or serialization failure
  is the one transient error where retrying at once is the documented remedy,
  and `process()` is idempotent by design, so the consumer base retries it up to
  three times with jitter before falling back to redelivery.

Same command, same machine, after the fix: zero deadlocks, every group drained in
under 30 s, 9,000 of 9,000 spans written.

## 4. Where it breaks, and how

An unthrottled burst finds the safety valve rather than the steady state
([`evidence/loadtest_burst.txt`](evidence/loadtest_burst.txt)).

| mode | offered rate | published | dropped by the SDK |
|---|---|---|---|
| paced | 785 events/s | 12,000 | 0 (0%) |
| burst | 25,812 events/s (16,000 events in 0.62 s) | 10,050 | **5,950 (37.2%)** |

At that rate the SDK's bounded 10,000-event queue fills faster than the drain
thread can empty it, and the publisher **sheds load**: it drops the event,
increments a counter, logs with backoff, and returns immediately. It does not
block the caller and does not grow memory. Everything that was published reached
Postgres; the spans "missing" from that run are exactly the shed events, which
never left the process.

That is the designed behaviour and the right trade. The alternative, an
unbounded queue, turns a Redis outage or a traffic spike into an out-of-memory
kill of SupportPilot itself -- the failure this architecture exists to prevent.
**Losing observability data is always preferable to losing the ticket.** Drops
are visible, not silent: `Observability.stats()` exposes `dropped_queue_full`,
and SupportPilot's integration surfaces it on its own `/health`.

---

## 5. An optimisation that measured slower

Batching the per-span upserts into one statement was a genuine win. Collapsing
the per-trace rollup `UPDATE`s into one statement with correlated subqueries
looked strictly better too -- eleven round trips became one -- and **measured
about 25% slower** in an earlier session (121.9 → 91.4 rows/s, under the old
throughput formula; not re-measured here). The correlated form runs eight
aggregate subqueries per trace row instead of one shared `GROUP BY` pass.
Round trips were not the bottleneck; scanning `spans` repeatedly was. It was
reverted, and the reason is a comment in `storage_writer.py` so nobody re-applies
it. "Fewer round trips" is a heuristic, not a measurement.

## 6. Honest limitations

* **One contended machine.** Producer, consumers and databases share a laptop
  that was short of memory. A real deployment adds network latency (Render →
  Neon is a WAN hop) and removes this contention. Re-run there before quoting
  production numbers.
* **One worker process.** All roles share one event loop. The consumer-group
  design supports more replicas (`hostname:pid` consumer names, `XCLAIM`
  recovery, and an e2e test that a second worker joins the group), but
  throughput with more than one was not measured.
* **No LLM judge latency.** The eval scorer ran its free heuristic backend. Judge
  latency is the provider's, off the ingestion path by construction.
* **Upstash's free tier is 10,000 commands/day**, which this load would exhaust
  in seconds. The free-tier deployment is sized for demo traffic; see
  `docs/DEPLOY.md`.
* **No soak.** The longest run was a few minutes. Connection-pool behaviour and
  memory growth over hours are unmeasured.
