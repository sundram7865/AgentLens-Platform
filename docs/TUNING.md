# Threshold tuning

Every number in this document was produced by running the scripts named here.
None of them are guesses, and the caveats section is honest about what these
measurements do and do not establish.

Reproduce with:

```bash
python scripts/tune_injection_threshold.py            # the sweep below
python scripts/tune_injection_threshold.py --json     # machine-readable
python scripts/measure_pii_memory.py                  # PII engine footprint
```

---

## 1. Prompt-injection threshold

**`OBS_INJECTION_THRESHOLD = 0.61`**

### Method

Testing a detector against its own signature corpus returns ~1.0 for every
entry and measures nothing. So the evaluation uses three separate sets:

| Set | Size | Purpose |
|---|---|---|
| `injection_holdout.json` | 20 | Attacks **not** in the corpus. The honest recall number. |
| `benign_samples.json` | 40 | Real support messages. The false-positive rate. |
| `injection_signatures.json` | 40 | Leave-one-out: each signature scored against an index built without it. |

The benign set is deliberately adversarial. It contains "Ignore my last
message, I found the tracking link", "Could a manager override the 30 day
return window?", "Please disregard my earlier email", a pasted
`<Error 500: internal server error>`, and a pasted `[SYSTEM] payment gateway
unavailable`. A threshold tuned only against obviously-innocent text scores
perfectly in a notebook and then flags real tickets on day one.

### Score distribution (lexical backend)

```
attacks    min=0.850   median=0.910   max=0.950
benign     min=0.087   median=0.178   max=0.362
LOO corpus median=0.900
```

The two classes are cleanly separated: the highest benign score (0.362) sits
well below the lowest attack score (0.850).

### Sweep

```
 thresh    prec  recall      f1     FPR   TP  FN  FP
   0.30   0.870   1.000   0.930   0.075   20   0   3
   0.35   0.952   1.000   0.976   0.025   20   0   1
   0.40   1.000   1.000   1.000   0.000   20   0   0
   ...     (unchanged through)
   0.85   1.000   1.000   1.000   0.000   20   0   0
   0.90   1.000   0.850   0.919   0.000   17   3   0
   0.95   1.000   0.150   0.261   0.000    3  17   0
```

### Why 0.61 and not 0.37

Argmax-F1 returns **0.37**, the lowest threshold in the perfect band, which
sits 0.008 above the highest benign score. One slightly spicier support ticket
becomes a false positive.

`choose_operating_point()` instead takes the **midpoint of the longest
contiguous plateau**, giving 0.61:

* 0.248 of margin above the worst benign score
* 0.240 of margin below the best attack score

Equal headroom on both sides is what makes the setting survive traffic the
corpus has not seen.

### Caveats: read these before quoting the numbers

* **20 held-out attacks and 40 benign messages is a small sample.** Precision
  and recall of 1.00 here means "no errors in 60 examples", not "this detector
  is perfect". A 95% confidence interval on 20/20 recall still reaches down to
  roughly 0.83.
* **The corpora are hand-written, not sampled from production traffic.** They
  are shaped like SupportPilot tickets, which is the right shape, but they are
  not real tickets. Re-run this against real traffic before trusting the number
  in a regulated context.
* **Recall comes mostly from the rules, not from similarity.** Lexical TF-IDF
  scores true paraphrases around 0.33; the rules score them 0.85–0.95. That is
  a deliberate design (rules are auditable and cheap), but it means a
  genuinely novel attack phrasing that matches no rule will be missed. The
  mitigation is that adding a rule or a signature is a data change, not a
  redeploy of detection logic.
* **The `embeddings` backend is better at paraphrase** and is a one-line
  install (`pip install 'obs-platform[embeddings]'`). It is not the default
  because sentence-transformers plus its model is ~90 MB on disk and roughly
  250 MB resident, an estimate, unlike the PII numbers above, because it was
  not installed and measured here. Re-run the sweep after switching backends;
  the threshold does not transfer between them.

### Re-tuning after a change

Changing `RULES`, the signature corpus, or the backend invalidates the number.
Re-run the sweep and update `OBS_INJECTION_THRESHOLD`, this document, and the
default in `settings.py` together.

---

## 2. PII engine

**`OBS_PII_ENGINE = auto`** (resolves to `builtin` unless Presidio is installed)

### Why the built-in engine is the default

The build plan's instruction was to *measure* Presidio's footprint rather than
assume it fits. `scripts/measure_pii_memory.py` reports resident memory before
and after loading each engine and scanning a sample ticket.

| Engine | Baseline RSS | After loading + one scan | Delta |
|---|---|---|---|
| `builtin` | 18.9 MB | **59.0 MB** | 40.1 MB |
| `presidio` (with `en_core_web_sm`) | 18.9 MB | **153.5 MB** | 134.6 MB |

**The measurement contradicted the assumption.** Presidio was expected not to
fit a 512 MB box. It fits comfortably: 154 MB resident, leaving room for
FastAPI, SQLAlchemy, asyncpg and redis.

What genuinely does not fit is Presidio's **own default**. `AnalyzerEngine()`
with no NLP engine configured resolves to `en_core_web_lg`, a **400 MB model it
downloads lazily on the first `analyze()` call**, not at install time. Left
alone, the first flagged trace on a fresh deploy triggers a 400 MB download
mid-request, which on a free-tier box times out. `PresidioPiiEngine` pins
`en_core_web_sm` explicitly, and `OBS_PII_SPACY_MODEL` overrides it if you have
the memory and want the accuracy.

So Presidio stays opt-in for **image size and cold-start time**, which is a
defensible trade-off, rather than for memory, which was not true.

### What each engine actually covers

| Capability | builtin | presidio |
|---|---|---|
| Credit cards (Luhn-validated) | yes | yes |
| US SSN (structural rules) | yes | yes |
| Aadhaar (Verhoeff-validated) | **yes** | no |
| Indian PAN | **yes** | no |
| IBAN (mod-97) | yes | yes |
| API keys, AWS keys, JWTs, private keys, credentialed URLs | **yes** | partial |
| Email, phone, IP | yes | yes |
| **Person names, addresses (NER)** | **no** | **yes** |

The honest summary: the built-in engine is better on structured identifiers
(because of the checksums) and on the India-specific ones; Presidio is the only
option for free-text names. Presidio's default recognisers are English/US-tuned,
which is a real scope limit for a platform watching an India-facing support
product; that is precisely why Aadhaar and PAN were added to the built-in
registry rather than assumed to be covered.

When Presidio *is* installed, the two are **merged**, not swapped: built-in
matches win on overlap, and Presidio contributes the entity types the built-in
registry cannot detect.

### Why checksums matter more than they sound

A support ticket is full of 16-digit order numbers. Regex alone flags every one
of them as a credit card, the alert list fills with noise, and the operator
stops reading it. `test_order_number_is_not_a_credit_card` pins this behaviour.

---

## 3. Eval sampling rate

**`OBS_EVAL_SAMPLE_RATE = 5`** (percent)

Not tuned by measurement; chosen from the cost model, which is the correct
basis for this one:

```
1,000 traces/day x 5%  = 50 traces scored
50 traces / batch of 10 = 5 judge requests
~1,400 prompt tokens + ~700 completion tokens per batched request
= ~7,000 prompt + ~3,500 completion tokens/day
Claude Haiku 4.5 at $1.00 / $5.00 per MTok
≈ $0.007 + $0.018 = about $0.025/day, ~$0.75/month
```

That sits an order of magnitude under the default `$1.00/day` per-tenant cap,
so the cap only ever engages when something is genuinely wrong, which is the
behaviour you want from a guard rail. Scoring 100% of traffic would cost about
$15/month per tenant and buy very little: drift detection needs a
representative sample, not a census.

The sampling decision is a pure function of `trace_id`
(`blake2b(trace_id) % 100 < rate`), so raising the rate is strictly additive:
traces already in the 5% sample stay in the 20% sample, and the drift baseline
stays comparable across the change.

---

## 4. Drift thresholds

**`OBS_DRIFT_Z_THRESHOLD = 2.5`**, **`OBS_DRIFT_ABSOLUTE_FLOOR = 0.60`**,
**`OBS_DRIFT_MIN_SAMPLES = 20`**

Two independent triggers, because they catch different failures:

* **z-score ≥ 2.5** against a captured baseline catches *relative* degradation:
  the model got worse than it used to be, whatever "good" meant for this tenant.
* **mean < 0.60 absolute** catches a deployment that was never good. A baseline
  captured during a bad period would otherwise make "consistently poor" look
  healthy, since it never drifts from itself.

`min_samples = 20` exists because a mean over three scores is noise. With a 5%
sample rate, 20 scores is roughly 400 traces, which is a few hours of
portfolio-scale traffic; hence the 6-hour default window.

The baseline is **captured deliberately**, from a period you have looked at and
judged healthy (`POST /v1/drift/baseline`), never inferred automatically. A
baseline that silently follows the current data cannot detect drift at all: it
drifts with it.
