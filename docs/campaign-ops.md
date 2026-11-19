# Watch and diagnose a campaign

Run these commands from a source checkout with the project environment installed.
The CPU bigram example runs eight recipes. One deliberately fails. Use one shell
for the campaign and a second shell for continuous notification.

~~~sh
db=.research/bigram.sqlite
campaign=cpu-bigram-smoothing
uv run --no-sync research swarm run examples/cpu_bigram/swarm.yaml --db "$db"
uv run --no-sync research swarm notify "$campaign" --db "$db" --timezone Europe/London \
  --score-target 8 --min-trials-per-hour 2 --fake-reporter --once
~~~

The first poll prints a JSON digest. It includes the selected score, the number
of trials completed in the preceding 60 minutes, and measured and reserved cost.
The score alert identifies the first recorded selection that meets the target in
the campaign's optimization direction. Each configured target fires once.
The rate alert starts after the first campaign hour and fires at most once per
local clock hour per configured minimum. Retried attempts count when their
trial reaches its final outcome.

The example uses an offline reporter. Use `--reporter-model MODEL` to add
prose from an installed Ollama model, or omit the provider flag for a numeric
digest. Set its deadline with `--reporter-timeout SECONDS`. The numeric digest
is already in the journal when the model starts. A model timeout records its
status alongside those measurements.

Omit `--once` to poll every ten seconds. Stop the watcher with Ctrl-C when the
campaign ends. Restart with the same queue and timezone. The watcher resumes
at the current local hour. The UTC offset distinguishes repeated hours when the
clocks go back. Time spent stopped does not generate backdated hourly digests.

## Retrieve an alert after a restart

The watcher commits each record beside the queue before writing it to stdout.
Watchers using the same queue share this journal, including across restarts.
A crash or broken pipe can interrupt stdout after the record was committed.
Read the journal to recover the original numeric evidence.

~~~sh
sqlite3 -readonly "$db.notifications.sqlite" \
  "SELECT payload FROM notifications WHERE campaign='cpu-bigram-smoothing' ORDER BY rowid"
~~~

## Get bounded advice

The fake exercises each role offline. Use an installed local Ollama model by
replacing `--fake` with `--model MODEL`. `--timeout` sets a wall-clock deadline
in seconds. The proposer returns recipes and rationales for the declared search
space. A batch may use up to the campaign's configured worker count.

~~~sh
uv run --no-sync research swarm advise proposer "$campaign" --db "$db" --fake --count 2
attempt=$(sqlite3 -readonly "$db" \
  "SELECT a.attempt_id FROM swarm_attempts a JOIN swarm_trials t USING(trial_id)
   WHERE t.campaign_id='cpu-bigram-smoothing' AND json_extract(a.observation,'$.status')='failed'
   ORDER BY a.started_at DESC LIMIT 1")
uv run --no-sync research swarm advise diagnoser "$campaign" --db "$db" --attempt "$attempt" --fake
uv run --no-sync research swarm advise reporter "$campaign" --db "$db" --fake --timeout 10
~~~

The diagnoser reads the specified failed or lost attempt and may suggest a new
recipe. Inspect its rationale before adding a candidate to a future spec.
The reporter summarizes campaign evidence. The scheduler owns dispatch, retries,
selection and budgets. All three advice commands open the campaign read-only.

Each active request exposes a `heartbeat.json` under `<db>.agent-jobs/`.
It contains the role and absolute deadline in Unix seconds. The provider updates
the file's modification time while awaiting a response. On expiry the supervisor
kills the provider process group, reaps the child and returns `timed_out`.
Cancellation also cleans up the job. A failed or incomplete advice command exits
with status 3. Successful siblings remain in the batch output. Read a late or
malformed response as missing advice and use the measured campaign evidence.
