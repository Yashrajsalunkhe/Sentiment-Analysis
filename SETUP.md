# Setup & Run Guide

Working, verified instructions for this project on a clean Linux machine.

This supplements [`README.md`](README.md), which documents the architecture and
project overview. **Read the [Corrections](#corrections-to-readme) section first** —
a few instructions there will fail on a modern machine.

---

## Architecture recap

```
producer-validation-tweets.py   ->  Kafka topic "numtest"  ->  consumer-pyspark.py  ->  MongoDB
   (reads the validation CSV)          (Docker: 9092)        (Spark ML inference)     (localhost:27017)
                                                                        |
                                                                        v
                                                            Django dashboard  ->  http://127.0.0.1:8000
```

The trained model is loaded by both the consumer and the Django `/classify/` page.

---

## Corrections to README

These are the issues that stop the documented steps from working. All were hit and
fixed while verifying this guide.

### 1. `kafka==1.3.5` cannot be imported on any modern Python

The old pin uses `async` as a variable name. `async` became a reserved keyword in
Python 3.7, so the package raises a hard `SyntaxError` on import:

```
File ".../kafka/producer/simple.py", line 54
    return '<SimpleProducer batch=%s>' % self.async
                                            ^^^^^
SyntaxError: invalid syntax
```

**Fix:** use the maintained `kafka-python` fork. It exposes the same `kafka` module
and the same `KafkaProducer` / `KafkaConsumer` API, so no code changes are needed.
`requirements.txt` has been updated to `kafka-python==2.0.2`.

> Do not install both. They both provide the `kafka` package name and will conflict.
> If you previously installed the old one, `pip uninstall kafka` first.

### 2. The Kafka topic is `numtest`, not `twitter`

The README tells you to create a topic named `twitter`, but the code uses `numtest`:

- `Kafka-PySpark/producer-validation-tweets.py:14` — `producer.send('numtest', ...)`
- `Kafka-PySpark/consumer-pyspark.py:53` — `KafkaConsumer('numtest', ...)`

Creating `twitter` accomplishes nothing. Use `numtest`.

### 3. The classify URL needs a trailing slash

The route is `classify/` (`Django-Dashboard/dashboard/urls.py:6`). A `POST` to
`/classify` returns **HTTP 500** with:

```
RuntimeError: You called this URL via POST, but the URL doesn't end in a slash and
you have APPEND_SLASH set. Django can't redirect to the slash URL while maintaining
POST data.
```

Always use `http://127.0.0.1:8000/classify/`.

### 4. `PYSPARK_PYTHON` must be set

If it is not, the Spark JVM workers fall back to whatever `python3` is on `PATH`.
When that differs in minor version from the driver, every job fails:

```
PYTHON_VERSION_MISMATCH: Python in worker has different version (3, 14) than that in
driver 3,11, PySpark cannot run with different minor versions.
```

This bites specifically when the venv Python differs from the system Python.
See [step 3](#3-export-pyspark_python) below.

### 5. The model file is a directory, not a pickle

`logistic_regression_model.pkl` looks like a 28-byte file but is a **directory** — a
Spark `PipelineModel` written as a folder tree (`metadata/`, `stages/0_Tokenizer_…`,
`1_StopWordsRemover_…`, `2_CountVectorizer_…`, `3_LogisticRegression_…`). This is
normal and `PipelineModel.load()` handles it. A copy exists in each app directory.

Because the load path is **relative** (`PipelineModel.load("logistic_regression_model.pkl")`),
you must start each app from its own directory or the model will not be found.

---

## Prerequisites

| Requirement | Version used | Notes |
| --- | --- | --- |
| Python | **3.11** | Do not use 3.12/3.13/3.14 — PySpark 3.5.1 does not support them |
| Java | 11 | Required by Spark. `java -version` must work |
| Docker + Compose | any recent | Runs Kafka and ZooKeeper |
| MongoDB | any recent | Must be reachable on `localhost:27017` |

Check what you have:

```bash
python3 --version && java -version && docker --version && docker compose version && mongod --version
```

`uv` makes the Python 3.11 venv step easy, but `python3.11 -m venv .venv` works too.

---

## Setup

### 1. Create the venv and install dependencies

Run from the repository root.

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
```

<details>
<summary>Without <code>uv</code></summary>

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
</details>

Verify — this catches the `kafka` problem immediately:

```bash
.venv/bin/python -c "import kafka, pymongo, nltk, pyspark, django; print('ok', kafka.__version__, pyspark.__version__)"
```

### 2. Start the services

**Kafka + ZooKeeper** (from the repository root):

```bash
docker compose -f zk-single-kafka-single.yml up -d
```

**MongoDB.** If you have `sudo` and systemd:

```bash
sudo systemctl start mongod
```

Without `sudo`, run it as your own user against a local data directory instead:

```bash
mkdir -p /tmp/opencode/mongodb-data /tmp/opencode/mongodb-log
mongod --dbpath /tmp/opencode/mongodb-data \
       --logpath /tmp/opencode/mongodb-log/mongod.log \
       --bind_ip 127.0.0.1 --port 27017 --fork
```

To stop that one later: `mongod --dbpath /tmp/opencode/mongodb-data --shutdown`

**Create the topic** (note `numtest`):

```bash
docker exec kafka1 kafka-topics --create \
  --topic numtest --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka1 kafka-topics --describe --topic numtest --bootstrap-server localhost:9092
```

> You can skip this — Kafka auto-creates the topic on first produce — but creating it
> explicitly lets you confirm the broker is healthy.

### 3. Export `PYSPARK_PYTHON`

This must be set **in every terminal** that runs Spark:

```bash
export PYSPARK_PYTHON=/absolute/path/to/this/repo/.venv/bin/python
```

Use an absolute path. Add it to your shell profile if you like.

---

## Running

You need **three terminals**. Each must `cd` into its own directory, and terminals 2
and 3 need `PYSPARK_PYTHON` exported.

### Terminal 1 — Kafka producer

```bash
cd Kafka-PySpark
export PYSPARK_PYTHON=/absolute/path/to/this/repo/.venv/bin/python
../.venv/bin/python producer-validation-tweets.py
```

Streams `Kafka-PySpark/twitter_validation.csv` into Kafka, one tweet every 3 seconds.

> The file has 1758 rows, so a full pass takes roughly **90 minutes**. Stop it with
> `Ctrl+C` whenever you have enough data.

### Terminal 2 — Spark consumer

```bash
cd Kafka-PySpark
export PYSPARK_PYTHON=/absolute/path/to/this/repo/.venv/bin/python
../.venv/bin/python consumer-pyspark.py
```

Loads the model, predicts each tweet, and inserts into MongoDB. Expect a few seconds
of Spark startup, then one block of output per tweet. Start this **before** the
producer so nothing is missed.

### Terminal 3 — Django dashboard

```bash
cd Django-Dashboard
export PYSPARK_PYTHON=/absolute/path/to/this/repo/.venv/bin/python
../.venv/bin/python manage.py collectstatic     # first run only
../.venv/bin/python manage.py runserver
```

Open <http://127.0.0.1:8000>.

- `/` — dashboard: tweet table, label rates, pie and bar charts
- `/classify/` — classify your own text with the same model

The first request downloads the NLTK `punkt` and `stopwords` corpora, so it is slow.
The dashboard regenerates the word-frequency plot into
`Django-Dashboard/BigDataProject/static/imgs/` on every load.

---

## Verifying it works

**Data reached MongoDB:**

```bash
mongosh --quiet bigdata_project --eval 'print(db.tweets.countDocuments({}))'
```

**Documents look right:**

```bash
mongosh --quiet bigdata_project --eval 'db.tweets.find().limit(3).forEach(d => printjson(d))'
```

Expected shape:

```json
{ "_id": "...", "tweet": "BBC News - Amazon boss Jeff Bezos ...", "prediction": "Neutral" }
```

**Counts per label** — these should match the dashboard exactly:

```bash
mongosh --quiet bigdata_project --eval \
  'JSON.stringify(db.tweets.aggregate([{$group:{_id:"$prediction",n:{$sum:1}}}]).toArray())'
```

**Model inference works** (from the repository root):

```bash
cd Kafka-PySpark
../.venv/bin/python -c "
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
p = PipelineModel.load('logistic_regression_model.pkl')
print(p.transform(spark.createDataFrame([('I love this',)], ['Text'])).collect()[0][6])
"
```

Expected class indices: `0` Negative, `1` Positive, `2` Neutral, `3` Irrelevant.

---

## Troubleshooting

**`SyntaxError: invalid syntax` pointing at `self.async`**
The old `kafka` package is installed. `pip uninstall kafka`, then install `kafka-python==2.0.2`.

**`PYTHON_VERSION_MISMATCH`**
`PYSPARK_PYTHON` is unset or points at a different minor version than the driver.
Export it in that terminal.

**`PipelineModel.load` / model not found**
You are in the wrong directory. `cd Kafka-PySpark` (or `Django-Dashboard`) first.

**Consumer receives nothing**
Check the topic name is `numtest` in both apps, and that the producer is actually
running. Also confirm the consumer can reach the broker:
`docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092`

**Consumer never sees earlier messages**
The consumer uses `group_id='my-group'` with `auto_offset_reset='earliest'`. If that
group already committed offsets, it resumes from them. To replay from the start, stop
the consumer **and wait for the group to go inactive** before resetting:

```bash
# Ctrl+C the consumer, then wait ~20s for the group to drain
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group
```

`CONSUMER-ID` should be `-`. If it is not, wait longer. Then:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my-group --topic numtest --reset-offsets --to-earliest --execute
```

Resetting too early fails with:

```
Error: Assignments can only be reset if the group 'my-group' is inactive,
but the current state is Stable.
```

**MongoDB connection refused**
`mongod` is not running. Check with `mongosh --quiet --eval 'db.runCommand({ping:1})'`.

**`mongod` fails to start via systemctl**
No `sudo`/passwordless access. Use the `--fork` command from
[step 2](#2-start-the-services) instead.

**Dashboard is empty**
Expected when MongoDB has no documents — the view short-circuits on an empty
collection. Run the producer and consumer first.

**Port 8000 or 9092 already in use**
`docker compose -f zk-single-kafka-single.yml down` then edit the `ports:` mapping in
`zk-single-kafka-single.yml`. For Django, pass a different port to `runserver`.

---

## Using your own tweet export

The producer reads `Kafka-PySpark/twitter_validation.csv`, which must be a
**headerless** CSV with four columns: `tweet_id, entity, sentiment, text`. Only the
last column is used for inference, so the sentiment column is ignored by the
consumer — it is carried along for reference.

To convert a Twitter/X or TweetClaw export, use the bundled converter:

```bash
.venv/bin/python tools/normalize_tweet_export.py path/to/your-export.csv \
  --output Kafka-PySpark/twitter_validation.csv
```

It accepts common headers (`tweet_id`, `account`, `sentiment`, `label`, `text`,
`tweet`, `full_text`, `content`, `body`) and maps sentiment values onto
`Negative` / `Positive` / `Neutral` / `Irrelevant`. Rows it cannot map are skipped.

Back up the original first if you want to keep it:

```bash
cp Kafka-PySpark/twitter_validation.csv Kafka-PySpark/twitter_validation.csv.bak
```

A working example lives at `examples/tweetclaw-reviewed-export.csv`.

---

## Shutting down

```bash
# Django
Ctrl+C in terminal 3

# Producer and consumer
Ctrl+C in terminals 1 and 2

# Kafka
docker compose -f zk-single-kafka-single.yml down

# MongoDB (only if you started it with --fork)
mongod --dbpath /tmp/opencode/mongodb-data --shutdown
```

---

## Project layout

| Path | Purpose |
| --- | --- |
| `Kafka-PySpark/producer-validation-tweets.py` | Reads the CSV, publishes to Kafka topic `numtest` |
| `Kafka-PySpark/consumer-pyspark.py` | Consumes, runs ML inference, writes to MongoDB |
| `Kafka-PySpark/logistic_regression_model.pkl/` | Spark `PipelineModel` (a directory) |
| `ML PySpark Model/Big_Data.ipynb` | Notebook that trains the model |
| `ML PySpark Model/twitter_training.csv` | Training set, ~74k labelled tweets |
| `Django-Dashboard/manage.py` | Django entry point |
| `Django-Dashboard/dashboard/views.py` | Dashboard queries MongoDB, renders charts |
| `Django-Dashboard/dashboard/consumer_user.py` | Loads the model for `/classify/` |
| `Django-Dashboard/logistic_regression_model.pkl/` | Model copy for Django |
| `tools/normalize_tweet_export.py` | Converts tweet exports to the expected CSV shape |
| `zk-single-kafka-single.yml` | Docker Compose for ZooKeeper + Kafka |

MongoDB database is `bigdata_project`, collection is `tweets`, with fields
`tweet` and `prediction`. Both apps connect to `localhost:27017`.
