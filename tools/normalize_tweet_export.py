import argparse
import csv
from pathlib import Path


TEXT_COLUMNS = ("text", "tweet", "full_text", "content", "body")
SENTIMENT_COLUMNS = ("sentiment", "label", "classification", "prediction")
ENTITY_COLUMNS = ("entity", "topic", "keyword", "account", "author", "username")
ID_COLUMNS = ("tweet_id", "id", "tweetId", "tweetID", "status_id")


def column_lookup(fieldnames):
    return {name.lower(): name for name in fieldnames or []}


def first_column(lookup, candidates):
    for candidate in candidates:
        match = lookup.get(candidate.lower())
        if match is not None:
            return match
    return None


def normalize_sentiment(value):
    normalized = value.strip().lower()
    mapping = {
        "neg": "Negative",
        "negative": "Negative",
        "pos": "Positive",
        "positive": "Positive",
        "neu": "Neutral",
        "neutral": "Neutral",
        "irrelevant": "Irrelevant",
        "other": "Irrelevant",
    }
    return mapping.get(normalized)


def convert(input_path, output_path, default_entity):
    with input_path.open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        lookup = column_lookup(reader.fieldnames)
        text_column = first_column(lookup, TEXT_COLUMNS)
        sentiment_column = first_column(lookup, SENTIMENT_COLUMNS)
        entity_column = first_column(lookup, ENTITY_COLUMNS)
        id_column = first_column(lookup, ID_COLUMNS)

        missing = []
        if text_column is None:
            missing.append("text, tweet, full_text, content, or body")
        if sentiment_column is None:
            missing.append("sentiment, label, classification, or prediction")
        if missing:
            raise SystemExit("Missing required column: " + "; ".join(missing))

        rows = []
        for index, row in enumerate(reader, start=1):
            text = row.get(text_column, "").strip()
            sentiment = normalize_sentiment(row.get(sentiment_column, ""))
            if not text or sentiment is None:
                continue

            row_id = row.get(id_column, "").strip() if id_column else ""
            entity = row.get(entity_column, "").strip() if entity_column else ""
            rows.append(
                {
                    "id": row_id or str(index),
                    "entity": entity or default_entity,
                    "sentiment": sentiment,
                    "text": text,
                }
            )

    if not rows:
        raise SystemExit("No rows with text and supported sentiment labels found.")

    with output_path.open("w", newline="", encoding="utf-8") as target_file:
        writer = csv.DictWriter(
            target_file, fieldnames=("id", "entity", "sentiment", "text")
        )
        writer.writerows(rows)

    return len(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Convert a reviewed TweetClaw or X export into the Kafka sample CSV shape."
    )
    parser.add_argument("input_csv", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("Kafka-PySpark/twitter_validation.csv"),
    )
    parser.add_argument("--default-entity", default="TweetClaw")
    args = parser.parse_args()

    count = convert(args.input_csv, args.output, args.default_entity)
    print(f"Wrote {count} rows to {args.output}")


if __name__ == "__main__":
    main()
