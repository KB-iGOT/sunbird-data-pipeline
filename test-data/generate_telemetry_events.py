#!/usr/bin/env python3
"""
Sunbird Telemetry Event Generator
Generates configurable telemetry events and pushes them to a Kafka topic
in batches of 10 or 100 (randomly chosen per batch).
"""

import json
import random
import time
import uuid
import hashlib
import argparse
from collections import defaultdict
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Environment-level configuration — override for each environment
# ---------------------------------------------------------------------------

ENVIRONMENTS = {
    "dev": {
        "channel": "b00bc992ef25f1a9a8d63291e20efc8d",
        "kafka_brokers": ["10.175.3.38:9092"],
        "kafka_topic": "dev.telemetry.ingestion",
        "pdata": {
            "id": "dev.diksha.app",
            "pid": "sunbird.app",
            "ver": "2.0.93",
        },
        "user_ids": [
            "d44de233-3757-484a-83cf-442ef37df33c",
            "f9f2defb-578a-42e0-8563-1234c467241e",
            "66b4d0f8-8e4b-4473-81c4-cb9b11eaf345",
            "7fb26abb-60a3-4ac2-a1fc-f8dab7f182ed",
            "66df350a-2f01-4f27-bf17-8b349f4ff232",
        ],
        "content_ids": [
            "do_31250767642728857625713",
            "do_31250767642750156825796",
            "do_312524605852426240123106",
            "do_31249346092941312024729",
            "do_3125105873883955202989",
        ],
    },
    "staging": {
        "channel": "505c7c48ac6dc1edc9b08f21db5a571d",
        "kafka_brokers": ["staging-kafka:9092"],
        "kafka_topic": "staging.telemetry.ingestion",
        "pdata": {
            "id": "staging.diksha.app",
            "pid": "sunbird.app",
            "ver": "2.0.102",
        },
        "user_ids": [
            "staging-user-001",
            "staging-user-002",
            "staging-user-003",
        ],
        "content_ids": [
            "do_staging_content_001",
            "do_staging_content_002",
            "do_staging_content_003",
        ],
    },
    "prod": {
        "channel": "ntp",
        "kafka_brokers": ["prod-kafka:9092"],
        "kafka_topic": "prod.telemetry.ingestion",
        "pdata": {
            "id": "prod.diksha.app",
            "pid": "sunbird.app",
            "ver": "2.0.102",
        },
        "user_ids": [
            "prod-user-001",
            "prod-user-002",
        ],
        "content_ids": [
            "do_prod_content_001",
            "do_prod_content_002",
        ],
    },
}

CONTENT_TYPES = ["resource", "collection", "textbookunit", "game", "story"]
ENVIRONMENTS_LIST = ["home", "contentplayer", "app", "public"]
PAGES = ["collection-detail", "content-detail", "explore", "play-collection"]


# ---------------------------------------------------------------------------
# Event builders — one per eid type
# ---------------------------------------------------------------------------

def _base_event(eid: str, user_id: str, content_id: str, channel: str, pdata: dict) -> dict:
    now_ms = int(time.time() * 1000)
    session_id = str(uuid.uuid4())
    device_id = hashlib.md5(user_id.encode()).hexdigest()
    content_type = random.choice(CONTENT_TYPES)
    pageid = random.choice(PAGES)
    env = random.choice(ENVIRONMENTS_LIST)

    mid = f"{eid}:{uuid.uuid4().hex}" if eid in ("IMPRESSION",) else str(uuid.uuid4())

    return {
        "eid": eid,
        "ets": now_ms - random.randint(0, 3_600_000),  # within last hour
        "ver": "3.0",
        "mid": mid,
        "actor": {
            "id": user_id,
            "type": "User",
        },
        "context": {
            "channel": channel,
            "pdata": pdata,
            "env": env,
            "sid": session_id,
            "did": device_id,
            "cdata": [],
            "rollup": {},
        },
        "object": {
            "id": content_id,
            "type": content_type,
            "ver": "1.0",
            "rollup": {
                "l1": content_id,
            },
        },
        "tags": [],
        # edata filled by caller
        "edata": {},
        # internals — removed before use
        "_pageid": pageid,
        "_content_type": content_type,
    }


def _build_START(user_id, content_id, channel, pdata):
    ev = _base_event("START", user_id, content_id, channel, pdata)
    pageid = ev.pop("_pageid")
    ct = ev.pop("_content_type")
    ev["edata"] = {"mode": "play", "pageid": pageid, "type": ct}
    return ev


def _build_END(user_id, content_id, channel, pdata):
    ev = _base_event("END", user_id, content_id, channel, pdata)
    pageid = ev.pop("_pageid")
    ct = ev.pop("_content_type")
    duration = random.randint(30, 3600)
    ev["edata"] = {"mode": "play", "pageid": pageid, "type": ct, "duration": duration}
    return ev


def _build_IMPRESSION(user_id, content_id, channel, pdata):
    ev = _base_event("IMPRESSION", user_id, content_id, channel, pdata)
    pageid = ev.pop("_pageid")
    ev.pop("_content_type")
    subtype = random.choice(["paginate", "pageexit", "detail"])
    ev["edata"] = {
        "type": "view",
        "subtype": subtype,
        "pageid": pageid,
        "uri": f"/{pageid}",
        "visits": [],
    }
    return ev


def _build_INTERACT(user_id, content_id, channel, pdata):
    ev = _base_event("INTERACT", user_id, content_id, channel, pdata)
    pageid = ev.pop("_pageid")
    ev.pop("_content_type")
    interact_types = ["TOUCH", "CLICK", "DRAG", "DROP", "SCROLL"]
    subtypes = ["content-play", "close-collection", "navigate", "select"]
    ev["edata"] = {
        "type": random.choice(interact_types),
        "subtype": random.choice(subtypes),
        "id": pageid,
        "pageid": pageid,
    }
    return ev


def _build_ASSESS(user_id, content_id, channel, pdata):
    ev = _base_event("ASSESS", user_id, content_id, channel, pdata)
    ev.pop("_pageid")
    ev.pop("_content_type")
    score = round(random.uniform(0, 1), 2)
    ev["edata"] = {
        "item": {
            "id": f"item_{uuid.uuid4().hex[:8]}",
            "maxscore": 1,
            "type": "mcq",
            "exlength": 0,
            "params": [],
            "uri": "",
            "title": "Sample Question",
            "mmc": [],
            "mc": [],
            "desc": "",
        },
        "index": random.randint(1, 10),
        "pass": "Yes" if score >= 0.5 else "No",
        "score": score,
        "resvalues": [],
        "duration": random.randint(5, 120),
    }
    return ev


def _build_ERROR(user_id, content_id, channel, pdata):
    ev = _base_event("ERROR", user_id, content_id, channel, pdata)
    ev.pop("_pageid")
    ev.pop("_content_type")
    errors = [
        ("NETWORK_ERROR", "MOBILEAPP", "Connection timeout"),
        ("CONTENT_ERROR", "CONTENTPLAYER", "Content failed to load"),
        ("AUTH_ERROR", "SDK", "Session expired"),
    ]
    err, errtype, trace = random.choice(errors)
    ev["edata"] = {"err": err, "errtype": errtype, "stacktrace": trace}
    return ev


def _build_LOG(user_id, content_id, channel, pdata):
    ev = _base_event("LOG", user_id, content_id, channel, pdata)
    ev.pop("_pageid")
    ev.pop("_content_type")
    levels = ["INFO", "WARN", "DEBUG"]
    ev["edata"] = {
        "type": "api_call",
        "level": random.choice(levels),
        "message": "TelemetryServiceImpl sync",
        "params": [{"status": "success"}, {"eventsCount": random.randint(1, 50)}],
    }
    return ev


EVENT_BUILDERS = {
    "START": _build_START,
    "END": _build_END,
    "IMPRESSION": _build_IMPRESSION,
    "INTERACT": _build_INTERACT,
    # "ASSESS": _build_ASSESS,
    # "ERROR": _build_ERROR,
    "LOG": _build_LOG,
}


# ---------------------------------------------------------------------------
# Batch envelope — wraps a list of events into api.telemetry format
# ---------------------------------------------------------------------------

def make_batch(events: list, channel: str) -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "id": "api.telemetry",
        "ver": "3.0",
        "ets": now_ms,
        "mid": str(uuid.uuid4()),
        "channel": channel,
        "pid": "sunbird.data-pipeline.test",
        "syncts": now_ms,
        "events": events,
    }


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def generate_events(
    total: int,
    user_ids: list,
    content_ids: list,
    channel: str,
    pdata: dict,
    event_types: list,
) -> list:
    events = []
    for _ in range(total):
        eid = random.choice(event_types)
        user_id = random.choice(user_ids)
        content_id = random.choice(content_ids)
        ev = EVENT_BUILDERS[eid](user_id, content_id, channel, pdata)
        events.append(ev)
    return events


def chunk_into_batches(events: list, batch_sizes=(10, 100)) -> list:
    """Split events into batches of randomly chosen size (10 or 100)."""
    batches = []
    i = 0
    while i < len(events):
        size = random.choice(batch_sizes)
        batches.append(events[i: i + size])
        i += size
    return batches


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def build_summary(events: list) -> dict:
    """
    Returns a structured summary dict:
      {
        "generated_at": "<ISO timestamp>",
        "total_events": N,
        "by_event_type": { "START": N, ... },
        "by_user": {
          "<user_id>": {
            "total": N,
            "by_event_type":   { "START": N, ... },
            "by_content_id":   { "do_xxx": N, ... },
          },
          ...
        }
      }
    """
    total_by_type = defaultdict(int)
    by_user: dict = defaultdict(lambda: {
        "total": 0,
        "by_event_type": defaultdict(int),
        "by_content_id": defaultdict(int),
    })

    for ev in events:
        eid = ev["eid"]
        uid = ev["actor"]["id"]
        cid = ev["object"]["id"]

        total_by_type[eid] += 1
        by_user[uid]["total"] += 1
        by_user[uid]["by_event_type"][eid] += 1
        by_user[uid]["by_content_id"][cid] += 1

    # Convert defaultdicts to plain dicts for serialisation
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_events": len(events),
        "by_event_type": dict(sorted(total_by_type.items())),
        "by_user": {
            uid: {
                "total": data["total"],
                "by_event_type": dict(sorted(data["by_event_type"].items())),
                "by_content_id": dict(sorted(data["by_content_id"].items())),
            }
            for uid, data in sorted(by_user.items())
        },
    }


def print_summary(summary: dict) -> None:
    all_types = sorted(summary["by_event_type"].keys())
    col_w = 10  # width per event-type column
    uid_w = 42  # width for user-id column

    sep = "─" * (uid_w + 10 + len(all_types) * (col_w + 1) + 2)

    print()
    print("=" * len(sep))
    print("  TELEMETRY GENERATION SUMMARY")
    print(f"  Generated at : {summary['generated_at']}")
    print(f"  Total events : {summary['total_events']}")
    print("=" * len(sep))

    # ── Overall totals by event type ──────────────────────────────────────
    print()
    print("  OVERALL — events by type:")
    for eid, count in summary["by_event_type"].items():
        bar = "█" * min(count, 40)
        print(f"    {eid:<12} {count:>6}  {bar}")

    # ── Per-user breakdown ────────────────────────────────────────────────
    print()
    print("  PER-USER BREAKDOWN:")
    print()

    # Header row
    header = f"  {'USER ID':<{uid_w}}  {'TOTAL':>7}  " + "  ".join(
        f"{t:>{col_w}}" for t in all_types
    )
    print(header)
    print("  " + sep)

    for uid, data in summary["by_user"].items():
        counts = "  ".join(
            f"{data['by_event_type'].get(t, 0):>{col_w}}" for t in all_types
        )
        print(f"  {uid:<{uid_w}}  {data['total']:>7}  {counts}")

    print("  " + sep)

    # Totals footer row
    footer_counts = "  ".join(
        f"{summary['by_event_type'].get(t, 0):>{col_w}}" for t in all_types
    )
    print(f"  {'TOTAL':<{uid_w}}  {summary['total_events']:>7}  {footer_counts}")

    # ── Per-user content breakdown ────────────────────────────────────────
    print()
    print("  PER-USER × CONTENT BREAKDOWN:")
    print()
    for uid, data in summary["by_user"].items():
        print(f"  {uid}")
        for cid, cnt in data["by_content_id"].items():
            print(f"    {'content':<10} {cid:<50}  {cnt:>5} events")
        print()

    print("  Use this table to verify counts in Druid after pipeline completes.")
    print("=" * len(sep))
    print()


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def push_to_kafka(batches: list, channel: str, brokers: list, topic: str, dry_run: bool) -> bool:
    """Push batches to Kafka. Returns True if all batches succeeded, False otherwise."""
    if dry_run:
        total_events = sum(len(b) for b in batches)
        print(f"[DRY-RUN] Would push {len(batches)} batches "
              f"({total_events} events) to topic '{topic}' on {brokers}")
        for i, batch in enumerate(batches):
            envelope = make_batch(batch, channel)
            print(f"  Batch {i+1}: {len(batch)} events  mid={envelope['mid']}")
        return True

    if not KAFKA_AVAILABLE:
        print(f"[ERROR] kafka-python is not installed. Run: pip install kafka-python")
        return False

    try:
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=10000,
            connections_max_idle_ms=15000,
        )
    except Exception as exc:
        print(f"[ERROR] Could not connect to Kafka broker(s) {brokers}: {exc}")
        print(f"[ERROR] No events were produced.")
        return False

    succeeded = 0
    failed = 0
    for i, batch in enumerate(batches):
        envelope = make_batch(batch, channel)
        try:
            record_meta = producer.send(topic, value=envelope).get(timeout=10)
            print(f"  Batch {i+1}/{len(batches)}: {len(batch)} events → "
                  f"partition={record_meta.partition} offset={record_meta.offset}")
            succeeded += 1
        except Exception as exc:
            print(f"  Batch {i+1}/{len(batches)}: FAILED — {exc}")
            failed += 1

    producer.flush()
    producer.close()

    total_events = sum(len(b) for b in batches)
    if failed == 0:
        print(f"\nDone. Pushed {total_events} events in {succeeded} batches to '{topic}'.")
        return True
    elif succeeded == 0:
        print(f"\n[ERROR] All {failed} batches failed. 0 events reached '{topic}' on {brokers}.")
        print(f"[ERROR] Check that the broker is reachable and the topic exists.")
        return False
    else:
        print(f"\n[WARNING] {succeeded} batches succeeded, {failed} batches failed for topic '{topic}'.")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate and push Sunbird telemetry events to Kafka."
    )
    parser.add_argument(
        "-n", "--num-events", type=int, default=100,
        help="Total number of events to generate (default: 100)",
    )
    parser.add_argument(
        "-e", "--env", choices=list(ENVIRONMENTS.keys()), default="dev",
        help="Target environment (default: dev)",
    )
    parser.add_argument(
        "--user-ids", nargs="+",
        help="Override user IDs (space-separated list)",
    )
    parser.add_argument(
        "--content-ids", nargs="+",
        help="Override content IDs (space-separated list)",
    )
    parser.add_argument(
        "--channel", help="Override channel ID",
    )
    parser.add_argument(
        "--brokers", nargs="+",
        help="Override Kafka broker(s) e.g. localhost:9092",
    )
    parser.add_argument(
        "--topic", help="Override Kafka topic name",
    )
    parser.add_argument(
        "--event-types", nargs="+",
        choices=list(EVENT_BUILDERS.keys()),
        default=list(EVENT_BUILDERS.keys()),
        help="Event types to generate (default: all types)",
    )
    parser.add_argument(
        "--batch-sizes", nargs="+", type=int, default=[10, 100],
        help="Allowed batch sizes to randomly pick from (default: 10 100)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print batch summary without connecting to Kafka",
    )
    parser.add_argument(
        "--dump-file",
        help="Also write all generated batches to a JSON file (one envelope per line)",
    )
    parser.add_argument(
        "--summary-file",
        help="Write the generation summary as JSON to this file (for Druid verification)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    env_cfg = ENVIRONMENTS[args.env]

    user_ids = args.user_ids or env_cfg["user_ids"]
    content_ids = args.content_ids or env_cfg["content_ids"]
    channel = args.channel or env_cfg["channel"]
    brokers = args.brokers or env_cfg["kafka_brokers"]
    topic = args.topic or env_cfg["kafka_topic"]
    pdata = env_cfg["pdata"]

    print(f"Environment  : {args.env}")
    print(f"Total events : {args.num_events}")
    print(f"Event types  : {args.event_types}")
    print(f"Batch sizes  : {args.batch_sizes}")
    print(f"Users pool   : {len(user_ids)} IDs")
    print(f"Content pool : {len(content_ids)} IDs")
    print(f"Kafka topic  : {topic} @ {brokers}")
    print(f"Dry run      : {args.dry_run}")
    print()

    events = generate_events(
        total=args.num_events,
        user_ids=user_ids,
        content_ids=content_ids,
        channel=channel,
        pdata=pdata,
        event_types=args.event_types,
    )

    batches = chunk_into_batches(events, batch_sizes=tuple(args.batch_sizes))

    print(f"Generated {len(events)} events split into {len(batches)} batches.")
    print()

    if args.dump_file:
        with open(args.dump_file, "w") as fh:
            for batch in batches:
                envelope = make_batch(batch, channel)
                fh.write(json.dumps(envelope) + "\n")
        print(f"Batches written to: {args.dump_file}")

    success = push_to_kafka(batches, channel, brokers, topic, dry_run=args.dry_run)

    if not success:
        raise SystemExit(1)

    summary = build_summary(events)
    print_summary(summary)

    if args.summary_file:
        with open(args.summary_file, "w") as fh:
            json.dump(summary, fh, indent=2)
        print(f"Summary JSON written to: {args.summary_file}")


if __name__ == "__main__":
    main()


# Dry-run: 500 events, dev env
# python3 test-data/generate_telemetry_events.py -n 500 --env dev --dry-run