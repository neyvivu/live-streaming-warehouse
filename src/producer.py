"""Synthetic live-stream event producer.

Emits JSON events (join / comment / gift / leave) shaped like a live-streaming
platform's clickstream. Writes to Kafka when a broker is reachable, otherwise
falls back to newline-delimited JSON files that the streaming job can tail.

    python -m src.producer --sink kafka --rate 200 --seconds 60
    python -m src.producer --sink file  --rate 200 --seconds 60
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

TOPIC = "live_events"
EVENT_TYPES = ["join", "comment", "gift", "leave"]
EVENT_WEIGHTS = [0.25, 0.45, 0.10, 0.20]
GIFT_TIERS = {"rose": 1, "star": 10, "rocket": 100, "castle": 1000}
COUNTRIES = ["SG", "VN", "ID", "MY", "TH", "PH"]


def make_event(rooms, users, late_pct=0.03):
    now = datetime.now(timezone.utc)
    # A small share of events arrive late — this is what the watermark handles.
    lag_s = random.randint(20, 90) if random.random() < late_pct else random.randint(0, 3)
    event_ts = now.timestamp() - lag_s
    etype = random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]
    gift = random.choice(list(GIFT_TIERS)) if etype == "gift" else None
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": etype,
        "event_ts": datetime.fromtimestamp(event_ts, timezone.utc).isoformat(),
        "ingest_ts": now.isoformat(),
        "room_id": random.choice(rooms),
        "user_id": random.choice(users),
        "country": random.choice(COUNTRIES),
        "gift_name": gift,
        "coins": GIFT_TIERS[gift] if gift else 0,
        "comment_len": random.randint(1, 120) if etype == "comment" else 0,
    }


def run(sink, rate, seconds, rooms_n, users_n, outdir, bootstrap):
    rooms = [f"room_{i:04d}" for i in range(rooms_n)]
    users = [f"user_{i:06d}" for i in range(users_n)]
    producer = None

    if sink == "kafka":
        from kafka import KafkaProducer  # imported lazily so file mode needs no kafka lib
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50,
        )
        print(f"producing to kafka topic '{TOPIC}' at {bootstrap}")
    else:
        os.makedirs(outdir, exist_ok=True)
        print(f"producing to files under {outdir}/")

    sent, started = 0, time.time()
    try:
        while time.time() - started < seconds:
            batch = [make_event(rooms, users) for _ in range(rate)]
            if producer:
                for e in batch:
                    producer.send(TOPIC, e)
                producer.flush()
            else:
                path = os.path.join(outdir, f"events-{int(time.time()*1000)}.json")
                tmp = path + ".tmp"  # write then rename so readers never see partial files
                with open(tmp, "w", encoding="utf-8") as f:
                    for e in batch:
                        f.write(json.dumps(e) + "\n")
                os.replace(tmp, path)
            sent += len(batch)
            print(f"  sent {sent} events", end="\r", flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.close()
    print(f"\ndone: {sent} events in {int(time.time()-started)}s")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--sink", choices=["kafka", "file"], default="file")
    p.add_argument("--rate", type=int, default=200, help="events per second")
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--rooms", type=int, default=50)
    p.add_argument("--users", type=int, default=5000)
    p.add_argument("--outdir", default="data/raw")
    p.add_argument("--bootstrap", default="localhost:9092")
    a = p.parse_args()
    run(a.sink, a.rate, a.seconds, a.rooms, a.users, a.outdir, a.bootstrap)
