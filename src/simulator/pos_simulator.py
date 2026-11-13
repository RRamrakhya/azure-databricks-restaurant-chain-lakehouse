"""POS order simulator. Modes:
  files     -> writes JSON-lines files into a folder / UC Volume (works on Databricks Free Edition)
  eventhub  -> publishes to Azure Event Hub (needs: pip install azure-eventhub;
               env EVENTHUB_SEND_CONNECTION_STRING; never commit the string)
Use --bad-rate to inject malformed events and --dup-rate to inject duplicates,
so the quarantine and idempotency paths get exercised."""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from src.generators.generate_batch_data import (gen_customers, gen_menu_items,
                                                gen_restaurants, make_order)


def build_event(rng, customers, restaurants, menu, now=None, bad_rate=0.0, seq=0):
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    ev = make_order(rng, f"L-{now:%Y%m%d}-{uuid.UUID(int=rng.getrandbits(128)).hex[:8]}",
                    customers, restaurants, menu, now)
    if rng.random() < bad_rate:
        kind = rng.choice(["null_customer", "neg_amount", "bad_items", "bad_type"])
        if kind == "null_customer":
            ev["customer_id"] = None
        elif kind == "neg_amount":
            ev["total_amount"] = -5.0
        elif kind == "bad_items":
            ev["items"] = "not-json"
        else:
            ev["order_type"] = "teleport"
    return ev


def run(args):
    rng = random.Random(args.seed)
    rest = gen_restaurants()
    cust = gen_customers(300, rng)
    menu = gen_menu_items(rest)
    sender = None
    if args.mode == "eventhub":
        from azure.eventhub import EventData, EventHubProducerClient  # lazy import
        sender = EventHubProducerClient.from_connection_string(
            os.environ["EVENTHUB_SEND_CONNECTION_STRING"], eventhub_name="orders")
    else:
        os.makedirs(args.out, exist_ok=True)
    last = None
    for i in range(args.count):
        ev = build_event(rng, cust, rest, menu, bad_rate=args.bad_rate, seq=i)
        batch = [ev]
        if last and rng.random() < args.dup_rate:
            batch.append(last)  # at-least-once delivery duplicate
        last = ev
        if sender:
            b = sender.create_batch()
            for e in batch:
                b.add(EventData(json.dumps(e)))
            sender.send_batch(b)
        else:
            path = os.path.join(args.out, f"orders_{int(time.time()*1000)}_{i}.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(json.dumps(e) for e in batch) + "\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["files", "eventhub"], default="files")
    ap.add_argument("--out", default="data/live")
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--interval", type=float, default=3.0)
    ap.add_argument("--bad-rate", type=float, default=0.05)
    ap.add_argument("--dup-rate", type=float, default=0.03)
    ap.add_argument("--seed", type=int, default=7)
    run(ap.parse_args())
