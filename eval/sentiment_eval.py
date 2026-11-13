"""Evaluation harness for the LLM review-enrichment step.
Why: an LLM in an ETL pipeline is a component that can silently degrade. We measure it against a labelled
golden set, compare with a cheap keyword baseline, and track JSON-validity. Run on every prompt/model change.
Usage:
  python -m eval.sentiment_eval --reviews data/batch/reviews.csv              # baseline only
  python -m eval.sentiment_eval --reviews ... --predictions llm_preds.jsonl   # score LLM output
predictions.jsonl: one JSON object per line: {"review_id": ..., "raw": "<model output string>"}"""
import argparse
import csv
import json
import re

ISSUES = ["delivery", "food_quality", "pricing", "portion_size"]
KEYWORDS = {
    "delivery": ["cold", "packaging", "late", "delivery took"],
    "food_quality": ["stale", "undercooked", "tasteless"],
    "pricing": ["expensive", "prices are too high"],
    "portion_size": ["portion", "tiny", "small"],
}


def baseline_predict(text):
    t = text.lower()
    flags = {k: any(w in t for w in ws) for k, ws in KEYWORDS.items()}
    sentiment = "negative" if any(flags.values()) else "positive"
    return {"sentiment": sentiment, **{f"issue_{k}": v for k, v in flags.items()}}


def parse_llm(raw):
    """Same extraction rule as the SQL (greedy {...}). Returns dict or None if invalid."""
    m = re.search(r"\{.*\}", raw or "", re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except ValueError:
        return None
    return d if d.get("sentiment") in {"positive", "neutral", "negative"} else None


def truth(row):
    t = row["_true_issue"]
    flags = {k: (t == k) for k in ISSUES}
    return {"sentiment": "positive" if t == "none" else "negative", **{f"issue_{k}": v for k, v in flags.items()}}


def _truthy(v):
    return v is True or str(v).lower() == "true"


def score(rows, preds):
    """preds: dict review_id -> prediction dict or None (invalid)."""
    n, valid, sent_ok = len(rows), 0, 0
    tp = {k: 0 for k in ISSUES}; fp = dict(tp); fn = dict(tp)
    for r in rows:
        p, t = preds.get(r["review_id"]), truth(r)
        if p is None:
            for k in ISSUES:
                fn[k] += t[f"issue_{k}"]
            continue
        valid += 1
        sent_ok += p["sentiment"] == t["sentiment"]
        for k in ISSUES:
            pv, tv = _truthy(p.get(f"issue_{k}")), t[f"issue_{k}"]
            tp[k] += pv and tv; fp[k] += pv and not tv; fn[k] += (not pv) and tv
    out = {"n": n, "json_valid_rate": valid / n if n else 0, "sentiment_accuracy": sent_ok / valid if valid else 0}
    for k in ISSUES:
        out[f"{k}_precision"] = tp[k] / (tp[k] + fp[k]) if tp[k] + fp[k] else 0.0
        out[f"{k}_recall"] = tp[k] / (tp[k] + fn[k]) if tp[k] + fn[k] else 0.0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reviews", required=True)
    ap.add_argument("--predictions")
    a = ap.parse_args()
    with open(a.reviews, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    base = {r["review_id"]: baseline_predict(r["review_text"]) for r in rows}
    print("BASELINE", json.dumps(score(rows, base), indent=1))
    if a.predictions:
        with open(a.predictions, encoding="utf-8") as f:
            llm = {j["review_id"]: parse_llm(j["raw"]) for j in map(json.loads, f)}
        print("LLM", json.dumps(score(rows, llm), indent=1))


if __name__ == "__main__":
    main()
