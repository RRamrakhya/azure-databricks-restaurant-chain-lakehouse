"""Minimal, dependency-free contract validator. Returns machine-readable reason codes
(used as the quarantine_reason in the pipeline, so a bad event is explainable)."""
import json
from datetime import datetime

_TYPES = {"string": str, "number": (int, float)}


def validate(event: dict, schema: dict) -> list:
    errors = []
    for field in schema["required"]:
        if event.get(field) is None:
            errors.append(f"missing:{field}")
    for field, rules in schema["properties"].items():
        v = event.get(field)
        if v is None:
            continue
        t = _TYPES[rules["type"]]
        if not isinstance(v, t) or isinstance(v, bool):
            errors.append(f"type:{field}")
            continue
        if "enum" in rules and v not in rules["enum"]:
            errors.append(f"enum:{field}")
        if rules.get("min_length") and len(v) < rules["min_length"]:
            errors.append(f"min_length:{field}")
        if "exclusive_minimum" in rules and not v > rules["exclusive_minimum"]:
            errors.append(f"range:{field}")
        fmt = rules.get("format")
        if fmt == "iso8601":
            try:
                datetime.fromisoformat(v)
            except ValueError:
                errors.append(f"format:{field}")
        elif fmt == "json_array_nonempty":
            try:
                parsed = json.loads(v)
                if not isinstance(parsed, list) or not parsed:
                    errors.append(f"format:{field}")
            except ValueError:
                errors.append(f"format:{field}")
    return errors


def load_schema(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)
