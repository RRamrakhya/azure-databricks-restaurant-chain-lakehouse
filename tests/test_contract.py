import os
import random
import unittest

from src.contracts.validate import load_schema, validate
from src.generators import generate_batch_data as g
from src.simulator.pos_simulator import build_event

SCHEMA = load_schema(os.path.join(os.path.dirname(__file__), "..", "src", "contracts", "order_event.schema.json"))


class TestContract(unittest.TestCase):
    def setUp(self):
        self.rng = random.Random(3)
        self.rest = g.gen_restaurants()
        self.cust = g.gen_customers(20, self.rng)
        self.menu = g.gen_menu_items(self.rest)

    def test_clean_events_pass(self):
        for _ in range(200):
            self.assertEqual(validate(build_event(self.rng, self.cust, self.rest, self.menu), SCHEMA), [])

    def test_bad_events_are_caught_with_reason_codes(self):
        seen = set()
        for _ in range(400):
            ev = build_event(self.rng, self.cust, self.rest, self.menu, bad_rate=1.0)
            errs = validate(ev, SCHEMA)
            self.assertTrue(errs)
            seen.update(e.split(":")[0] for e in errs)
        self.assertTrue({"missing", "range", "format", "enum"} <= seen)

    def test_pipeline_rule_names_cover_contract_fields(self):
        src = open(os.path.join(os.path.dirname(__file__), "..", "src", "pipelines", "01_bronze_live_orders.py")).read()
        for field in SCHEMA["required"]:
            self.assertIn(field, src)


if __name__ == "__main__":
    unittest.main()
