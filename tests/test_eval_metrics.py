import csv
import os
import random
import tempfile
import unittest

from eval import sentiment_eval as ev
from src.generators import generate_batch_data as g


class TestEval(unittest.TestCase):
    def _rows(self):
        rng = random.Random(9)
        rest, cust = g.gen_restaurants(), g.gen_customers(40, random.Random(9))
        menu = g.gen_menu_items(rest)
        orders = g.gen_historical_orders(800, cust, rest, menu, rng)
        return g.gen_reviews(orders, rng, rate=0.5)

    def test_baseline_scores_reasonably_and_metrics_bounded(self):
        rows = self._rows()
        res = ev.score(rows, {r["review_id"]: ev.baseline_predict(r["review_text"]) for r in rows})
        self.assertEqual(res["json_valid_rate"], 1.0)
        # keyword baseline is strong on explicit issues but over-flags "great portion" praise (a known weakness an LLM should fix)
        self.assertEqual(res["delivery_recall"], 1.0)
        self.assertLess(res["portion_size_precision"], 0.5)
        for k, v in res.items():
            if k.endswith(("precision", "recall", "accuracy", "rate")):
                self.assertTrue(0 <= v <= 1, k)

    def test_invalid_llm_output_counts_against_validity(self):
        rows = self._rows()[:10]
        preds = {r["review_id"]: ev.parse_llm("sorry, I cannot") for r in rows}
        self.assertEqual(ev.score(rows, preds)["json_valid_rate"], 0)

    def test_parser_handles_code_fences(self):
        raw = 'Sure!\n```json\n{"sentiment": "negative", "issue_delivery": true}\n```'
        self.assertEqual(ev.parse_llm(raw)["sentiment"], "negative")


if __name__ == "__main__":
    unittest.main()
