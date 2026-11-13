import json
import random
import unittest

from src.generators import generate_batch_data as g


class TestGenerators(unittest.TestCase):
    def setUp(self):
        self.rng = random.Random(1)
        self.rest = g.gen_restaurants()
        self.cust = g.gen_customers(50, self.rng)
        self.menu = g.gen_menu_items(self.rest)

    def test_order_total_matches_items(self):
        orders = g.gen_historical_orders(200, self.cust, self.rest, self.menu, self.rng)
        for o in orders:
            self.assertAlmostEqual(o["total_amount"], sum(i["subtotal"] for i in json.loads(o["items"])), places=2)

    def test_order_ids_unique_and_fks_valid(self):
        orders = g.gen_historical_orders(300, self.cust, self.rest, self.menu, self.rng)
        self.assertEqual(len({o["order_id"] for o in orders}), 300)
        rids = {r["restaurant_id"] for r in self.rest}
        cids = {c["customer_id"] for c in self.cust}
        self.assertTrue(all(o["restaurant_id"] in rids and o["customer_id"] in cids for o in orders))

    def test_deterministic(self):
        a = g.gen_customers(10, random.Random(5))
        b = g.gen_customers(10, random.Random(5))
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
