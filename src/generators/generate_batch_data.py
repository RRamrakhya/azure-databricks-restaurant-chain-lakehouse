"""Synthetic batch data for the restaurant lakehouse (customers, restaurants,
menu_items, historical_orders, reviews). Deterministic given --seed.
No PII: names/emails are fabricated."""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Pune"]
FIRST = ["Aarav", "Diya", "Vihaan", "Anaya", "Kabir", "Isha", "Rohan", "Meera", "Arjun", "Sana"]
LAST = ["Sharma", "Patel", "Iyer", "Khan", "Gupta", "Nair", "Singh", "Das", "Reddy", "Joshi"]
MENU = [
    ("Butter Chicken", "Curry", 350), ("Paneer Tikka Masala", "Curry", 320),
    ("Dal Makhani", "Curry", 260), ("Garlic Naan", "Bread", 70),
    ("Tandoori Roti", "Bread", 40), ("Hyderabadi Biryani", "Rice", 380),
    ("Gulab Jamun", "Dessert", 120), ("Masala Chai", "Beverage", 60),
    ("Mango Lassi", "Beverage", 110), ("Chicken Tikka", "Starter", 300),
]
ORDER_TYPES = ["dine-in", "takeaway", "delivery"]
# (hour, weight): lunch and dinner peaks
HOUR_WEIGHTS = {11: 2, 12: 6, 13: 7, 14: 4, 15: 1, 18: 3, 19: 7, 20: 9, 21: 6, 22: 2}

POS_TEMPLATES = ["Loved the {item}, great portion and service.", "Excellent {item}, will order again."]
NEG = {
    "delivery": ["Food arrived cold and the packaging was torn.", "Delivery took over an hour, very late."],
    "food_quality": ["The {item} was stale and tasteless.", "Found the {item} undercooked."],
    "pricing": ["Way too expensive for what you get.", "Prices are too high for this quality."],
    "portion_size": ["Portion size was tiny for the price.", "Very small portion of {item}."],
}


def gen_restaurants(n=5):
    names = ["Spice Route", "Curry Leaf", "Tandoor Tales", "Saffron House", "Masala Mile"]
    return [
        {"restaurant_id": f"REST-{i+1:02d}", "restaurant_name": names[i % len(names)],
         "city": CITIES[i % len(CITIES)], "cuisine_type": "North Indian",
         "manager_name": f"{FIRST[i]} {LAST[i]}", "seating_capacity": 40 + 10 * i,
         "opening_year": 2012 + i}
        for i in range(n)
    ]


def gen_customers(n, rng):
    rows = []
    for i in range(n):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        rows.append({"customer_id": f"C-{i+1:04d}", "first_name": fn, "last_name": ln,
                     "email": f"{fn.lower()}.{ln.lower()}{i+1}@example.com",
                     "phone": f"+91-9{rng.randint(100000000, 999999999)}",
                     "city": rng.choice(CITIES),
                     "join_date": (datetime(2022, 1, 1) + timedelta(days=rng.randint(0, 900))).date().isoformat()})
    return rows


def gen_menu_items(restaurants):
    rows, k = [], 1
    for r in restaurants:
        for name, cat, price in MENU:
            rows.append({"menu_item_id": f"M{k:03d}", "item_name": name, "item_category": cat,
                         "unit_price": price, "restaurant_id": r["restaurant_id"],
                         "is_available": True})
            k += 1
    return rows


def _pick_hour(rng):
    hours, weights = zip(*HOUR_WEIGHTS.items())
    return rng.choices(hours, weights)[0]


def make_order(rng, order_id, customers, restaurants, menu, ts):
    rest = rng.choice(restaurants)
    items_pool = [m for m in menu if m["restaurant_id"] == rest["restaurant_id"]]
    lines = []
    for m in rng.sample(items_pool, rng.randint(1, 4)):
        q = rng.randint(1, 3)
        lines.append({"item_id": m["menu_item_id"], "item_name": m["item_name"],
                      "item_category": m["item_category"], "quantity": q,
                      "unit_price": float(m["unit_price"]),
                      "subtotal": round(q * m["unit_price"], 2)})
    return {"order_id": order_id, "customer_id": rng.choice(customers)["customer_id"],
            "restaurant_id": rest["restaurant_id"], "order_timestamp": ts.isoformat(),
            "total_amount": round(sum(l["subtotal"] for l in lines), 2),
            "items": json.dumps(lines), "order_type": rng.choice(ORDER_TYPES),
            "status": "completed"}


def gen_historical_orders(n, customers, restaurants, menu, rng, start=datetime(2024, 1, 1), days=365):
    rows = []
    for i in range(n):
        day = start + timedelta(days=rng.randint(0, days - 1))
        # weekend surge: re-roll once for Mon-Thu to bias toward Fri-Sun
        if day.weekday() < 4 and rng.random() < 0.35:
            day += timedelta(days=4 - day.weekday())
        ts = day.replace(hour=_pick_hour(rng), minute=rng.randint(0, 59), second=rng.randint(0, 59))
        rows.append(make_order(rng, f"H-{i+1:06d}", customers, restaurants, menu, ts))
    return rows


def gen_reviews(orders, rng, rate=0.25):
    rows, k = [], 1
    for o in orders:
        if rng.random() > rate:
            continue
        item = json.loads(o["items"])[0]["item_name"]
        issue = rng.choices(["none", "delivery", "food_quality", "pricing", "portion_size"],
                            [55, 15, 12, 10, 8])[0]
        if issue == "none":
            text, rating = rng.choice(POS_TEMPLATES).format(item=item), rng.choice([4, 5, 5])
        else:
            text, rating = rng.choice(NEG[issue]).format(item=item), rng.choice([1, 2, 2, 3])
        rows.append({"review_id": f"R-{k:05d}", "order_id": o["order_id"],
                     "customer_id": o["customer_id"], "restaurant_id": o["restaurant_id"],
                     "rating": rating, "review_text": text, "_true_issue": issue})
        k += 1
    return rows


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/batch")
    p.add_argument("--customers", type=int, default=500)
    p.add_argument("--orders", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    rng = random.Random(a.seed)
    os.makedirs(a.out, exist_ok=True)
    rest = gen_restaurants()
    cust = gen_customers(a.customers, rng)
    menu = gen_menu_items(rest)
    orders = gen_historical_orders(a.orders, cust, rest, menu, rng)
    reviews = gen_reviews(orders, rng)
    for name, rows in [("restaurants", rest), ("customers", cust), ("menu_items", menu),
                       ("historical_orders", orders), ("reviews", reviews)]:
        write_csv(os.path.join(a.out, f"{name}.csv"), rows)
    print({k: len(v) for k, v in [("restaurants", rest), ("customers", cust), ("menu_items", menu),
                                  ("orders", orders), ("reviews", reviews)]})


if __name__ == "__main__":
    main()
