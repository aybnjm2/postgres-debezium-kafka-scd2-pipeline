import random
import time
import argparse
import psycopg2
from faker import Faker

fake = Faker()

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="bank", user="bank", password="bank"
)
cur = conn.cursor()


def random_id(table, pk, where=""):
    cur.execute(f"SELECT {pk} FROM {table} {where} ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def new_customer():
    cur.execute(
        "INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s) RETURNING customer_id",
        (fake.name(), fake.unique.email(), fake.city()),
    )
    cid = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO accounts (customer_id, account_type, balance) VALUES (%s, %s, %s)",
        (cid, random.choice(["checking", "savings", "credit"]), 100),
    )
    return f"NEW customer {cid}"


def move_customer():
    cid = random_id("customers", "customer_id")
    if cid is None:
        return "no customers yet"
    cur.execute(
        "UPDATE customers SET city = %s, updated_at = now() WHERE customer_id = %s",
        (fake.city(), cid),
    )
    return f"UPDATE customer {cid} moved city"


def make_transaction():
    aid = random_id("accounts", "account_id", "WHERE status = 'open'")
    if aid is None:
        return "no open accounts yet"
    amount = round(random.uniform(-200, 500), 2)
    cur.execute(
        "INSERT INTO transactions (account_id, amount) VALUES (%s, %s)", (aid, amount)
    )
    cur.execute(
        "UPDATE accounts SET balance = balance + %s, updated_at = now() WHERE account_id = %s",
        (amount, aid),
    )
    return f"TXN account {aid} amount {amount}"


def close_account():
    aid = random_id("accounts", "account_id", "WHERE status = 'open'")
    if aid is None:
        return "no open accounts yet"
    cur.execute(
        "UPDATE accounts SET status = 'closed', updated_at = now() WHERE account_id = %s",
        (aid,),
    )
    return f"CLOSE account {aid}"


ACTIONS = [
    (new_customer, 15),
    (move_customer, 15),
    (make_transaction, 65),
    (close_account, 5),
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between actions")
    args = parser.parse_args()

    funcs, weights = zip(*ACTIONS)
    print("Generating changes... Ctrl+C to stop")
    while True:
        action = random.choices(funcs, weights=weights)[0]
        print(action())
        conn.commit()  # each action = one transaction = one commit
        time.sleep(args.delay)