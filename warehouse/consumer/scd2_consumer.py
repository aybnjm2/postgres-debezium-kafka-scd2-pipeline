import json
from datetime import datetime, timezone
import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer

WAREHOUSE_CONN = dict(
    host="localhost", port=5432, dbname="bank", user="bank", password="bank"
)

TOPICS = [
    "bank.public.customers",
    "bank.public.accounts",
    "bank.public.transactions",
]

conn = psycopg2.connect(**WAREHOUSE_CONN)
conn.autocommit = False
cur = conn.cursor()


def now():
    return datetime.now(timezone.utc)


def close_current_row(table, business_key_col, business_key_val, lsn, event_ts):
    """Ferme la ligne is_current=true si elle existe et n'est pas deja plus recente que lsn."""
    cur.execute(
        f"""
        UPDATE warehouse.{table}
        SET valid_to = %s, is_current = false
        WHERE {business_key_col} = %s AND is_current = true AND source_lsn < %s
        """,
        (event_ts, business_key_val, lsn),
    )


def upsert_scd2_customer(before, after, op, lsn, event_ts):
    if op == "d":
        close_current_row("dim_customers", "customer_id", before["customer_id"], lsn, event_ts)
        return

    cid = after["customer_id"] # idempotence si on a deja une ligne courante avec un LSN >= a celui ci on ignore
    cur.execute(
        "SELECT source_lsn FROM warehouse.dim_customers WHERE customer_id=%s AND is_current=true",
        (cid,),
    )
    row = cur.fetchone()
    if row and row[0] is not None and row[0] >= lsn:
        return  # evenement deja applique ou plus ancien -> ignore (idempotence)

    close_current_row("dim_customers", "customer_id", cid, lsn, event_ts)
    cur.execute(
        """
        INSERT INTO warehouse.dim_customers
            (customer_id, full_name, email, city, valid_from, valid_to, is_current, source_lsn)
        VALUES (%s, %s, %s, %s, %s, NULL, true, %s)
        """,
        (cid, after["full_name"], after["email"], after["city"], event_ts, lsn),
    )


def upsert_scd2_account(before, after, op, lsn, event_ts):
    if op == "d":
        close_current_row("dim_accounts", "account_id", before["account_id"], lsn, event_ts)
        return

    aid = after["account_id"]
    cur.execute(
        "SELECT source_lsn FROM warehouse.dim_accounts WHERE account_id=%s AND is_current=true",
        (aid,),
    )
    row = cur.fetchone()
    if row and row[0] is not None and row[0] >= lsn:
        return

    close_current_row("dim_accounts", "account_id", aid, lsn, event_ts)
    cur.execute(
        """
        INSERT INTO warehouse.dim_accounts
            (account_id, customer_id, account_type, balance, status, valid_from, valid_to, is_current, source_lsn)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, true, %s)
        """,
        (aid, after["customer_id"], after["account_type"], after["balance"], after["status"], event_ts, lsn),
    )


def insert_fact_transaction(after, lsn):
    if after is None:
        return  # delete sur une table de faits -> on ignore pour l'instant
    cur.execute(
        """
        INSERT INTO warehouse.fact_transactions (txn_id, account_id, amount, created_at, source_lsn)
        VALUES (%s, %s, %s, to_timestamp(%s / 1000000.0), %s)
        ON CONFLICT (txn_id) DO NOTHING
        """,
        (after["txn_id"], after["account_id"], after["amount"], after["created_at"], lsn),
    )


def handle_message(topic, payload):
    op = payload["op"]
    before = payload.get("before")
    after = payload.get("after")
    lsn = payload["source"]["lsn"]
    event_ts = datetime.fromtimestamp(payload["ts_ms"] / 1000, tz=timezone.utc)

    if topic.endswith("customers"):
        upsert_scd2_customer(before, after, op, lsn, event_ts)
    elif topic.endswith("accounts"):
        upsert_scd2_account(before, after, op, lsn, event_ts)
    elif topic.endswith("transactions"):
        insert_fact_transaction(after, lsn)


def main():
    schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = DeserializingConsumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "scd2-warehouse-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "value.deserializer": avro_deserializer,
    })
    consumer.subscribe(TOPICS)
    print("Consumer SCD2 demarre...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("ERREUR:", msg.error())
                continue

            payload = msg.value()   # decoded to dictionary by AvroDeserializer
            try:
                handle_message(msg.topic(), payload)
                conn.commit()
                consumer.commit(msg)  # on ne commit l'offset Kafka QU'APRES le commit DB
                print(f"[{msg.topic()}] op={payload['op']} applique, offset={msg.offset()}")
            except Exception as e:
                conn.rollback()
                print(f"ECHEC sur offset {msg.offset()}: {e}")
                # on ne commit pas l'offset -> le message sera retente au redemarrage
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()