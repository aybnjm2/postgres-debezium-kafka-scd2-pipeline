import psycopg2
import sys

# les checks : 
# idempotence
# cohérence temporelle SCD2
# intégrité référentielle
# pipeline non-vide

conn = psycopg2.connect(host="localhost", port=5432, dbname="bank", user="bank", password="bank")
cur = conn.cursor()

failures = []


def check(name, sql, expect_zero=True):
    cur.execute(sql)
    count = cur.fetchone()[0]
    ok = (count == 0) if expect_zero else (count > 0)
    status = "OK" if ok else "FAIL"
    print(f"[{status}] {name} (résultat: {count})")
    if not ok:
        failures.append(name)


# 1. Jamais deux lignes courantes pour le meme client
check(
    "Un seul is_current=true par customer_id",
    """
    SELECT count(*) FROM (
        SELECT customer_id FROM warehouse.dim_customers
        WHERE is_current = true
        GROUP BY customer_id HAVING count(*) > 1
    ) t
    """,
)

# 2. meme chose pour les comptes
check(
    "Un seul is_current=true par account_id",
    """
    SELECT count(*) FROM (
        SELECT account_id FROM warehouse.dim_accounts
        WHERE is_current = true
        GROUP BY account_id HAVING count(*) > 1
    ) t
    """,
)

# 3. Pas de trou ni de chevauchement dans l'historique d'un meme client
check(
    "Pas de chevauchement de périodes dans dim_customers",
    """
    SELECT count(*) FROM (
        SELECT customer_id, valid_from,
               LAG(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from) AS prev_valid_to
        FROM warehouse.dim_customers
    ) t
    WHERE prev_valid_to IS NOT NULL AND valid_from < prev_valid_to
    """,
)

# 4. Chaque transaction fait reference a un compte qui existe dans dim_accounts
check(
    "Toutes les transactions référencent un account_id connu",
    """
    SELECT count(*) FROM warehouse.fact_transactions f
    WHERE NOT EXISTS (
        SELECT 1 FROM warehouse.dim_accounts a WHERE a.account_id = f.account_id
    )
    """,
)

# 5. On a bien des donnees (pipeline pas vide)
check(
    "Au moins une ligne dans dim_customers",
    "SELECT count(*) FROM warehouse.dim_customers",
    expect_zero=False,
)

cur.close()
conn.close()

if failures:
    print(f"\n{len(failures)} test(s) en échec: {failures}")
    sys.exit(1)
else:
    print("\nTous les tests passent.")
    sys.exit(0)