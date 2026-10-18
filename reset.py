import subprocess
import time
import requests
from pathlib import Path

ROOT = Path(__file__).parent
CONNECTOR_CONFIG = ROOT / "warehouse" / "connector" / "register-postgres.json"


def run(cmd):
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def wait_for_connect(timeout=60):
    print("Attente de Kafka Connect...")
    for _ in range(timeout):
        try:
            r = requests.get("http://localhost:8083/connectors")
            if r.status_code == 200:
                print("Kafka Connect prêt.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    raise TimeoutError("Kafka Connect n'a pas démarré à temps")


def register_connector():
    import json
    with open(CONNECTOR_CONFIG) as f:
        config = json.load(f)
    resp = requests.post(
        "http://localhost:8083/connectors",
        headers={"Content-Type": "application/json"},
        json=config,
    )
    print(f"Enregistrement du connecteur: {resp.status_code}")
    if resp.status_code not in (200, 201, 409):
        print(resp.json())
        raise RuntimeError("Échec de l'enregistrement du connecteur")


if __name__ == "__main__":
    run(["docker", "compose", "down", "-v"])
    run(["docker", "compose", "up", "-d", "--build"])
    wait_for_connect()
    time.sleep(5)  # laisse Schema Registry finir de démarrer aussi
    register_connector()
    print("\nPipeline prêt. Lancez maintenant:")
    print("  python warehouse/generator/generate.py --delay 0.5")
    print("  python warehouse/consumer/scd2_consumer.py")