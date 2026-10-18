import json
import requests
from pathlib import Path

config_path = Path(__file__).parent / "register-postgres.json"
with open(config_path) as f:
    config = json.load(f)

resp = requests.post(
    "http://localhost:8083/connectors",
    headers={"Content-Type": "application/json"},
    json=config,
)
print(resp.status_code)
print(json.dumps(resp.json(), indent=2))