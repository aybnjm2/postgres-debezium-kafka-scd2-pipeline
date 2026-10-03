import json
import requests

with open("connector/register-postgres.json") as f:
    config = json.load(f)

resp = requests.post(
    "http://localhost:8083/connectors",
    headers={"Content-Type": "application/json"},
    json=config,
)
print(resp.status_code)
print(json.dumps(resp.json(), indent=2))