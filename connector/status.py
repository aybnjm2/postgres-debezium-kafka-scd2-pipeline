import requests
import json

resp = requests.get("http://localhost:8083/connectors/bank-connector/status")
print(json.dumps(resp.json(), indent=2))