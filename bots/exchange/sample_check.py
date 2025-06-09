import json
from dateutil.parser import isoparse
from collections import defaultdict

with open("sample_events.json", "r") as f:
    events = json.load(f)

time_bounds = defaultdict(lambda: {"first": None, "last": None})

for event in events:
    event_type = event["type"]
    timestamp_str = event.get("trade_time") or event.get("timestamp")
    timestamp = isoparse(timestamp_str)

    if time_bounds[event_type]["first"] is None or timestamp < time_bounds[event_type]["first"]:
        time_bounds[event_type]["first"] = timestamp
    if time_bounds[event_type]["last"] is None or timestamp > time_bounds[event_type]["last"]:
        time_bounds[event_type]["last"] = timestamp

for event_type, bounds in time_bounds.items():
    print(f"{event_type.upper()} range: {bounds['first']} to {bounds['last']}")
