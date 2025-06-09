from collections import Counter
from dateutil.parser import isoparse

# Assuming 'events' is the merged list of trade and order book events
event_types = [event['type'] for event in events]
type_counts = Counter(event_types)
print(f"Event Type Counts: {type_counts}")

# Extract timestamps
timestamps = [
    isoparse(event.get('trade_time') or event.get('timestamp'))
    for event in events
]

# Check if timestamps are sorted
is_sorted = all(earlier <= later for earlier, later in zip(timestamps, timestamps[1:]))
print(f"Events are sorted: {is_sorted}")

# Display first and last timestamps
print(f"First event timestamp: {timestamps[0]}")
print(f"Last event timestamp: {timestamps[-1]}")
