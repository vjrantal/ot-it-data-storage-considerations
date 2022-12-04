from datetime import datetime, timedelta
import json
import pandas as pd
import random
from uuid import uuid4

def create_sample(node_id: str, value: int, source_timestamp: datetime, status_code: str):
  return {
    "NodeId": node_id,
    "Value": value,
    "SourceTimestamp": source_timestamp,
    "StatusCode": status_code
  }

def generate_samples(
  number_of_nodes: int,
  number_of_values_per_node: int,
  timestamp_variance_in_seconds: int,
  start_time: datetime):
  samples = []
  status_code = 0

  for _ in range(number_of_nodes):
    guid = str(uuid4())
    node_id = f"ns=2;s=Number\\{guid}"

    value = random.randint(0, number_of_nodes)
    source_timestamp_delta_in_seconds = random.randint(0, timestamp_variance_in_seconds)
    source_timestamp = start_time + timedelta(0, source_timestamp_delta_in_seconds)

    sample = create_sample(node_id, value, source_timestamp, status_code)
    samples.append(sample)

  df = pd.DataFrame(samples)
  return df

if __name__ == "__main__":
  number_of_nodes = 10000
  number_of_values_per_node = 1
  timestamp_variance_in_seconds = 720*60
  start_time = datetime(2022, 12, 4)
  df = generate_samples(number_of_nodes, number_of_values_per_node, timestamp_variance_in_seconds, start_time)
  print(df.head())