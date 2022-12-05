from data_generator import generate_samples
from datetime import datetime
import os
from writers import delta_writer, parquet_writer
from readers import parquet_reader
import timer

# Generation configuration
number_of_nodes = 10000
number_of_values_per_node = 1
timestamp_variance_in_seconds = 259200
start_time = datetime(2022, 12, 4)

# output directories
root_output_directory = "output"
delta_output_root_directory = f"{root_output_directory}/delta"
parquet_output_root_directory = f"{root_output_directory}/parquet"

# init writers
delta_writer = delta_writer.DeltaWriter()
parquet_writer = parquet_writer.ParquetWriter()

# init readers
parquet_reader = parquet_reader.ParquetReader()

# init helpers
stop_watch = timer.Timer()

def main():
  os.makedirs(root_output_directory, exist_ok=True)

  print("generating sample data...")
  df = generate_samples(number_of_nodes, number_of_values_per_node, timestamp_variance_in_seconds, start_time)
  
  df['year'] = df['SourceTimestamp'].dt.year
  df['month'] = df['SourceTimestamp'].dt.month
  df['day'] = df['SourceTimestamp'].dt.day
  
  parquet_df = df.copy(True)
  delta_df = df.copy(True)

  partition_columns = ['year', 'month', 'day']

  print("\n\n")
  print("========= Started Running Parquet Writer Test =========")
  stop_watch.start()
  parquet_writer.convert(parquet_df, partition_columns, parquet_output_root_directory)
  stop_watch.stop()
  print("========= Finished Running Parquet Writer Test =========")

  # print("\n\n")
  # print("========= Started Running Delta Writer Test =========")
  # stop_watch.start()
  # delta_writer.convert(delta_df, partition_columns, delta_output_root_directory)
  # stop_watch.stop()
  # print("========= Finished Running Delta Writer Test =========")

  print("\n\n")
  print("========= Started Running Parquet Reader Test =========")
  stop_watch.start()
  parquet_reader.read(parquet_output_root_directory)
  stop_watch.stop()
  print("========= Finished Running Parquet Writer Test =========")

if __name__ == "__main__":
  main()
