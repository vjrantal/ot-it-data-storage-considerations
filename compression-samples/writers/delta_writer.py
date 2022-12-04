import pandas as pd
from deltalake.writer import write_deltalake
from writers.data_writer import DataWriter

class DeltaWriter(DataWriter):
  def convert(self, df: pd.DataFrame, partitions: list[str], root_path: str):
    write_deltalake(root_path, df, partition_by=partitions)


if __name__ == "__main__":
  root_path = "delta_data"
  data = [{"col_1": 3, "col_2": 4}, {"col_1": 1, "col_2": 2}, {"col_1": 5, "col_2": 6}]
  df = pd.DataFrame(data)

  avro_writer = DeltaWriter()
  avro_writer.convert(df, ["col_1"], root_path)