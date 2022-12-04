import pandas as pd
from writers.data_writer import DataWriter

class AvroWriter(DataWriter):
  df = None

  def __init__(self, df: pd.DataFrame):
    self.df = df

  def convert(self):
    self.df = self.df.sort_values(by="col_1")
    records = self.df.to_dict('records')
    print(records)


if __name__ == "__main__":
  data = [{"col_1": 3, "col_2": 4}, {"col_1": 1, "col_2": 2}, {"col_1": 5, "col_2": 6}]
  df = pd.DataFrame(data)

  avro_writer = AvroWriter(df)
  avro_writer.convert()