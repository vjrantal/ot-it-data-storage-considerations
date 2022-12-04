import pandas as pd

class DataWriter:
  def convert(self, df: pd.DataFrame, partitions: list[str], root_path: str):
    raise NotImplementedError()