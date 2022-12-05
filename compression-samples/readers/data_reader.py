import pandas as pd

class DataReader:
  def read(self, root_path: str) -> pd.DataFrame:
    raise NotImplementedError()