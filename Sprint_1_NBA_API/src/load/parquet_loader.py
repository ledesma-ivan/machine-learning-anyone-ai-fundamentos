import os
import pandas as pd

def load_to_parquet(df: pd.DataFrame, parquet_path: str):
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df.to_parquet(parquet_path)
    print(f"Archivo guardado en Parquet: {parquet_path}")
