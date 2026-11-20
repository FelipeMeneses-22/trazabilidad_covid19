import sys
import pandas as pd

sys.path.insert(0, ".")
from src.comunes.config import CSV_PATH
from src.comunes.columnas import mapa_columnas

m = pd.read_csv(CSV_PATH, nrows=20000, dtype=str)
inv = {v: k for k, v in mapa_columnas(list(m.columns)).items()}
print("pandas", pd.__version__)
for canon in ["id_caso", "fecha_notificacion", "cod_municipio"]:
    col = inv.get(canon)
    print(f"\n{canon}  <-  {col!r}")
    print(m[col].head(3).tolist())
    print("no nulos en crudo:", m[col].notna().sum())
print("\nids numéricos:", pd.to_numeric(m[inv["id_caso"]], errors="coerce").notna().sum())
print("fechas parseadas:", pd.to_datetime(m[inv["fecha_notificacion"]], errors="coerce", format="mixed").notna().sum())