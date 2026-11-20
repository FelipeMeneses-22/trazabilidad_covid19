"""Limpieza con pandas (la usan el productor Kafka y las pruebas).
Spark aplica las mismas reglas en src/procesamiento/limpieza.py."""
import pandas as pd

from .columnas import CANONICAS, FECHAS, validar

TEXTO_VACIO = {"", "nan", "none", "n/a", "-", "-   -", "sin dato", "nat"}


def _texto(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s.str.lower().isin(TEXTO_VACIO))


def _numerico(s: pd.Series) -> pd.Series:
    """Quita separadores de miles y espacios ('1,556,979' -> '1556979', '76,001' -> '76001')."""
    return _texto(s).str.replace(r"[,\s]", "", regex=True).str.replace(r"\.0$", "", regex=True)


def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    mapa, faltantes, _ = validar(list(df.columns))
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas {faltantes}. Ejecuta scripts/inspeccionar_csv.py")
    df = df[list(mapa)].rename(columns=mapa)
    for c in CANONICAS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[CANONICAS].copy()

    for c in FECHAS:  # el portal exporta MM/DD/YYYY hh:mm:ss AM; la API, ISO 8601
        df[c] = pd.to_datetime(_texto(df[c]), errors="coerce", format="mixed").dt.normalize()

    df["id_caso"] = pd.to_numeric(_numerico(df["id_caso"]), errors="coerce").astype("Int64")
    df["edad"] = pd.to_numeric(_numerico(df["edad"]), errors="coerce").astype("Int64")
    df["cod_departamento"] = _numerico(df["cod_departamento"]).str.zfill(2)
    df["cod_municipio"] = _numerico(df["cod_municipio"]).str.zfill(5)
    for c in ["nombre_departamento", "nombre_municipio"]:
        df[c] = _texto(df[c]).str.upper()
    for c in ["sexo", "tipo_contagio", "estado", "recuperado", "ubicacion_caso", "tipo_recuperacion"]:
        df[c] = _texto(df[c]).str.lower()
    df["sexo"] = df["sexo"].str.upper()

    return df.dropna(subset=["id_caso", "fecha_notificacion", "cod_municipio"])
