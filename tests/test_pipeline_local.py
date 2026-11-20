import numpy as np
import pandas as pd

from src.comunes.columnas import mapa_columnas, validar
from src.comunes.transformar import normalizar
from src.grafo.generar_contactos import generar_aristas

ENCABEZADOS_PORTAL = ["fecha reporte web", "ID de caso", "Fecha de notificación", "Código DIVIPOLA departamento",
                      "Nombre departamento", "Código DIVIPOLA municipio", "Nombre municipio", "Edad"]
ENCABEZADOS_API = ["fecha_reporte_web", "id_de_caso", "fecha_de_notificaci_n", "departamento", "departamento_nom",
                   "ciudad_municipio", "ciudad_municipio_nom", "edad"]


def test_mapea_encabezados_del_portal_y_de_la_api():
    for cols in (ENCABEZADOS_PORTAL, ENCABEZADOS_API):
        _, faltantes, _ = validar(cols)
        assert faltantes == []
    assert set(mapa_columnas(ENCABEZADOS_PORTAL).values()) == set(mapa_columnas(ENCABEZADOS_API).values())


def test_normalizar_acepta_fechas_iso_y_formato_portal():
    df = pd.DataFrame({
        "ID de caso": ["1", "2", "x"],
        "Fecha de notificación": ["2020-03-06T00:00:00.000", "03/06/2020 12:00:00 AM", "2020-03-07T00:00:00.000"],
        "Código DIVIPOLA departamento": ["73", "73", "73"], "Nombre departamento": ["Tolima"] * 3,
        "Código DIVIPOLA municipio": ["73001", "73001", "73001"], "Nombre municipio": ["Ibagué"] * 3,
        "Fecha de muerte": ["-   -", "", "2020-04-01T00:00:00.000"],
    })
    out = normalizar(df)
    assert len(out) == 2                      # 'x' no es un id válido
    assert out["fecha_notificacion"].nunique() == 1
    assert out["fecha_muerte"].isna().all()
    assert out["nombre_departamento"].iloc[0] == "TOLIMA"


def _casos(n=3000, semilla=0):
    rng = np.random.default_rng(semilla)
    return pd.DataFrame({
        "id_caso": np.arange(1, n + 1),
        "cod_municipio": rng.choice(["73001", "73449"], n),
        "fecha_notificacion": pd.Timestamp("2020-04-01") + pd.to_timedelta(rng.integers(0, 120, n), unit="D"),
        "tipo_contagio": rng.choice(["relacionado", "comunitaria", "importado", "en estudio"], n),
    })


def test_contactos_es_bosque_y_respeta_reglas():
    df = _casos()
    a = generar_aristas(df, semilla=42)
    assert len(a) > 0
    assert a["destino_id"].is_unique                       # un solo contagiador por caso
    assert (a["dias"] >= 0).all() and (a["dias"] <= 14).all()
    assert (a["origen_id"] != a["destino_id"]).all()
    muni = df.set_index("id_caso")["cod_municipio"]
    assert (muni[a["origen_id"]].to_numpy() == muni[a["destino_id"]].to_numpy()).all()
    importados = set(df.loc[df["tipo_contagio"] == "importado", "id_caso"])
    assert importados.isdisjoint(set(a["destino_id"]))     # importados nunca tienen contagiador
    fecha = df.set_index("id_caso")["fecha_notificacion"]
    assert (fecha[a["origen_id"]].to_numpy() <= fecha[a["destino_id"]].to_numpy()).all()


def test_contactos_es_reproducible():
    df = _casos()
    pd.testing.assert_frame_equal(generar_aristas(df, 7), generar_aristas(df, 7))
    assert not generar_aristas(df, 7).equals(generar_aristas(df, 8))
