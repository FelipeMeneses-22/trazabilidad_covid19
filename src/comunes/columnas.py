"""Normalización de nombres de columnas.

El CSV descargado del portal usa encabezados "de pantalla" ("ID de caso",
"Código DIVIPOLA municipio") y la API Socrata usa nombres cortos
("id_de_caso", "ciudad_municipio"). Se aceptan ambos y se mapean a un
nombre canónico en snake_case.
"""
import re
import unicodedata


def slug(texto: str) -> str:
    s = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


# slug del encabezado original -> nombre canónico
ALIAS = {
    "id_de_caso": "id_caso",
    "fecha_reporte_web": "fecha_reporte",
    "fecha_de_notificacion": "fecha_notificacion",
    "fecha_de_notificaci_n": "fecha_notificacion",
    "fecha_de_inicio_de_sintomas": "fecha_inicio_sintomas",
    "fecha_inicio_sintomas": "fecha_inicio_sintomas",
    "fecha_de_diagnostico": "fecha_diagnostico",
    "fecha_diagnostico": "fecha_diagnostico",
    "fecha_de_muerte": "fecha_muerte",
    "fecha_muerte": "fecha_muerte",
    "fecha_de_recuperacion": "fecha_recuperacion",
    "fecha_recuperado": "fecha_recuperacion",
    "codigo_divipola_departamento": "cod_departamento",
    "departamento": "cod_departamento",           # API: 'departamento' es el código
    "nombre_departamento": "nombre_departamento",
    "departamento_nom": "nombre_departamento",
    "codigo_divipola_municipio": "cod_municipio",
    "ciudad_municipio": "cod_municipio",          # API: 'ciudad_municipio' es el código
    "nombre_municipio": "nombre_municipio",
    "ciudad_municipio_nom": "nombre_municipio",
    "edad": "edad",
    "unidad_de_medida_de_edad": "unidad_edad",
    "unidad_medida": "unidad_edad",
    "sexo": "sexo",
    "tipo_de_contagio": "tipo_contagio",
    "fuente_tipo_contagio": "tipo_contagio",
    "ubicacion_del_caso": "ubicacion_caso",
    "ubicacion": "ubicacion_caso",
    "estado": "estado",
    "recuperado": "recuperado",
    "tipo_de_recuperacion": "tipo_recuperacion",
    "tipo_recuperacion": "tipo_recuperacion",
    "codigo_iso_del_pais": "cod_pais",
    "pais_viajo_1_cod": "cod_pais",
    "nombre_del_pais": "nombre_pais",
    "pais_viajo_1_nom": "nombre_pais",
    "pertenencia_etnica": "pertenencia_etnica",
    "per_etn": "pertenencia_etnica",
    "nombre_del_grupo_etnico": "grupo_etnico",
    "nom_grupo": "grupo_etnico",
}

FECHAS = ["fecha_reporte", "fecha_notificacion", "fecha_inicio_sintomas",
          "fecha_diagnostico", "fecha_muerte", "fecha_recuperacion"]
REQUERIDAS = ["id_caso", "fecha_notificacion", "cod_departamento", "nombre_departamento",
              "cod_municipio", "nombre_municipio"]
CANONICAS = ["id_caso", "fecha_reporte", "fecha_notificacion", "cod_departamento",
             "nombre_departamento", "cod_municipio", "nombre_municipio", "edad", "unidad_edad",
             "sexo", "tipo_contagio", "ubicacion_caso", "estado", "recuperado",
             "fecha_inicio_sintomas", "fecha_diagnostico", "fecha_muerte", "fecha_recuperacion",
             "tipo_recuperacion", "cod_pais", "nombre_pais", "pertenencia_etnica", "grupo_etnico"]


def mapa_columnas(columnas):
    """{encabezado original: nombre canónico}; si dos columnas colisionan gana la primera."""
    mapa, usados = {}, set()
    for original in columnas:
        canon = ALIAS.get(slug(original))
        if canon and canon not in usados:
            mapa[original] = canon
            usados.add(canon)
    return mapa


def validar(columnas):
    """Devuelve (mapa, faltantes_requeridas, no_reconocidas)."""
    mapa = mapa_columnas(columnas)
    faltantes = [c for c in REQUERIDAS if c not in mapa.values()]
    no_reconocidas = [c for c in columnas if c not in mapa]
    return mapa, faltantes, no_reconocidas
