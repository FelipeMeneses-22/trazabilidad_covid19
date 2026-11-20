"""Configuración central: todo se lee de variables de entorno (.env)."""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional
    pass

RAIZ = Path(__file__).resolve().parents[2]

CSV_PATH = os.getenv("CSV_PATH", str(RAIZ / "data/raw/Casos_positivos_de_COVID-19_en_Colombia.csv"))
LAKE_DIR = Path(os.getenv("LAKE_DIR", RAIZ / "data/lake"))
SILVER_DIR = LAKE_DIR / "silver/casos"
GOLD_DIR = LAKE_DIR / "gold"
GRAFO_DIR = Path(os.getenv("GRAFO_DIR", RAIZ / "data/grafo"))
RESULTADOS_DIR = Path(os.getenv("RESULTADOS_DIR", RAIZ / "resultados"))

KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "eventos_casos")

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = "trazabilidad"

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "cambiar_esta_clave")

# Subconjunto del grafo (no hacen falta los 6M de casos para demostrar trazabilidad)
DEPARTAMENTOS_GRAFO = [d.strip().upper() for d in os.getenv("DEPARTAMENTOS_GRAFO", "TOLIMA").split(",") if d.strip()]
SEMILLA = int(os.getenv("SEMILLA", "42"))
