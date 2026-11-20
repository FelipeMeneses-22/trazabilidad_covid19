"""Conexión a Cassandra.

Con Python 3.12 el driver de Cassandra necesita el paquete `pyasyncore` (el módulo `asyncore`
fue eliminado de Python 3.12 y en Windows no existe la extensión libev):

    pip install pyasyncore
"""
try:
    from cassandra.cluster import Cluster
except Exception as e:  # cassandra.DependencyException
    if "connection class" in str(e):
        raise SystemExit("El driver de Cassandra no encuentra un reactor de red. "
                         "Con Python 3.12 instala:  pip install pyasyncore") from e
    raise

from .config import CASSANDRA_HOSTS


def conectar(keyspace: str | None = None):
    cluster = Cluster(CASSANDRA_HOSTS)
    return cluster.connect(keyspace) if keyspace else cluster.connect()
