"""Crea un par de llaves RSA para conectarte a Snowflake sin contraseña."""
import base64
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

destino = Path.home() / ".snowflake" / "rsa_key.p8"
if destino.exists() and "--forzar" not in sys.argv:
    sys.exit(f"Ya existe {destino}. Para crear una nueva: python scripts/generar_llave_snowflake.py --forzar")

llave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
destino.parent.mkdir(parents=True, exist_ok=True)
destino.write_bytes(llave.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                        serialization.NoEncryption()))
try:
    destino.chmod(0o600)
except OSError:
    pass

publica = base64.b64encode(llave.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)).decode()

print(f"Llave privada guardada en: {destino}\n")
print("1) En tu archivo .env, la línea de la llave debe ser:\n")
print(f"SNOWFLAKE_PRIVATE_KEY_FILE={destino.as_posix()}\n")
print("2) En Snowsight (hoja SQL, rol ACCOUNTADMIN) ejecuta estas dos líneas:\n")
print("SET u = CURRENT_USER();")
print(f"ALTER USER IDENTIFIER($u) SET RSA_PUBLIC_KEY='{publica}';")