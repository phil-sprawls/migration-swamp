import base64
import json
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CryptoError(Exception):
    pass


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str


_OAEP = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


def generate_keypair() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_pem, public_pem


def encrypt_credentials(creds: Credentials, public_pem: str, aad: str) -> str:
    public_key = serialization.load_pem_public_key(public_pem.encode())
    data_key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    plaintext = json.dumps(
        {"username": creds.username, "password": creds.password}
    ).encode()
    ciphertext = AESGCM(data_key).encrypt(nonce, plaintext, aad.encode())
    envelope = {
        "k": base64.b64encode(public_key.encrypt(data_key, _OAEP)).decode(),
        "n": base64.b64encode(nonce).decode(),
        "c": base64.b64encode(ciphertext).decode(),
    }
    return base64.b64encode(json.dumps(envelope).encode()).decode()


def decrypt_credentials(envelope: str, private_pem: str, aad: str) -> Credentials:
    try:
        private_key = serialization.load_pem_private_key(
            private_pem.encode(), password=None
        )
        outer = json.loads(base64.b64decode(envelope))
        data_key = private_key.decrypt(base64.b64decode(outer["k"]), _OAEP)
        plaintext = AESGCM(data_key).decrypt(
            base64.b64decode(outer["n"]), base64.b64decode(outer["c"]), aad.encode()
        )
        inner = json.loads(plaintext)
        return Credentials(inner["username"], inner["password"])
    except Exception as exc:  # noqa: BLE001 - single failure surface by design
        raise CryptoError("credential envelope could not be decrypted") from exc
