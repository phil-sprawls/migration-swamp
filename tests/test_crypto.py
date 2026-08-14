import pytest

from migration_swamp.crypto import (
    Credentials, CryptoError, decrypt_credentials, encrypt_credentials,
    generate_keypair,
)


AAD = "request_id=r1|requester=phil"


def test_round_trip():
    priv, pub = generate_keypair()
    creds = Credentials("phil", "s3cret!pw")
    envelope = encrypt_credentials(creds, pub, AAD)
    assert decrypt_credentials(envelope, priv, AAD) == creds


def test_envelope_is_opaque_ascii():
    _, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("phil", "s3cret!pw"), pub, AAD)
    assert "phil" not in envelope and "s3cret!pw" not in envelope
    envelope.encode("ascii")  # job parameters must be plain text


def test_tampered_envelope_rejected():
    priv, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub, AAD)
    tampered = envelope[:-4] + ("AAAA" if envelope[-4:] != "AAAA" else "BBBB")
    with pytest.raises(CryptoError):
        decrypt_credentials(tampered, priv, AAD)


def test_wrong_key_rejected():
    _, pub = generate_keypair()
    other_priv, _ = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub, AAD)
    with pytest.raises(CryptoError):
        decrypt_credentials(envelope, other_priv, AAD)


def test_envelope_bound_to_aad():
    priv, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub, "request_id=A")
    with pytest.raises(CryptoError):
        decrypt_credentials(envelope, priv, "request_id=B")
