import pytest

from migration_swamp.crypto import (
    Credentials, CryptoError, decrypt_credentials, encrypt_credentials,
    generate_keypair,
)


def test_round_trip():
    priv, pub = generate_keypair()
    creds = Credentials("phil", "s3cret!pw")
    envelope = encrypt_credentials(creds, pub)
    assert decrypt_credentials(envelope, priv) == creds


def test_envelope_is_opaque_ascii():
    _, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("phil", "s3cret!pw"), pub)
    assert "phil" not in envelope and "s3cret!pw" not in envelope
    envelope.encode("ascii")  # job parameters must be plain text


def test_tampered_envelope_rejected():
    priv, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub)
    tampered = envelope[:-4] + ("AAAA" if envelope[-4:] != "AAAA" else "BBBB")
    with pytest.raises(CryptoError):
        decrypt_credentials(tampered, priv)


def test_wrong_key_rejected():
    _, pub = generate_keypair()
    other_priv, _ = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub)
    with pytest.raises(CryptoError):
        decrypt_credentials(envelope, other_priv)
