"""Generate the DEV-ONLY keypair committed for laptop demos.
At work: run once, put public key in config/env, private key in the
migration-swamp secret scope, and DELETE the dev keys."""
from pathlib import Path

from migration_swamp.crypto import generate_keypair

keys = Path(__file__).resolve().parent.parent / "keys"
keys.mkdir(exist_ok=True)
private_pem, public_pem = generate_keypair()
(keys / "dev_private.pem").write_text(private_pem)
(keys / "dev_public.pem").write_text(public_pem)
print("wrote keys/dev_private.pem and keys/dev_public.pem (DEV ONLY)")
