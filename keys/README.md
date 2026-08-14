# DEV-ONLY keys

`dev_private.pem` / `dev_public.pem` exist so the laptop demo and notebooks
run end-to-end. They protect nothing real. At work: generate a fresh pair,
put the public key in config or `SWAMP_PUBLIC_KEY_PEM`, put the private key
in secret scope `migration-swamp` (key `private_key_pem`), and delete this
directory.
