"""Interactive credential collection. This module is the single swap point
for the company's interactive-auth pattern during work hardening. Credentials
are returned in memory only - never widgets, never disk."""
import getpass

from migration_swamp.crypto import Credentials


def prompt_credentials(source_system: str, prompt=input,
                       secret=getpass.getpass) -> Credentials:
    username = prompt(
        f"[{source_system}] username (used only to verify your access): "
    )
    password = secret(f"[{source_system}] password (never stored): ")
    return Credentials(username, password)
