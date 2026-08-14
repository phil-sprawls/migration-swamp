from migration_swamp.crypto import Credentials
from migration_swamp.interactive_auth import prompt_credentials


def test_prompt_uses_secret_input_for_password():
    calls = []

    def fake_prompt(msg):
        calls.append(("prompt", msg))
        return "phil"

    def fake_secret(msg):
        calls.append(("secret", msg))
        return "pw"

    creds = prompt_credentials("oracle", prompt=fake_prompt, secret=fake_secret)
    assert creds == Credentials("phil", "pw")
    assert [kind for kind, _ in calls] == ["prompt", "secret"]
    assert "oracle" in calls[0][1]
