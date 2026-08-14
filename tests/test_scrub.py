from migration_swamp.scrub import scrub


def test_scrub_replaces_all_occurrences():
    out = scrub("login failed for phil with pw=hunter2 (hunter2)",
                ["hunter2", "phil"])
    assert "hunter2" not in out and "phil" not in out
    assert out.count("***") == 3


def test_scrub_ignores_empty_secrets():
    assert scrub("hello", ["", "hello"]) == "***"
