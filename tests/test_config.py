from __future__ import annotations

import pytest

from collector.config import ERROR, WARNING, ConfigError, load_config, parse_sources

VALID_ENV = {
    "API_ID": "12345",
    "API_HASH": "b" * 32,
    "DESTINATION_CHAT": "@my_archive",
    "SOURCE_GROUPS": "@alpha,@beta|Beta alias",
}


def test_parse_sources_supports_alias_and_disable():
    groups = parse_sources("@alpha, @beta|Beta, -1001234|Gamma|disabled\n# comment\n")
    assert [g.identifier for g in groups] == ["@alpha", "@beta", "-1001234"]
    assert groups[1].alias == "Beta"
    assert groups[2].enabled is False


def test_parse_sources_deduplicates_case_insensitively():
    assert len(parse_sources("@Alpha, @alpha, alpha")) == 1


def test_parse_sources_empty():
    assert parse_sources(None) == []
    assert parse_sources("   ") == []


def test_load_config_valid(tmp_path):
    env = dict(VALID_ENV, DATA_DIR=str(tmp_path))
    config = load_config(env, dotenv_path=None)
    assert config.api_id == 12345
    assert config.destination_chat == "@my_archive"
    assert len(config.enabled_sources) == 2
    assert config.errors() == []
    assert config.database_path.parent == tmp_path


def test_load_config_missing_credentials_raises():
    env = {"SOURCE_GROUPS": "@alpha", "DESTINATION_CHAT": "@dest"}
    with pytest.raises(ConfigError) as excinfo:
        load_config(env, dotenv_path=None)
    assert "API_ID" in str(excinfo.value)


def test_load_config_rejects_bad_hash():
    with pytest.raises(ConfigError):
        load_config(dict(VALID_ENV, API_HASH="not-a-hash"), dotenv_path=None)


def test_destination_used_as_source_is_fatal_at_load(tmp_path):
    """Audit item 3: a detected problem must stop startup, not just be reported."""

    env = dict(VALID_ENV, DATA_DIR=str(tmp_path), SOURCE_GROUPS="@dest")
    env["DESTINATION_CHAT"] = "@dest"
    with pytest.raises(ConfigError) as excinfo:
        load_config(env, dotenv_path=None)
    assert "also configured as a source" in str(excinfo.value)


def test_problems_are_structured_not_text_matched(tmp_path):
    env = dict(VALID_ENV, DATA_DIR=str(tmp_path), SOURCE_GROUPS="@dest")
    env["DESTINATION_CHAT"] = "@dest"
    config = load_config(env, dotenv_path=None, require_credentials=False)
    problems = config.problems()
    assert problems
    assert all(p.severity in (ERROR, WARNING) for p in problems)
    loop_problem = next(p for p in problems if "also configured as a source" in p.message)
    assert loop_problem.field == "SOURCE_GROUPS"
    assert loop_problem.severity == ERROR


def test_local_only_commands_work_without_credentials(tmp_path):
    """`status` must not be blocked by missing Telegram credentials."""

    env = {"DATA_DIR": str(tmp_path)}
    config = load_config(env, dotenv_path=None, require_credentials=False)
    assert config.api_id is None
    assert config.errors(require_credentials=False)  # still reported as errors...
    # ...but loading them was not fatal, which is what matters for local commands.
    assert isinstance(config.errors(), list)


def test_validate_reports_concurrency_bounds(tmp_path):
    with pytest.raises(ConfigError):
        load_config(dict(VALID_ENV, DATA_DIR=str(tmp_path), WORKER_CONCURRENCY="9"), dotenv_path=None)


def test_dashboard_requires_credentials(tmp_path):
    env = dict(VALID_ENV, DATA_DIR=str(tmp_path), DASHBOARD_ENABLED="true")
    with pytest.raises(ConfigError) as excinfo:
        load_config(env, dotenv_path=None)
    assert "DASHBOARD_USERNAME" in str(excinfo.value)


def test_dashboard_public_bind_is_a_warning_not_an_error(tmp_path):
    env = dict(
        VALID_ENV,
        DATA_DIR=str(tmp_path),
        DASHBOARD_ENABLED="true",
        DASHBOARD_USERNAME="admin",
        DASHBOARD_PASSWORD="a-very-long-password",
        DASHBOARD_HOST="0.0.0.0",
    )
    config = load_config(env, dotenv_path=None)
    assert config.errors() == []
    assert any("binds beyond loopback" in w for w in config.warnings())


def test_invalid_boolean_is_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_config(dict(VALID_ENV, DATA_DIR=str(tmp_path), ANNOTATE_SOURCE="maybe"), dotenv_path=None)


def test_history_window_ordering(tmp_path):
    env = dict(
        VALID_ENV,
        DATA_DIR=str(tmp_path),
        HISTORY_START_DATE="2024-06-01",
        HISTORY_END_DATE="2024-01-01",
    )
    with pytest.raises(ConfigError) as excinfo:
        load_config(env, dotenv_path=None)
    assert "HISTORY_START_DATE" in str(excinfo.value)


def test_copy_mode_produces_a_warning(tmp_path):
    config = load_config(
        dict(VALID_ENV, DATA_DIR=str(tmp_path), FORWARD_MODE="copy"), dotenv_path=None
    )
    assert any("copy mode" in w for w in config.warnings())


def test_ensure_directories_restricts_permissions(tmp_path):
    env = dict(VALID_ENV, DATA_DIR=str(tmp_path / "state"))
    config = load_config(env, dotenv_path=None)
    config.ensure_directories()
    assert (config.data_dir.stat().st_mode & 0o777) == 0o700
    assert (config.log_dir.stat().st_mode & 0o777) == 0o700


def test_new_supervision_settings_are_parsed(tmp_path):
    env = dict(
        VALID_ENV,
        DATA_DIR=str(tmp_path),
        HEARTBEAT_INTERVAL="15",
        IMPORT_RECHECK_INTERVAL="120",
        RECONCILE_SCAN_LIMIT="250",
        EVENT_RETENTION_DAYS="7",
    )
    config = load_config(env, dotenv_path=None)
    assert config.heartbeat_interval == 15
    assert config.import_recheck_interval == 120
    assert config.reconcile_scan_limit == 250
    assert config.event_retention_days == 7
