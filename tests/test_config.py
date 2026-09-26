"""Config validation (entropy_robinhood_lighter_arbitrage.config) — YAML schema, thresholds, hedge."""
from __future__ import annotations

import copy
import os

import pytest

from entropy_robinhood_lighter_arbitrage.config import (
    HEDGE_VENUES,
    ConfigError,
    load_config,
)

VALID = {
    "thresholds": {"midline_bps": 5.0, "upper_bps": 4.0, "lower_bps": 4.0},
    "entropy": {"dex": "io"},
    "hedge": {},
    "sizing": {},
    "inventory": {},
    "execution": {},
    "recorder": {},
    "logging": {},
}


@pytest.fixture(autouse=True)
def isolated_credentials(monkeypatch):
    # Tests use synthetic credentials and never consume a user's environment.
    for key in ("HL_PRIVATE_KEY", "HL_ACCOUNT_ADDRESS", "HL_PRIVATE_KEY_XYZ",
                "HL_ACCOUNT_ADDRESS_XYZ", "LIGHTER_ACCOUNT_INDEX",
                "LIGHTER_API_KEY_INDEX", "LIGHTER_API_PRIVATE_KEY"):
        monkeypatch.setenv(key, "")


@pytest.mark.parametrize("section,key,value", [
    ("execution", "net_tolerance_base", float("nan")),
    ("thresholds", "upper_bps", float("inf")),
    ("entropy", "max_position_usd", float("inf")),
    ("hedge", "taker_fee_bps", float("nan")),
])
def test_nonfinite_values_rejected(tmp_path, section, key, value):
    raw = fresh()
    raw[section][key] = value
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="finite"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")


@pytest.mark.parametrize("section,key,value", [
    ("execution", "settle_timeout_sec", 0),
    ("execution", "leg_slippage_bps", -1),
    ("execution", "hedge_slippage_bps", 10000),
    ("execution", "max_consecutive_errors", 0),
    ("entropy", "max_orders_per_min", 0),
    ("hedge", "max_position_usd", -1),
    ("inventory", "floor_frac", 1),
    ("risk", "net_tolerance_usd", -1),
    ("logging", "max_bytes", 0),
])
def test_invalid_ranges_rejected(tmp_path, section, key, value):
    raw = fresh()
    raw.setdefault(section, {})[key] = value
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match=key):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")


def test_sizing_cross_constraints(tmp_path):
    path = write_yaml(str(tmp_path), fresh(sizing={"min_order_notional_usd": 600}))
    with pytest.raises(ConfigError, match="min_order_notional"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")


def test_pair_paths_and_safe_identity(tmp_path):
    path = write_yaml(str(tmp_path), VALID)
    cfg = load_config(path, "/nonexistent.env", symbol="../SNDK", hedge_venue="lighter-rh")
    other = load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")
    assert cfg.pair_id != other.pair_id
    assert "/" not in cfg.pair_id and ".." not in cfg.pair_id
    assert cfg.pair_id in cfg.state_db
    assert cfg.pair_id in cfg.recorder_csv
    assert cfg.pair_id in cfg.health_file


def test_unknown_path_template_rejected(tmp_path):
    path = write_yaml(str(tmp_path), fresh(recorder={"csv": "logs/{symbol}/minutes.csv"}))
    with pytest.raises(ConfigError, match="template"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")


@pytest.mark.parametrize("sidecar", ["", ".lock", "-wal", "-shm"])
def test_health_cannot_replace_journal_or_sidecars(tmp_path, sidecar):
    state = tmp_path / "orders.sqlite3"
    raw = fresh()
    raw["runtime"] = {"state_db": str(state), "health_file": str(state) + sidecar}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_collision_check_resolves_symlinks(tmp_path):
    state = tmp_path / "orders.sqlite3"
    alias = tmp_path / "alias.json"
    alias.symlink_to(state)
    raw = fresh()
    raw["runtime"] = {"state_db": str(state), "health_file": str(alias)}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_health_temporary_file_cannot_truncate_journal(tmp_path):
    raw = fresh()
    raw["runtime"] = {"health_file": str(tmp_path / "health.json"),
                      "state_db": str(tmp_path / "health.json.tmp")}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_rotating_log_cannot_overwrite_journal(tmp_path):
    raw = fresh()
    raw["logging"]["file"] = str(tmp_path / "engine.log")
    raw["runtime"] = {"state_db": str(tmp_path / "engine.log.1")}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_health_cannot_overwrite_daily_recording(tmp_path):
    raw = fresh(recorder={"csv": str(tmp_path / "minutes.csv")})
    raw["runtime"] = {"health_file": str(tmp_path / "minutes-2026-09-26.csv")}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


@pytest.mark.parametrize("target", ["state_db", "health_file"])
def test_daily_trade_log_cannot_overwrite_runtime_files(tmp_path, target):
    raw = fresh(logging={"trades_csv": str(tmp_path / "trades.csv")})
    raw["runtime"] = {target: str(tmp_path / "trades-2026-09-26.csv")}
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_daily_recording_and_trade_file_names_cannot_overlap(tmp_path):
    raw = fresh(recorder={"csv": str(tmp_path / "minutes")},
                logging={"trades_csv": str(tmp_path / "minutes.csv")})
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="path.*collision"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


@pytest.mark.parametrize("section,limit", [("entropy", 1), ("hedge", 2)])
def test_send_budget_includes_opening_and_recovery_reserve(tmp_path, section, limit):
    raw = fresh()
    raw[section]["max_orders_per_min"] = limit
    path = write_yaml(str(tmp_path), raw)
    with pytest.raises(ConfigError, match="max_orders_per_min.*3"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                    read_credentials=False)


def test_outputs_can_share_directory_with_distinct_names(tmp_path):
    raw = fresh(recorder={"csv": str(tmp_path / "minutes.csv")},
                logging={"file": str(tmp_path / "engine.log"),
                         "trades_csv": str(tmp_path / "trades.csv")})
    raw["runtime"] = {"state_db": str(tmp_path / "orders.sqlite3"),
                      "health_file": str(tmp_path / "health.json")}
    path = write_yaml(str(tmp_path), raw)
    cfg = load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh",
                      read_credentials=False)
    assert cfg.state_db == str(tmp_path / "orders.sqlite3")


def test_bad_credential_index_does_not_echo_value(tmp_path, monkeypatch):
    marker = "synthetic-secret-string"
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", marker)
    path = write_yaml(str(tmp_path), VALID)
    with pytest.raises(ConfigError) as caught:
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="lighter-rh")
    assert marker not in str(caught.value)
    assert "LIGHTER_ACCOUNT_INDEX" in str(caught.value)


def test_offline_load_does_not_read_credentials(tmp_path, monkeypatch):
    from unittest.mock import patch
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "synthetic-invalid-secret")
    path = write_yaml(str(tmp_path), VALID)
    with patch("entropy_robinhood_lighter_arbitrage.config.load_dotenv") as dotenv, \
         patch("entropy_robinhood_lighter_arbitrage.config._env_s") as read_string, \
         patch("entropy_robinhood_lighter_arbitrage.config._env_i") as read_int:
        cfg = load_config(path, "/must-not-open.env", symbol="SNDK",
                          hedge_venue="lighter-rh", read_credentials=False)
    assert not cfg.creds_complete
    dotenv.assert_not_called()
    read_string.assert_not_called()
    read_int.assert_not_called()


def test_example_uses_venue_fee_and_persistence_defaults():
    cfg = load_config("config.example.yaml", "/nonexistent.env", symbol="SNDK", hedge_venue="tradexyz")
    assert cfg.hedge.fee_bps == 1.0
    assert cfg.premium_persist_sec == 0.3
    assert cfg.leg_slippage_bps == 1.0


def write_yaml(tmp: str, data: dict) -> str:
    import yaml
    path = os.path.join(tmp, "config.yaml")
    with open(path, "w") as fh:
        yaml.safe_dump(data, fh)
    return path


def fresh(**overrides) -> dict:
    """Deep copy of VALID so tests never mutate the shared fixture."""
    d = copy.deepcopy(VALID)
    for section, values in overrides.items():
        d[section] = values
    return d


def test_valid_config_lighter_rh(tmp_path) -> None:
    path = write_yaml(str(tmp_path), VALID)
    cfg = load_config(path, "/nonexistent.env", symbol="SNDK",
                      hedge_venue="lighter-rh")
    assert cfg.symbol == "SNDK"
    assert cfg.hedge_venue == "lighter-rh"
    assert cfg.entropy.kind == "hl" and cfg.entropy.hl_dex == "io"
    assert cfg.hedge.kind == "lighter"
    assert cfg.hedge.label == "RH"
    assert cfg.hedge.lighter_profile.name == "robinhood"
    assert cfg.midline_bps == 5.0
    assert cfg.upper_bps == 4.0 and cfg.lower_bps == 4.0
    assert cfg.creds_complete is False  # no keys in env


def test_valid_config_lighter_mainnet(tmp_path) -> None:
    path = write_yaml(str(tmp_path), VALID)
    cfg = load_config(path, "/nonexistent.env", symbol="SNDK",
                      hedge_venue="lighter")
    assert cfg.hedge.label == "LIGHTER"
    assert cfg.hedge.lighter_profile.name == "mainnet"
    assert cfg.hedge.lighter_profile.chain_id == 304


def test_valid_config_tradexyz(tmp_path) -> None:
    path = write_yaml(str(tmp_path), VALID)
    cfg = load_config(path, "/nonexistent.env", symbol="SNDK",
                      hedge_venue="tradexyz")
    assert cfg.hedge.kind == "hl"
    assert cfg.hedge.hl_dex == "xyz"
    assert cfg.hedge.fee_bps == 1.0  # default for tradexyz


def test_unknown_key_rejected(tmp_path) -> None:
    bad = fresh(sizing={"take_fraction": 0.5, "bogus": 1})
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="unknown config key 'sizing.bogus'"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="lighter-rh")


def test_missing_thresholds_rejected(tmp_path) -> None:
    bad = fresh()
    del bad["thresholds"]["upper_bps"]
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="thresholds.upper_bps"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="lighter-rh")


def test_nonpositive_band_rejected(tmp_path) -> None:
    bad = fresh(thresholds={"midline_bps": 0.0, "upper_bps": -1.0,
                            "lower_bps": 4.0})
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="must be > 0"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="lighter-rh")


def test_take_fraction_bounds(tmp_path) -> None:
    bad = fresh(sizing={"take_fraction": 0})
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="take_fraction"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="lighter-rh")


def test_bad_take_fraction_type(tmp_path) -> None:
    bad = fresh(sizing={"take_fraction": "half"})
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="must be a number"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="lighter-rh")


def test_bad_hedge_rejected(tmp_path) -> None:
    path = write_yaml(str(tmp_path), VALID)
    with pytest.raises(ConfigError, match="--hedge must be one of"):
        load_config(path, "/nonexistent.env", symbol="SNDK", hedge_venue="bogus")


def test_missing_config_file_rejected() -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/config.yaml", "/nonexistent.env",
                    symbol="SNDK", hedge_venue="lighter-rh")


def test_same_market_tradexyz_rejected(tmp_path) -> None:
    bad = fresh(entropy={"dex": "xyz"})
    path = write_yaml(str(tmp_path), bad)
    with pytest.raises(ConfigError, match="same market"):
        load_config(path, "/nonexistent.env", symbol="SNDK",
                    hedge_venue="tradexyz")


def test_creds_complete_with_env(tmp_path, monkeypatch) -> None:
    path = write_yaml(str(tmp_path), VALID)
    monkeypatch.setenv("HL_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("HL_ACCOUNT_ADDRESS", "0xabc")
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_PRIVATE_KEY", "k" * 64)
    # load_config() calls load_dotenv() which never overrides existing env,
    # so the monkeypatched values win.
    cfg = load_config(path, "/nonexistent.env", symbol="SNDK",
                      hedge_venue="lighter-rh")
    assert cfg.entropy.hl_creds.complete is True
    assert cfg.hedge.lighter_creds.complete is True
    assert cfg.creds_complete is True


def test_hedge_venues_constant() -> None:
    assert HEDGE_VENUES == ("lighter", "lighter-rh", "tradexyz")
