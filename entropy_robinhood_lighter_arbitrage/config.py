"""Configuration: strategy from a YAML file, credentials from .env, market
selection (symbol + hedge venue) from the command line.

The split is deliberate: config.yaml IS the strategy (thresholds, sizing,
risk) and is safe to share/commit as an example; .env holds only secrets;
which markets to trade is stated explicitly on every start (--symbol,
--hedge). Every YAML key is validated against the schema below, so a typo
is an error rather than a setting that silently does nothing.

Threshold model (fixed numbers the user derives from recorded minute data):

    premium_bps = (entropy_price / hedge_price - 1) * 10_000

    SELL entropy / BUY hedge  fires when the executable premium
        (entropy bid over hedge ask) >= midline_bps + upper_bps
    BUY entropy / SELL hedge  fires when the executable premium
        (entropy ask under hedge bid) <= midline_bps - lower_bps

    Hurdles include configured taker fees. Actual returns still depend on
    fills, price movement, funding and the hedge/recovery path.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from string import Formatter
from typing import Any

import yaml
from dotenv import load_dotenv

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz")


@dataclass(frozen=True)
class LighterProfile:
    name: str
    api_url: str
    ws_url: str
    chain_id: int


# Endpoint profiles for the two supported zkLighter deployments (these match
# lighter-python's lighter.endpoint_profiles, duplicated here so --record-only
# data collection works without the SDK installed).
LIGHTER_PROFILES: dict[str, LighterProfile] = {
    "lighter": LighterProfile(
        "mainnet", "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream", 304),
    "lighter-rh": LighterProfile(
        "robinhood", "https://api.rh.lighter.xyz",
        "wss://api.rh.lighter.xyz/stream", 466324),
}


@dataclass
class LighterCreds:
    account_index: int | None
    api_key_index: int | None
    api_private_key: str | None = field(repr=False)

    @property
    def complete(self) -> bool:
        return (self.account_index is not None and self.api_key_index is not None
                and bool(self.api_private_key))


@dataclass
class HLCreds:
    private_key: str | None = field(repr=False)
    account_address: str | None

    @property
    def complete(self) -> bool:
        return bool(self.private_key)


@dataclass
class VenueConf:
    key: str                  # "entropy" | "hedge"
    kind: str                 # "hl" | "lighter"
    label: str                # human name for logs, e.g. "ENTROPY", "RH"
    symbol: str
    fee_bps: float
    cap_usd: float
    orders_per_min: int
    # hl
    hl_dex: str = ""
    hl_creds: HLCreds | None = None
    # lighter
    lighter_profile: LighterProfile | None = None
    lighter_creds: LighterCreds | None = None


@dataclass
class Config:
    symbol: str
    hedge_venue: str
    entropy: VenueConf
    hedge: VenueConf
    # thresholds (the whole signal)
    midline_bps: float
    upper_bps: float
    lower_bps: float
    # sizing
    take_fraction: float
    max_order_notional: float
    min_order_notional: float
    # inventory ladder
    inventory_scale_bps: float
    inventory_floor_frac: float
    # execution
    premium_persist_sec: float
    cooldown_sec: float
    settle_timeout_sec: float
    leg_slippage_bps: float
    hedge_slippage_bps: float
    net_tolerance_base: float
    max_consecutive_errors: int
    rate_limit_pause_sec: float
    staleness_sec: float
    reconcile_sec: float
    venue_probe_sec: float
    http_keepalive_sec: float
    # recorder
    recorder_enabled: bool
    recorder_csv: str
    # logging
    log_level: str
    status_interval_sec: float
    trades_csv: str
    dashboard: bool
    log_file: str
    # runtime
    hl_api_url: str = HL_API_URL
    hl_ws_url: str = HL_WS_URL
    submit_timeout_sec: float = 5.0
    recovery_poll_sec: float = 2.0
    recovery_timeout_sec: float = 120.0
    shutdown_timeout_sec: float = 30.0
    reconcile_grace_sec: float = 5.0
    net_tolerance_usd: float = 1.0
    max_dust_usd: float = 10.0
    min_free_collateral_usd: float = 25.0
    balance_staleness_sec: float = 90.0
    position_staleness_sec: float = 60.0
    max_session_loss_usd: float = 50.0
    state_db: str = "state/{pair}/orders.sqlite3"
    health_file: str = "logs/{pair}/health.json"
    log_max_bytes: int = 10485760
    log_backup_count: int = 5
    recorder_rotate_daily: bool = True
    recorder_max_pending_rows: int = 1440
    min_disk_free_mb: float = 256.0

    @property
    def pair_id(self) -> str:
        return market_pair_id(self.symbol, self.entropy.hl_dex, self.hedge_venue)

    @property
    def creds_complete(self) -> bool:
        for v in (self.entropy, self.hedge):
            if v.kind == "hl" and not (v.hl_creds and v.hl_creds.complete):
                return False
            if v.kind == "lighter" and not (v.lighter_creds
                                            and v.lighter_creds.complete):
                return False
        return True


# ----------------------------------------------------------------- YAML layer

# Schema: nested dict of key -> type (or nested dict). Unknown keys are errors.
_SCHEMA: dict[str, Any] = {
    "thresholds": {
        "midline_bps": float,
        "upper_bps": float,
        "lower_bps": float,
    },
    "entropy": {
        "dex": str,
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "hedge": {
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "sizing": {
        "take_fraction": float,
        "max_order_notional_usd": float,
        "min_order_notional_usd": float,
    },
    "inventory": {
        "scale_bps": float,
        "floor_frac": float,
    },
    "execution": {
        "premium_persist_sec": float,
        "cooldown_sec": float,
        "settle_timeout_sec": float,
        "leg_slippage_bps": float,
        "hedge_slippage_bps": float,
        "net_tolerance_base": float,
        "max_consecutive_errors": int,
        "rate_limit_pause_sec": float,
        "staleness_sec": float,
        "reconcile_sec": float,
        "venue_probe_sec": float,
        "http_keepalive_sec": float,
        "submit_timeout_sec": float,
        "recovery_poll_sec": float,
        "recovery_timeout_sec": float,
        "shutdown_timeout_sec": float,
        "reconcile_grace_sec": float,
    },
    "risk": {
        "net_tolerance_usd": float,
        "max_dust_usd": float,
        "min_free_collateral_usd": float,
        "balance_staleness_sec": float,
        "position_staleness_sec": float,
        "max_session_loss_usd": float,
    },
    "runtime": {
        "state_db": str,
        "health_file": str,
        "min_disk_free_mb": float,
    },
    "recorder": {
        "enabled": bool,
        "csv": str,
        "rotate_daily": bool,
        "max_pending_rows": int,
    },
    "logging": {
        "level": str,
        "status_interval_sec": float,
        "trades_csv": str,
        "dashboard": bool,
        "file": str,
        "max_bytes": int,
        "backup_count": int,
    },
}


class ConfigError(ValueError):
    pass


def _validate(node: Any, schema: dict[str, Any], path: str = "") -> None:
    if not isinstance(node, dict):
        raise ConfigError(f"'{path or '<root>'}' must be a mapping")
    for key, val in node.items():
        here = f"{path}.{key}" if path else str(key)
        if key not in schema:
            raise ConfigError(f"unknown config key '{here}' "
                              f"(valid: {', '.join(sorted(schema))})")
        want = schema[key]
        if isinstance(want, dict):
            _validate(val, want, here)
        elif want is float:
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be a number")
            try:
                finite = math.isfinite(val)
            except OverflowError:
                finite = False
            if not finite:
                raise ConfigError(f"'{here}' must be finite")
        elif want is int:
            if not isinstance(val, int) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be an integer")
        elif want is bool:
            if not isinstance(val, bool):
                raise ConfigError(f"'{here}' must be true/false")
        elif want is str and not isinstance(val, str):
            raise ConfigError(f"'{here}' must be a string")


def _get(d: dict, section: str, key: str, default):
    return (d.get(section) or {}).get(key, default)


# ------------------------------------------------------------------ env layer

def _env_s(name: str) -> str | None:
    v = os.getenv(name)
    return (v.strip() or None) if v is not None else None


def _env_i(name: str) -> int | None:
    v = _env_s(name)
    if v is None:
        return None
    try:
        index = int(v)
    except (ValueError, OverflowError):
        raise ConfigError(f"credential {name} must be a nonnegative integer") from None
    if index < 0:
        raise ConfigError(f"credential {name} must be a nonnegative integer")
    return index


def market_pair_id(symbol: str, entropy_dex: str, hedge_venue: str) -> str:
    """Readable filesystem-safe identity; hash prevents sanitized-name collisions."""
    identity = (symbol, entropy_dex, hedge_venue)
    slug = "-".join(re.sub(r"[^a-z0-9_-]+", "-", part.lower()).strip("-_")[:48]
                    or "market" for part in identity)
    digest = hashlib.sha256(json.dumps(identity, ensure_ascii=True).encode()).hexdigest()[:10]
    return f"{slug}-{digest}"


def _path_template(value: str, pair_id: str, key: str) -> str:
    if not value.strip() or "\x00" in value:
        raise ConfigError(f"'{key}' must be a nonempty path")
    try:
        for _, field, spec, conversion in Formatter().parse(value):
            if field is not None and (field != "pair" or spec or conversion):
                raise ValueError
        return value.format(pair=pair_id)
    except (ValueError, KeyError, IndexError):
        raise ConfigError(f"'{key}' path template only supports {{pair}}") from None


def _validate_runtime(cfg: Config) -> None:
    # Validate effective defaults as well as user values before any signing or I/O.
    positive = {
        "sizing.max_order_notional_usd": cfg.max_order_notional,
        "sizing.min_order_notional_usd": cfg.min_order_notional,
        "execution.settle_timeout_sec": cfg.settle_timeout_sec,
        "execution.submit_timeout_sec": cfg.submit_timeout_sec,
        "execution.recovery_poll_sec": cfg.recovery_poll_sec,
        "execution.recovery_timeout_sec": cfg.recovery_timeout_sec,
        "execution.shutdown_timeout_sec": cfg.shutdown_timeout_sec,
        "execution.max_consecutive_errors": cfg.max_consecutive_errors,
        "execution.rate_limit_pause_sec": cfg.rate_limit_pause_sec,
        "execution.staleness_sec": cfg.staleness_sec,
        "execution.reconcile_sec": cfg.reconcile_sec,
        "execution.venue_probe_sec": cfg.venue_probe_sec,
        "risk.balance_staleness_sec": cfg.balance_staleness_sec,
        "risk.position_staleness_sec": cfg.position_staleness_sec,
        "risk.max_session_loss_usd": cfg.max_session_loss_usd,
        "logging.status_interval_sec": cfg.status_interval_sec,
        "logging.max_bytes": cfg.log_max_bytes,
        "logging.backup_count": cfg.log_backup_count,
        "recorder.max_pending_rows": cfg.recorder_max_pending_rows,
    }
    nonnegative = {
        "inventory.scale_bps": cfg.inventory_scale_bps,
        "execution.premium_persist_sec": cfg.premium_persist_sec,
        "execution.cooldown_sec": cfg.cooldown_sec,
        "execution.net_tolerance_base": cfg.net_tolerance_base,
        "execution.reconcile_grace_sec": cfg.reconcile_grace_sec,
        "execution.http_keepalive_sec": cfg.http_keepalive_sec,
        "risk.net_tolerance_usd": cfg.net_tolerance_usd,
        "risk.max_dust_usd": cfg.max_dust_usd,
        "risk.min_free_collateral_usd": cfg.min_free_collateral_usd,
        "runtime.min_disk_free_mb": cfg.min_disk_free_mb,
    }
    for venue in (cfg.entropy, cfg.hedge):
        positive[f"{venue.key}.max_position_usd"] = venue.cap_usd
        if venue.orders_per_min < 3:
            raise ConfigError(f"'{venue.key}.max_orders_per_min' must be >= 3 "
                              "(one opening order plus two recovery slots)")
        if not 0 <= venue.fee_bps < 10000:
            raise ConfigError(f"'{venue.key}.taker_fee_bps' must be in [0, 10000)")
    for key, value in positive.items():
        if value <= 0:
            raise ConfigError(f"'{key}' must be > 0")
    for key, value in nonnegative.items():
        if value < 0:
            raise ConfigError(f"'{key}' must be >= 0")
    for key, value in (("leg_slippage_bps", cfg.leg_slippage_bps),
                       ("hedge_slippage_bps", cfg.hedge_slippage_bps)):
        if not 0 <= value < 10000:
            raise ConfigError(f"'execution.{key}' must be in [0, 10000)")
    if not 0 <= cfg.inventory_floor_frac < 1:
        raise ConfigError("'inventory.floor_frac' must be in [0, 1)")
    if cfg.min_order_notional > cfg.max_order_notional:
        raise ConfigError("sizing.min_order_notional_usd must not exceed max_order_notional_usd")
    if cfg.recovery_poll_sec > cfg.recovery_timeout_sec:
        raise ConfigError("execution.recovery_poll_sec must not exceed recovery_timeout_sec")
    if cfg.submit_timeout_sec > cfg.shutdown_timeout_sec:
        raise ConfigError("execution.shutdown_timeout_sec must cover submit_timeout_sec")
    if cfg.net_tolerance_usd > cfg.max_dust_usd:
        raise ConfigError("risk.net_tolerance_usd must not exceed max_dust_usd")
    if cfg.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise ConfigError("logging.level must be DEBUG, INFO, WARNING, ERROR or CRITICAL")


def _validate_artifact_paths(cfg: Config, config_file: str, env_file: str) -> None:
    """Independent writers must never overwrite an input, journal, or sidecar."""
    keys = ("recorder_csv", "trades_csv", "log_file", "state_db", "health_file")
    paths = {key: Path(getattr(cfg, key)).expanduser().resolve() for key in keys}
    health_path = Path(cfg.health_file).expanduser()
    paths["health_temp"] = health_path.with_name(health_path.name + ".tmp").resolve()
    if len(set(paths.values())) != len(paths):
        raise ConfigError("artifact path collision: each output must use a distinct file")
    protected = {Path(config_file).expanduser().resolve(), Path(env_file).expanduser().resolve()}
    if any(path in protected for path in paths.values()):
        raise ConfigError("artifact path collision: outputs cannot overwrite config or credentials")
    state = paths["state_db"]
    sidecars = {Path(str(state) + suffix).resolve() for suffix in (".lock", "-wal", "-shm")}
    if any(path in sidecars for key, path in paths.items() if key != "state_db"):
        raise ConfigError("artifact path collision: journal sidecar files are reserved")
    log_path = paths["log_file"]
    for key, path in paths.items():
        if key == "log_file" or path.parent != log_path.parent:
            continue
        if path.name.startswith(log_path.name + "."):
            suffix = path.name[len(log_path.name) + 1:]
            if re.fullmatch(r"[1-9][0-9]*", suffix) and \
                    len(suffix) <= len(str(cfg.log_backup_count)) and \
                    int(suffix) <= cfg.log_backup_count:
                raise ConfigError("artifact path collision: rotating log backup files are reserved")
    daily_series = [("trades_csv", Path(cfg.trades_csv).suffix)]
    if cfg.recorder_rotate_daily:
        daily_series.append(("recorder_csv", Path(cfg.recorder_csv).suffix or ".csv"))
    daily_targets = set()
    for csv_key, extension in daily_series:
        # Rotation derives the filename before resolving its final component.
        csv_path = Path(getattr(cfg, csv_key)).expanduser()
        parent = csv_path.parent.resolve()
        target = (parent, csv_path.stem, extension)
        if target in daily_targets:
            raise ConfigError("artifact path collision: daily CSV writers share a filename pattern")
        daily_targets.add(target)
        pattern = re.compile(re.escape(csv_path.stem) + r"-(\d{4}-\d{2}-\d{2})"
                             + re.escape(extension))
        other_paths = {path for key, path in paths.items() if key != csv_key} | protected | sidecars
        for path in other_paths:
            if path.parent != parent:
                continue
            match = pattern.fullmatch(path.name)
            if match:
                try:
                    date.fromisoformat(match.group(1))
                except ValueError:
                    continue
                raise ConfigError("artifact path collision: daily CSV files are reserved")


# -------------------------------------------------------------------- loading

def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str, read_credentials: bool = True) -> Config:
    if read_credentials:
        load_dotenv(env_file)
    try:
        with open(config_file) as fh:
            raw = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigError(
            f"config file '{config_file}' not found — copy config.example.yaml "
            f"to config.yaml and edit it")
    except yaml.YAMLError:
        raise ConfigError("config file contains invalid YAML syntax") from None
    _validate(raw, _SCHEMA)

    symbol = (symbol or "").strip()
    if not symbol:
        raise ConfigError("--symbol is required, e.g. --symbol SNDK")
    if hedge_venue not in HEDGE_VENUES:
        raise ConfigError(
            f"--hedge must be one of {list(HEDGE_VENUES)}, got {hedge_venue!r}")

    thr = raw.get("thresholds") or {}
    for k in ("midline_bps", "upper_bps", "lower_bps"):
        if k not in thr:
            raise ConfigError(f"'thresholds.{k}' is required — derive it from "
                              f"recorded minute data")
    upper, lower = float(thr["upper_bps"]), float(thr["lower_bps"])
    if upper <= 0 or lower <= 0:
        raise ConfigError("thresholds.upper_bps and lower_bps must be > 0 "
                          "(minimum required signal bands)")

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail")

    entropy_dex = _get(raw, "entropy", "dex", "io")
    if not entropy_dex or any(char.isspace() for char in entropy_dex) or ":" in entropy_dex:
        raise ConfigError("entropy.dex must be a nonempty dex name without whitespace or ':'")
    if hedge_venue == "tradexyz" and entropy_dex == "xyz":
        raise ConfigError("entropy.dex 'xyz' with hedge_venue 'tradexyz' is "
                          "the same market on both legs")

    # Offline validation and recording must not read either credential source.
    env_s = _env_s if read_credentials else lambda _name: None
    env_i = _env_i if read_credentials else lambda _name: None
    entropy_hl_creds = HLCreds(env_s("HL_PRIVATE_KEY"),
                               env_s("HL_ACCOUNT_ADDRESS"))
    entropy = VenueConf(
        key="entropy", kind="hl", label="ENTROPY",
        symbol=symbol,
        fee_bps=float(_get(raw, "entropy", "taker_fee_bps", 0.0)),
        cap_usd=float(_get(raw, "entropy", "max_position_usd", 1000.0)),
        orders_per_min=int(_get(raw, "entropy", "max_orders_per_min", 120)),
        hl_dex=entropy_dex,
        hl_creds=entropy_hl_creds,
    )

    if hedge_venue == "tradexyz":
        hedge = VenueConf(
            key="hedge", kind="hl", label="XYZ",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 1.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 120)),
            hl_dex="xyz",
            hl_creds=HLCreds(
                env_s("HL_PRIVATE_KEY_XYZ") or env_s("HL_PRIVATE_KEY"),
                env_s("HL_ACCOUNT_ADDRESS_XYZ") or env_s("HL_ACCOUNT_ADDRESS")),
        )
    else:
        hedge = VenueConf(
            key="hedge", kind="lighter",
            label="LIGHTER" if hedge_venue == "lighter" else "RH",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 0.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 30)),
            lighter_profile=LIGHTER_PROFILES[hedge_venue],
            lighter_creds=LighterCreds(env_i("LIGHTER_ACCOUNT_INDEX"),
                                       env_i("LIGHTER_API_KEY_INDEX"),
                                       env_s("LIGHTER_API_PRIVATE_KEY")),
        )

    cfg = Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        entropy=entropy,
        hedge=hedge,
        midline_bps=float(thr["midline_bps"]),
        upper_bps=upper,
        lower_bps=lower,
        take_fraction=take_fraction,
        max_order_notional=float(_get(raw, "sizing", "max_order_notional_usd", 500.0)),
        min_order_notional=float(_get(raw, "sizing", "min_order_notional_usd", 10.0)),
        inventory_scale_bps=float(_get(raw, "inventory", "scale_bps", 10.0)),
        inventory_floor_frac=float(_get(raw, "inventory", "floor_frac", 0.0)),
        premium_persist_sec=float(_get(raw, "execution", "premium_persist_sec", 0.3)),
        cooldown_sec=float(_get(raw, "execution", "cooldown_sec", 0.0)),
        settle_timeout_sec=float(_get(raw, "execution", "settle_timeout_sec", 5.0)),
        leg_slippage_bps=float(_get(raw, "execution", "leg_slippage_bps", 1.0)),
        hedge_slippage_bps=float(_get(raw, "execution", "hedge_slippage_bps", 20.0)),
        net_tolerance_base=float(_get(raw, "execution", "net_tolerance_base", 0.001)),
        max_consecutive_errors=int(_get(raw, "execution", "max_consecutive_errors", 3)),
        rate_limit_pause_sec=float(_get(raw, "execution", "rate_limit_pause_sec", 10.0)),
        staleness_sec=float(_get(raw, "execution", "staleness_sec", 10.0)),
        reconcile_sec=float(_get(raw, "execution", "reconcile_sec", 15.0)),
        venue_probe_sec=float(_get(raw, "execution", "venue_probe_sec", 30.0)),
        http_keepalive_sec=float(_get(raw, "execution", "http_keepalive_sec", 10.0)),
        recorder_enabled=bool(_get(raw, "recorder", "enabled", True)),
        recorder_csv=_get(raw, "recorder", "csv", "logs/{pair}/minutes.csv"),
        log_level=str(_get(raw, "logging", "level", "INFO")).upper(),
        status_interval_sec=float(_get(raw, "logging", "status_interval_sec", 30.0)),
        trades_csv=_get(raw, "logging", "trades_csv", "logs/{pair}/trades.csv"),
        dashboard=bool(_get(raw, "logging", "dashboard", True)),
        log_file=_get(raw, "logging", "file", "logs/{pair}/engine.log"),
        submit_timeout_sec=float(_get(raw, "execution", "submit_timeout_sec", 5.0)),
        recovery_poll_sec=float(_get(raw, "execution", "recovery_poll_sec", 2.0)),
        recovery_timeout_sec=float(_get(raw, "execution", "recovery_timeout_sec", 120.0)),
        shutdown_timeout_sec=float(_get(raw, "execution", "shutdown_timeout_sec", 30.0)),
        reconcile_grace_sec=float(_get(raw, "execution", "reconcile_grace_sec", 5.0)),
        net_tolerance_usd=float(_get(raw, "risk", "net_tolerance_usd", 1.0)),
        max_dust_usd=float(_get(raw, "risk", "max_dust_usd", 10.0)),
        min_free_collateral_usd=float(_get(raw, "risk", "min_free_collateral_usd", 25.0)),
        balance_staleness_sec=float(_get(raw, "risk", "balance_staleness_sec", 90.0)),
        position_staleness_sec=float(_get(raw, "risk", "position_staleness_sec", 60.0)),
        max_session_loss_usd=float(_get(raw, "risk", "max_session_loss_usd", 50.0)),
        state_db=_get(raw, "runtime", "state_db", "state/{pair}/orders.sqlite3"),
        health_file=_get(raw, "runtime", "health_file", "logs/{pair}/health.json"),
        min_disk_free_mb=float(_get(raw, "runtime", "min_disk_free_mb", 256.0)),
        log_max_bytes=int(_get(raw, "logging", "max_bytes", 10485760)),
        log_backup_count=int(_get(raw, "logging", "backup_count", 5)),
        recorder_rotate_daily=bool(_get(raw, "recorder", "rotate_daily", True)),
        recorder_max_pending_rows=int(_get(raw, "recorder", "max_pending_rows", 1440)),
    )
    _validate_runtime(cfg)
    for attr, key in (("recorder_csv", "recorder.csv"), ("trades_csv", "logging.trades_csv"),
                      ("log_file", "logging.file"), ("state_db", "runtime.state_db"),
                      ("health_file", "runtime.health_file")):
        value = _path_template(getattr(cfg, attr), cfg.pair_id, key)
        setattr(cfg, attr, str(Path(value).expanduser()))
    _validate_artifact_paths(cfg, config_file, env_file)
    return cfg
