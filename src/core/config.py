from pathlib import Path
from typing import List

import yaml
from pydantic import BaseModel, Field, PositiveFloat, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


class BotConfig(BaseModel):
    cycle_interval_seconds: PositiveFloat = 60
    jitter_seconds: float = 5
    log_level: str = "INFO"
    log_file: str = "logs/bot.log"
    database_path: str = "data/bot.db"


class MarketFilterConfig(BaseModel):
    min_volume_24h: float = 1000.0
    max_spread_percent: float = 5.0
    min_liquidity_depth: float = 500.0
    allow_keywords: List[str] = Field(default_factory=list)
    deny_keywords: List[str] = Field(default_factory=list)


class RiskConfig(BaseModel):
    starting_capital: float = 10000.0
    equity_floor: float = 8000.0
    max_daily_loss: float = 1000.0
    max_error_burst: int = 5
    kelly_fraction: float = 0.25
    max_position_size_percent: float = 10.0
    max_market_exposure_percent: float = 20.0
    max_total_exposure_percent: float = 50.0

    @validator("kelly_fraction")
    def kelly_in_0_1(cls, v: float) -> float:
        if not 0.0 < v <= 1.0:
            raise ValueError("kelly_fraction must be between 0 and 1")
        return v


class FeesConfig(BaseModel):
    taker_fee_bps: float = 20.0
    slippage_buffer_bps: float = 10.0
    fixed_buffer_bps: float = 5.0


class EstimatorConfig(BaseModel):
    fair_value_source: str = "midpoint"  # midpoint, last_trade, weighted
    min_edge_bps: float = 50.0


class AppConfig(BaseModel):
    bot: BotConfig
    market_filters: MarketFilterConfig
    risk: RiskConfig
    fees: FeesConfig
    estimator: EstimatorConfig


class EnvSettings(BaseSettings):
    DATABASE_PATH: str | None = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def _load_yaml_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r") as f:
        return yaml.safe_load(f) or {}


def load_config(config_path: Path | None = None) -> AppConfig:
    load_dotenv()
    if config_path is None:
        config_path = Path("config.yaml")
    data = _load_yaml_config(config_path)
    cfg = AppConfig(**data)

    env = EnvSettings()
    if env.DATABASE_PATH:
        cfg.bot.database_path = env.DATABASE_PATH

    return cfg
