import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DataEngSettings:
    s3_endpoint_url: str = field(default_factory=lambda: os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: os.environ.get("S3_ACCESS_KEY", "posttrade"))
    s3_secret_key: str = field(default_factory=lambda: os.environ.get("S3_SECRET_KEY", "posttrade123"))
    s3_region: str = field(default_factory=lambda: os.environ.get("S3_REGION", "us-east-1"))

    bronze_bucket: str = field(default_factory=lambda: os.environ.get("BRONZE_BUCKET", "bronze"))
    silver_path: str = field(default_factory=lambda: os.environ.get("SILVER_PATH", "./dataeng_lake/silver"))
    gold_duckdb_path: str = field(
        default_factory=lambda: os.environ.get("GOLD_DUCKDB_PATH", "./dataeng_lake/gold/posttrade_gold.duckdb")
    )


settings = DataEngSettings()
