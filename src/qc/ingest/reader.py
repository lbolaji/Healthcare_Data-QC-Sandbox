import io
import logging
import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _s3_client():
    return boto3.client("s3")


def discover_clients_domains(bucket: str, prefix: str, run_date: str) -> list[tuple[str, str]]:
    s3 = _s3_client()
    search_prefix = f"{prefix}/date={run_date}/"
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    seen = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp["Prefix"].rstrip("/").split("/")[-1]
            if part.startswith("client="):
                client_name = part.split("=", 1)[1]
                domain_prefix = f"{search_prefix}{part}/"
                for dpage in paginator.paginate(Bucket=bucket, Prefix=domain_prefix, Delimiter="/"):
                    for dp in dpage.get("CommonPrefixes", []):
                        dpart = dp["Prefix"].rstrip("/").split("/")[-1]
                        if dpart.startswith("domain="):
                            domain_name = dpart.split("=", 1)[1]
                            key = (client_name, domain_name)
                            if key not in seen:
                                seen.add(key)
                                results.append(key)
    logger.info("discovered %d client+domain pairs for date=%s", len(results), run_date)
    return results


def load_domain(bucket: str, prefix: str, client: str, domain: str, run_date: str) -> pd.DataFrame:
    s3 = _s3_client()
    key_prefix = f"{prefix}/date={run_date}/client={client}/domain={domain}/"
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if key.endswith(".parquet"):
                frames.append(pq.read_table(io.BytesIO(body)).to_pandas())
            elif key.endswith(".csv"):
                frames.append(pd.read_csv(io.BytesIO(body)))
            else:
                logger.warning("skipping unsupported file type: %s", key)
    if not frames:
        raise FileNotFoundError(f"no data files at s3://{bucket}/{key_prefix}")
    df = pd.concat(frames, ignore_index=True)
    logger.info("loaded client=%s domain=%s rows=%d", client, domain, len(df))
    return df
