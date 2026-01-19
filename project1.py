import os
import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime
import logging

# ---------- logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- config ----------
S3_BUCKET = os.environ["S3_BUCKET"]
S3_KEY = os.environ["S3_KEY"]
FALLBACK_PATH = os.environ["FALLBACK_PATH"]

RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]
RDS_HOST = os.environ["RDS_HOST"]
RDS_DB = os.environ["RDS_DB"]

GLUE_CRAWLER = os.environ["GLUE_CRAWLER"]

# ---------- clients ----------
s3 = boto3.client("s3")
glue = boto3.client("glue")

# ---------- functions ----------
def read_from_s3():
    logger.info("Reading CSV from S3")
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    csv_data = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(csv_data))


def load_to_mysql(df):
    logger.info("Loading data into RDS")
    engine = create_engine(
        f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:3306/{RDS_DB}"
    )

    df.to_sql(
        name="ingestin_db",
        con=engine,
        if_exists="append",   # production-safe
        index=False,
        chunksize=5000,
        method="multi"
    )


def write_to_fallback(df):
    path = f"{FALLBACK_PATH}/load_date={datetime.now().date()}/"
    logger.warning(f"Writing fallback data to {path}")
    df.to_parquet(path, engine="pyarrow", index=False)


def trigger_crawler():
    try:
        glue.start_crawler(Name=GLUE_CRAWLER)
        logger.info("Glue crawler started")
    except glue.exceptions.CrawlerRunningException:
        logger.info("Glue crawler already running")


# ---------- main ----------
if __name__ == "__main__":
    try:
        df = read_from_s3()
        load_to_mysql(df)
        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error("MySQL load failed", exc_info=True)
        write_to_fallback(df)
        trigger_crawler()
