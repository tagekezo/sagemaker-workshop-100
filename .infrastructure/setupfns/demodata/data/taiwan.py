"""Demo data loader for UCI Taiwan Credit dataset

See https://archive.ics.uci.edu/ml/datasets/default+of+credit+card+clients
"""

# Python Built-Ins:
from contextlib import closing
import io
import logging
import os
import time
import traceback

# External Dependencies:
import boto3
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import s3fs

logger = logging.getLogger("taiwan")


def load(s3_bucket_name: str, s3_prefix: str):
    print(f"boto3 version: {boto3.__version__}")
    print(f"pandas version: {pd.__version__}")
    print(f"s3fs version: {s3fs.__version__}")
    logger.info("Fetching data...")

    # Retry this request with backoff, in case spinning up a large session runs in to throttling issues:
    retry_strategy = Retry(
        backoff_factor=2,  # Time = {backoff factor} * (2 ** ({number of total retries} - 1))
        method_whitelist=["GET", "HEAD", "OPTIONS"],
        status_forcelist=[429, 500, 502, 503, 504],
        total=3,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    reqsess = requests.Session()
    reqsess.mount("https://", adapter)
    reqsess.mount("http://", adapter)
    res = reqsess.get(
        r"https://archive.ics.uci.edu/ml/machine-learning-databases/00350/default%20of%20credit%20card%20clients.xls",
        #stream=True,  # pandas.read_excel() needs seek() on the stream anyway, so will need to buffer it
    )

    logger.info("Extracting CSV...")
    with closing(res):
        df = pd.read_excel(io.BytesIO(res.content))
    # colnames = [c["name"] for c in schema]
    # with res.raw as rawdata:
    #     df = pd.read_excel(rawdata)

    # assert len(colnames) == len(df.columns), "Downloaded data file did not match expected schema!"
    # # Adjust for our use case:
    # for colspec in schema:
    #     colname = colspec["name"]
    #     colmap = colspec.get("map")
    #     if colmap:
    #         df[colname] = df[colname].apply(lambda v: colmap.get(v, v))

    # # Add dummy record ID and timestamp fields for SageMaker Feature Store:
    # df.index = df.index.set_names(["txn_id"])
    # df.reset_index(inplace=True)
    # df["txn_timestamp"] = round(time.time())

    # # Move the transaction timestamp (created at end) to just after the ID:
    # colnames = df.columns.tolist()
    # colnames.insert(1, colnames.pop())
    # df = df[colnames]

    out_uri = f"s3://{s3_bucket_name}/{s3_prefix}taiwan.csv"
    logger.info(f"Writing CSV to {out_uri}")
    print(f"Saving to {out_uri}")
    try:
        df.to_csv(out_uri, header=False, index=False)
    except Exception as e:
        # TODO: Why does pandas S3 auth not work correctly even with pinned versions? PermissionsError
        print(f"Preferred upload method failed - trying alternative")
        traceback.print_exc()
        bucketfs = s3fs.S3FileSystem(anon=False)  # uses default credentials
        with bucketfs.open(f"{s3_bucket_name}/{s3_prefix}taiwan.csv", "wb") as f:
            df.to_csv(f, header=False, index=False)
