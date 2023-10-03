import asyncio

try:
    from neo4j._async.driver import AsyncGraphDatabase as async_db
except ModuleNotFoundError:
    print('Error! You should be running neo4j python driver version 5 to use async features')
import time
import pandas as pd
from neo4j import GraphDatabase as sync_db
import yaml
import datetime
import sys
import gzip
from zipfile import ZipFile
from urllib.parse import urlparse
import boto3
from smart_open import open
import io
import pathlib
import ijson
import awswrangler as wr

def main():
    driver = sync_db.driver("bolt://localhost:7687", auth=("snowj", "abcd1234"), database="medverse")
    session = driver.session()
    result = session.run("return datetime({epochmillis: apoc.date.parse('2180-08-05 20:58:00', 'ms', 'yyyy-MM-dd HH:mm:ss')})")
    print(result.single()[0])
    session.close()
    driver.close()


if __name__ == '__main__':
    main()