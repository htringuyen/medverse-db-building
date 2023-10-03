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

config = dict()
supported_compression_formats = ['gzip', 'zip', 'none']


class LocalServer(object):

    def __init__(self):
        self._driver = sync_db.driver(config['server_uri'],
                                            auth=(config['admin_user'],
                                                  config['admin_pass']), encrypted=False,
                                            database=config['database'])
        self._async_driver = async_db.driver(config['server_uri'],
                                            auth=(config['admin_user'],
                                                  config['admin_pass']), encrypted=False,
                                            database=config['database'])
        self.db_config = {}

    async def close(self):
        self._driver.close()
        await self._async_driver.close()

    async def load_file(self, file):
        # Set up parameters/defaults
        # Check skip_file first so we can exit early
        skip = file.get('skip_file') or False
        if skip:
            print("Skipping this file: {}", file['url'])
            return

        print("{} : Reading file", datetime.datetime.utcnow())

        # Check if we are using async or sync
        params = self.get_params(file)
        mode = params['mode']

        if mode == 'async' and not params['thread_count']:
            print('Error! thread_count should be specified when running with mod async')
            return

        # If file type is specified, use that.  Else check the extension.  Else, treat as csv
        type = file.get('type') or 'NA'
        if type != 'NA':
            if type == 'csv' and mode == 'sync':
                self.load_csv(file)
            elif type == 'csv' and mode == 'async':
                await self.load_csv_async(file)
            elif type == 'json':
                self.load_json(file)
            elif type == 'parquet':
                self.load_parquet(file)
            else:
                print("Error! Can't process file because unknown type", type, "was specified")
        else:
            file_suffixes = pathlib.Path(file['url']).suffixes
            if '.csv' in file_suffixes and mode == 'sync':
                self.load_csv(file)
            elif '.csv' in file_suffixes and mode == 'async':
                await self.load_csv_async(file)
            elif '.json' in file_suffixes:
                self.load_json(file)
            elif '.parquet' in file_suffixes:
                self.load_parquet(file)
            else:
                if mode == 'sync':
                    self.load_csv(file)
                elif mode == 'async':
                    await self.load_csv_async(file)

    @staticmethod
    def get_params(file):
        params = dict()
        params['skip_records'] = file.get('skip_records') or 0
        params['compression'] = file.get('compression') or 'none'
        if params['compression'] not in supported_compression_formats:
            print("Unsupported compression format: {}", params['compression'])

        params['url'] = file['url']
        print("File {}", params['url'])
        params['cql'] = file['cql']
        params['mode'] = file.get('mode') or 'sync'
        params['thread_count'] = file.get('thread_count') or None
        params['chunk_size'] = file.get('chunk_size') or 1000
        params['log_step'] = file.get('log_step') or 1
        params['field_sep'] = (file.get('field_separator') if file.get('field_separator') != '\\t' else '\t') or ','

        params['parquet_suffix_whitelist'] = file.get('parquet_suffix_whitelist') or None
        params['parquet_suffix_blacklist'] = file.get('parquet_suffix_blacklist') or None
        params['parquet_partition_filter'] = file.get('parquet_partition_filter') or None
        params['parquet_columns'] = file.get('parquet_columns') or None
        params['parquet_start_from_mod_date'] = file.get('parquet_start_from_mod_date') or None
        params['parquet_up_to_mod_date'] = file.get('parquet_up_to_mod_date') or None
        params['parquet_s3_additional_args'] = file.get('parquet_s3_additional_args') or None
        params['parquet_as_dataset'] = file.get('parquet_as_dataset') or False
        return params

    def load_csv(self, file):
        start = time.time()
        rows_count = 0
        with self._driver.session() as session:
            params = self.get_params(file)
            openfile = file_handle(params['url'], params['compression'])

            # - The file interfaces should be consistent in Python but they aren't
            if params['compression'] == 'zip':
                header = openfile.readline().decode('UTF-8')
            else:
                header = str(openfile.readline())

            # Grab the header from the file and pass that to pandas.  This allow the header
            # to be applied even if we are skipping lines of the file
            header = header.strip().split(params['field_sep'])

            # Pandas' read_csv method is highly optimized and fast :-)
            row_chunks = pd.read_csv(openfile, dtype=str, sep=params['field_sep'], on_bad_lines='error',
                                     index_col=False, skiprows=params['skip_records'], names=header,
                                     low_memory=False, engine='c', compression='infer', header=None,
                                     chunksize=params['chunk_size'], keep_default_na=False, na_values=[''])

            for i, rows in enumerate(row_chunks):
                rows_count += len(rows)
                if i % params['log_step'] == 0:
                    print(file['url'], datetime.datetime.now(), str(i) + "|" + str(i * params['chunk_size']), flush=True)
                # Chunk up the rows to enable additional fastness :-)
                rows_dict = {'rows': rows.fillna(value="").to_dict('records')}
                session.run(params['cql'],
                            dict=rows_dict).consume()
        end = time.time()
        print(f"{rows_count} rows ingested, time elapsed: {end - start} seconds")
        print("{} : Completed file", datetime.datetime.now())


    async def load_csv_async(self, file):
        start = time.time()
        rows_count = 0
        try:
            params = self.get_params(file)
            openfile = file_handle(params['url'], params['compression'])

            # - The file interfaces should be consistent in Python but they aren't
            if params['compression'] == 'zip':
                header = openfile.readline().decode('UTF-8')
            else:
                header = str(openfile.readline())

            # Grab the header from the file and pass that to pandas.  This allow the header
            # to be applied even if we are skipping lines of the file
            header = header.strip().split(params['field_sep'])

            # Pandas' read_csv method is highly optimized and fast :-)
            row_chunks = pd.read_csv(openfile, dtype=str, sep=params['field_sep'], on_bad_lines='error',
                                     index_col=False, skiprows=params['skip_records'], names=header,
                                     low_memory=False, engine='c', compression='infer', header=None,
                                     chunksize=params['chunk_size'], keep_default_na=False, na_values=[''])
            process_params = []
            session_index = 0
            awaiter = None
            for i, rows in enumerate(row_chunks):
                if i % params['log_step'] == 0:
                    print(file['url'], datetime.datetime.now(), str(i) + "|" + str(i * params['chunk_size']), flush=True)
                session_index = i % params['thread_count']

                rows_count += len(rows)

                # Chunk up the rows to enable additional fastness :-)
                rows_dict = {'rows': rows.fillna(value="").to_dict('records')}

                process_params.append({'session_index': session_index, 'cql': params['cql'], 'rows_dict': rows_dict})

                if session_index == params['thread_count'] - 1:
                    tasks = []
                    for p in process_params:
                        tasks.append(asyncio.create_task(
                            self.run_cql_wrapper(p['session_index'], p['cql'], p['rows_dict'])))

                    if awaiter is not None:
                        await awaiter

                    awaiter = asyncio.gather(*tasks)

                    process_params = []

            tasks = []
            for p in process_params:
                tasks.append(asyncio.create_task(
                    self.run_cql_wrapper(p['session_index'], p['cql'], p['rows_dict'])))
            if len(tasks) > 0:
                await asyncio.gather(*tasks)

        except Exception as e:
            print("Error!" + str(e))
        end = time.time()
        print(f"{rows_count} rows ingested, time elapsed: {end - start} seconds")
        print("{} : Completed file", datetime.datetime.now())


    # This function is created to retry when deadlocks occur
    # However it decreases performance greatly and it seems to be loading the data nevertheless
    # So I set the retry count to 1 so it actually does not take into considerations deadlocks
    async def run_cql_wrapper(self, session_index, cql, dict):
        max_try_count, i, retry = 5, 0, True
        while retry and i < max_try_count:
            try:
                await self.run_cql(session_index, cql, dict)
                retry = False
            except Exception as e:
                if hasattr(e, 'code') and e.code == 'Neo.TransientError.Transaction.DeadlockDetected':
                    print('Deadlock detected! Session: %s' % session_index)
                    i += 1
                else:
                    print('Exception occured (Session : %d)' % (session_index))
                    print(e)
                    i += 1

    async def run_cql(self, session_index, cql, dict):
        #print('Running session %d' % session_index)

        async with self._async_driver.session(**self.db_config) as session:
            await session.run(cql, dict=dict)

        #print('Completed session %d' % session_index)

    def pre_ingest(self):
        if 'pre_ingest' in config:
            statements = config['pre_ingest']

            with self._driver.session() as session:
                for statement in statements:
                    session.run(statement)

    def post_ingest(self):
        if 'post_ingest' in config:
            statements = config['post_ingest']

            with self._driver.session() as session:
                for statement in statements:
                    session.run(statement)


def file_handle(url, compression):
    parsed = urlparse(url)
    if parsed.scheme == 's3':
        path = get_s3_client().get_object(Bucket=parsed.netloc, Key=parsed.path[1:])['Body']
    elif parsed.scheme == 'file':
        path = parsed.path
    else:
        path = url
    if compression == 'gzip':
        return gzip.open(path, 'rt')
    elif compression == 'zip':
        # Only support single file in ZIP archive for now
        if isinstance(path, str):
            buffer = path
        else:
            buffer = io.BytesIO(path.read())
        zf = ZipFile(buffer)
        filename = zf.infolist()[0].filename
        return zf.open(filename)
    else:
        return open(path)


def get_s3_client():
    return boto3.Session().client('s3')


def load_config(configuration):
    global config
    with open(configuration) as config_file:
        config = yaml.SafeLoader(config_file).get_data()


async def main():
    start = time.time()
    configuration = sys.argv[1]
    load_config(configuration)
    server = LocalServer()
    server.pre_ingest()
    file_list = config['files']
    for file in file_list:
        await server.load_file(file)
    server.post_ingest()
    await server.close()
    end = time.time()
    print(f"\npyingest completed in {end - start} seconds")


if __name__ == "__main__":
    asyncio.run(main())
