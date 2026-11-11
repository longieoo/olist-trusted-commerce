import hashlib
import json
import os
import time
from pathlib import PurePosixPath
import boto3
from botocore.exceptions import ClientError
from .contracts import digest

def require(names):
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError("Missing variables: " + ", ".join(missing))

class Store:
    def __init__(self):
        require(["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"])
        self.bucket = os.getenv("S3_BUCKET", "olist")
        self.client = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"), aws_access_key_id=os.environ["MINIO_ROOT_USER"], aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"], region_name="us-east-1")
        for attempt in range(6):
            try:
                if not any(x["Name"] == self.bucket for x in self.client.list_buckets()["Buckets"]):
                    self.client.create_bucket(Bucket=self.bucket)
                break
            except Exception:
                if attempt == 5: raise
                time.sleep(min(2**attempt, 8))

    def get(self, key):
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def maybe(self, key):
        try: return self.get(key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]: return None
            raise

    def put(self, key, data, immutable=False):
        if immutable:
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=data, IfNoneMatch="*")
                return
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["PreconditionFailed", "412", "ConditionalRequestConflict"]: raise
                if self.get(key) != data: raise ValueError("Immutable object conflict: " + key) from None
                return
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def json(self, key, value, immutable=False):
        self.put(key, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode(), immutable)

    def put_file(self, key, path, immutable=False):
        from .io_utils import hash_file
        options = {'IfNoneMatch': '*'} if immutable else {}
        try:
            with open(path, 'rb') as stream:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=stream, **options)
        except ClientError as error:
            if not immutable or error.response['Error']['Code'] not in ['PreconditionFailed', '412', 'ConditionalRequestConflict']:
                raise
            remote = self.describe(key)
            from pathlib import Path
            if remote['sha256'] != hash_file(path) or remote['bytes'] != Path(path).stat().st_size:
                raise ValueError('Immutable object conflict: ' + key) from None

    def keys(self, prefix):
        return [o["Key"] for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix) for o in page.get("Contents", [])]

    def describe(self, key, rows=None):
        digest = hashlib.sha256(); size = 0
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        with response['Body'] as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk); size += len(chunk)
        return {"key": key, "sha256": digest.hexdigest(), "bytes": size, "rows": rows}

    def verify(self, manifest, prefix):
        if type(manifest.get('schema_version')) is not int or manifest['schema_version'] != 1 or not isinstance(manifest.get('files'), list) or not manifest['files']:
            raise ValueError("Invalid/empty manifest")
        seen = set()
        for f in manifest["files"]:
            key = f["key"]
            if not key.startswith(prefix + "/") or ".." in PurePosixPath(key).parts or key in seen:
                raise ValueError("Unsafe/duplicate manifest path")
            seen.add(key)
            actual = self.describe(key)
            if actual['sha256'] != f['sha256'] or actual['bytes'] != f['bytes']:
                raise ValueError("Manifest checksum mismatch: " + key)
        return manifest

    def committed(self, key, prefix):
        return self.verify(json.loads(self.get(key)), prefix)

def batch_id(dataset_version, start=None, end=None):
    return digest([dataset_version, start, end, 1])[:24]
