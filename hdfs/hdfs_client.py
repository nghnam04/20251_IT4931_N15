"""
HDFS client wrapper using hdfs.InsecureClient (WebHDFS)
"""
from hdfs import InsecureClient
import os
from pathlib import Path
import tempfile
from typing import Optional


class HDFSClientWrapper:
    def __init__(self, webhdfs_url: str = None, user: str = "root", timeout: int = 30):
        """
        webhdfs_url: e.g. http://namenode:9870
        user: HDFS user (root or hdfs)
        """
        self.webhdfs_url = webhdfs_url or os.getenv("HDFS_WEB_URL", "http://localhost:9870")
        self.user = user or os.getenv("HDFS_USER", "root")
        self.client = InsecureClient(self.webhdfs_url, user=self.user, timeout=timeout)

    def ensure_dir(self, hdfs_dir: str):
        """
        Ensure directory exists on HDFS (creates recursively)
        """
        try:
            self.client.status(hdfs_dir)
        except Exception:
            # create
            self.client.makedirs(hdfs_dir)

    def upload_file(self, local_path: str, hdfs_path: str, overwrite: bool = False):
        """
        Upload local file to HDFS path. hdfs_path is full path including filename.
        """
        self.ensure_dir(str(Path(hdfs_path).parent))
        with open(local_path, "rb") as f:
            # write takes a file-like object
            self.client.write(hdfs_path, f, overwrite=overwrite)

    def write_bytes(self, data: bytes, hdfs_path: str, overwrite: bool = False):
        """
        Write bytes to HDFS path.
        """
        self.ensure_dir(str(Path(hdfs_path).parent))
        # write accepts bytes like this
        self.client.write(hdfs_path, data, overwrite=overwrite)

    def write_local_temp_and_upload(self, write_callback, hdfs_path: str, overwrite: bool = False, suffix: str = ".parquet"):
        """
        Utility: create a temp file, run write_callback(path) to write to it,
        then upload to HDFS.
        write_callback(local_path) should write the file to local_path.
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
        try:
            write_callback(tmp_path)
            self.upload_file(tmp_path, hdfs_path, overwrite=overwrite)
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
