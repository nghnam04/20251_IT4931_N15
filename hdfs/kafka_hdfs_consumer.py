"""
Kafka -> HDFS sink (Parquet) (dev-ready)

Reads JSON messages from Kafka topic, batches them, writes Parquet (via pyarrow)
and uploads to HDFS (WebHDFS) using hdfs.InsecureClient.

Usage example:
python -m hdfs_integration.kafka_hdfs_consumer \
    --bootstrap-servers localhost:9092 \
    --topic my-topic \
    --hdfs-web http://localhost:9870 \
    --hdfs-base /data/project/raw \
    --hdfs-user root \
    --batch-size 500 \
    --flush-interval 30
"""
import argparse
import json
import time
from datetime import datetime
from typing import List, Dict, Any
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from .hdfs_client import HDFSClientWrapper


def get_partition_path_from_timestamp(ts_str: str) -> str:
    """
    Normalize a timestamp string to partition path like ingest_date=YYYY-MM-DD
    Accepts ISO or epoch seconds as string. If parsing fails, use current date.
    """
    try:
        # try ISO
        if ts_str.isdigit():
            dt = datetime.fromtimestamp(int(ts_str))
        else:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.now()
        except Exception:
            dt = datetime.utcnow()
    return f"ingest_date={dt.strftime('%Y-%m-%d')}"


class KafkaHDFSSink:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "hdfs-sink-group",
        hdfs_web: str = "http://localhost:9870",
        hdfs_user: str = "root",
        hdfs_base: str = "/data/project/raw",
        batch_size: int = 1000,
        flush_interval: int = 60
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )
        self.hdfs = HDFSClientWrapper(webhdfs_url=hdfs_web, user=hdfs_user)
        self.hdfs_base = hdfs_base.rstrip("/")
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()

    def flatten(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adapt this to your message schema. By default returns message as-is but ensures certain fields.
        Ensure there is a time_published or timestamp field for partitioning.
        """
        data = msg.copy()
        # If nested under 'data', bring up fields
        if "data" in data and isinstance(data["data"], dict):
            data.update(data.pop("data"))
        # add ingest_timestamp
        data.setdefault("ingest_timestamp", datetime.utcnow().isoformat())
        return data

    def write_parquet_and_upload(self, records: List[Dict[str, Any]], partition_path: str):
        """
        Create a pyarrow table from records and write to local temp parquet,
        then upload to HDFS under hdfs_base/partition_path/part-<ts>.parquet
        """
        if not records:
            return
        table = pa.Table.from_pylist(records)
        timestmp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"part-{timestmp}.parquet"
        hdfs_path = f"{self.hdfs_base}/{partition_path}/{filename}"
        def write_cb(local_path: str):
            pq.write_table(table, local_path, compression="snappy")
        self.hdfs.write_local_temp_and_upload(write_cb, hdfs_path, overwrite=False, suffix=".parquet")

    def flush(self):
        if not self.buffer:
            return
        # group by partition
        partitions: Dict[str, List[Dict[str, Any]]] = {}
        for m in self.buffer:
            # choose a timestamp field if available
            time_field = m.get("time_published") or m.get("timestamp") or m.get("ingest_timestamp")
            partition = get_partition_path_from_timestamp(str(time_field)) if time_field else get_partition_path_from_timestamp("")
            partitions.setdefault(partition, []).append(m)
        for partition_path, recs in partitions.items():
            try:
                self.hdfs.ensure_dir(f"{self.hdfs_base}/{partition_path}")
                self.write_parquet_and_upload(recs, partition_path)
            except Exception as e:
                print(f"[ERROR] writing partition {partition_path}: {e}")
        # commit offsets after successful writes
        try:
            self.consumer.commit()
        except Exception:
            pass
        self.buffer = []
        self.last_flush_time = time.time()

    def run(self):
        print("Starting Kafka->HDFS sink...")
        try:
            for message in self.consumer:
                try:
                    rec = message.value
                    rec_flat = self.flatten(rec)
                    self.buffer.append(rec_flat)
                    if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush_time) >= self.flush_interval:
                        self.flush()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"[WARN] error processing message: {e}")
        except KeyboardInterrupt:
            print("Interrupted, flushing remaining...")
            self.flush()
        finally:
            try:
                if self.buffer:
                    self.flush()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Kafka to HDFS Parquet sink")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group-id", default="hdfs-sink-group")
    parser.add_argument("--hdfs-web", default=None, help="WebHDFS URL, e.g. http://localhost:9870")
    parser.add_argument("--hdfs-user", default="root")
    parser.add_argument("--hdfs-base", default="/data/project/raw")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--flush-interval", type=int, default=60)
    args = parser.parse_args()

    sink = KafkaHDFSSink(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        hdfs_web=args.hdfs_web or None,
        hdfs_user=args.hdfs_user,
        hdfs_base=args.hdfs_base,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval
    )
    sink.run()


if __name__ == "__main__":
    main()
