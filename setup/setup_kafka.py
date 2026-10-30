"""
setup/setup_kafka.py
─────────────────────
Downloads Apache Kafka (if not present), starts Zookeeper + Kafka broker,
and creates or recreates all required topics.

Usage:
    python setup/setup_kafka.py [--kafka-dir C:/kafka] [--recreate] [--partitions 4]
"""

import argparse
import os
import pathlib
import subprocess
import sys
import time
import urllib.request
import shutil

KAFKA_VERSION     = "3.7.0"
SCALA_VERSION     = "2.13"
KAFKA_ARCHIVE     = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
KAFKA_URL_ARCHIVE = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}"
KAFKA_URL_MIRROR  = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{KAFKA_ARCHIVE}"
DEFAULT_KAFKA_DIR = pathlib.Path("C:/kafka")

TOPICS = {
    "taxi.raw":     {"partitions": 4, "replication": 1},
    "taxi.clean":   {"partitions": 4, "replication": 1},
    "taxi.metrics": {"partitions": 1, "replication": 1},
    "taxi.lsh":     {"partitions": 1, "replication": 1},
}


def find_kafka(kafka_dir: pathlib.Path) -> pathlib.Path | None:
    if kafka_dir.exists():
        candidates = list(kafka_dir.glob("kafka*/bin/windows"))
        if candidates:
            return candidates[0].parent.parent
        if (kafka_dir / "bin" / "windows").exists():
            return kafka_dir
    return None


def download_and_extract(kafka_dir: pathlib.Path):
    archive = pathlib.Path(KAFKA_ARCHIVE)
    print(f"Downloading Kafka {KAFKA_VERSION} ...")
    try:
        urllib.request.urlretrieve(KAFKA_URL_ARCHIVE, str(archive))
    except Exception as e:
        print(f"Archive URL failed ({e}), trying mirror ...")
        urllib.request.urlretrieve(KAFKA_URL_MIRROR, str(archive))

    print("Extracting ...")
    import tarfile
    with tarfile.open(str(archive), "r:gz") as t:
        t.extractall(str(kafka_dir.parent))
    extracted = kafka_dir.parent / f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}"
    if extracted.exists():
        if kafka_dir.exists():
            shutil.rmtree(str(kafka_dir))
        extracted.rename(kafka_dir)
    archive.unlink(missing_ok=True)
    print(f"Kafka installed to {kafka_dir}")


def recreate_topic(kafka_dir: pathlib.Path, name: str, partitions: int, replication: int, delete_first: bool = False):
    """Create topic with exact requested partitions. Handles already exists cleanly on Windows."""
    bat = str(kafka_dir / "bin" / "windows" / "kafka-topics.bat")
    if delete_first:
        del_cmd = [
            bat,
            "--bootstrap-server", "127.0.0.1:9092",
            "--delete",
            "--topic", name,
        ]
        try:
            subprocess.run(del_cmd, capture_output=True, text=True, timeout=10)
            time.sleep(1)
        except Exception:
            pass

    create_cmd = [
        bat,
        "--bootstrap-server", "127.0.0.1:9092",
        "--create",
        "--topic", name,
        "--partitions", str(partitions),
        "--replication-factor", str(replication),
    ]
    try:
        result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            print(f"  [OK] Topic '{name}' created with {partitions} partition(s)")
        else:
            msg = [l for l in (result.stderr + "\n" + result.stdout).splitlines() if l.strip()]
            err_line = msg[-1] if msg else "exists"
            print(f"  [NOTE] Topic '{name}': {err_line}")
    except Exception as e:
        print(f"  [WARN] Topic '{name}' creation timed out: {e}")


def patch_windows_scripts(kafka_dir: pathlib.Path):
    server_bat = kafka_dir / "bin" / "windows" / "kafka-server-start.bat"
    if server_bat.exists():
        content = server_bat.read_text(encoding="utf-8", errors="ignore")
        old = 'wmic os get osarchitecture | find /i "32-bit" >nul 2>&1'
        if old in content:
            content = content.replace(old, 'cmd /c exit 1')
            server_bat.write_text(content, encoding="utf-8")
        elif "IF 1==2" in content:
            content = content.replace("rem wmic removed for Win11\r\n    IF 1==2", "cmd /c exit 1")
            content = content.replace("rem wmic removed for Win11\n    IF 1==2", "cmd /c exit 1")
            server_bat.write_text(content, encoding="utf-8")


def is_port_in_use(port: int) -> bool:
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            return s.connect_ex(("127.0.0.1", port)) == 0
    except Exception:
        return False


def start_zookeeper(kafka_dir: pathlib.Path):
    if is_port_in_use(2181):
        print("Zookeeper is already running on 127.0.0.1:2181.")
        return None
    zk_bat = kafka_dir / "bin" / "windows" / "zookeeper-server-start.bat"
    zk_cfg = kafka_dir / "config" / "zookeeper.properties"
    print("Starting Zookeeper ...")
    proc = subprocess.Popen(
        [str(zk_bat), str(zk_cfg)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
    )
    return proc


def start_broker(kafka_dir: pathlib.Path):
    if is_port_in_use(9092):
        print("Kafka broker is already running on 127.0.0.1:9092.")
        return None
    kb_bat = kafka_dir / "bin" / "windows" / "kafka-server-start.bat"
    kb_cfg = kafka_dir / "config" / "server.properties"
    print("Starting Kafka broker ...")
    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = "-Xmx1G -Xms1G"
    proc = subprocess.Popen(
        [str(kb_bat), str(kb_cfg)],
        env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
    )
    return proc


def wait_for_kafka(retries: int = 20, delay: float = 3.0):
    import socket
    print("Waiting for Kafka to be ready", end="", flush=True)
    for _ in range(retries):
        try:
            with socket.create_connection(("127.0.0.1", 9092), timeout=2):
                print(" [OK]")
                return True
        except OSError:
            print(".", end="", flush=True)
            time.sleep(delay)
    print(" [FAIL] Kafka not reachable after timeout")
    return False


def main():
    parser = argparse.ArgumentParser(description="Kafka setup for BDA Assignment 2")
    parser.add_argument("--kafka-dir", default=str(DEFAULT_KAFKA_DIR), help="Kafka installation directory")
    parser.add_argument("--skip-start", action="store_true", help="Skip starting Zookeeper/broker")
    parser.add_argument("--partitions", type=int, default=4, help="Partition count for taxi.raw/taxi.clean")
    parser.add_argument("--recreate", action="store_true", default=False, help="Recreate/delete topics (only safe on fresh/stopped Kafka)")
    args = parser.parse_args()

    kafka_dir = pathlib.Path(args.kafka_dir)
    TOPICS["taxi.raw"]["partitions"]   = args.partitions
    TOPICS["taxi.clean"]["partitions"] = args.partitions

    found = find_kafka(kafka_dir)
    if not found:
        download_and_extract(kafka_dir)
        found = find_kafka(kafka_dir)

    if not found:
        print("ERROR: Could not find or install Kafka.")
        sys.exit(1)

    kafka_dir = found
    patch_windows_scripts(kafka_dir)
    print(f"Kafka directory: {kafka_dir}")

    if not args.skip_start:
        zk_proc  = start_zookeeper(kafka_dir)
        time.sleep(5)
        kb_proc  = start_broker(kafka_dir)

        if not wait_for_kafka():
            print("Kafka failed to start. Check the console windows for errors.")
            sys.exit(1)
    else:
        print("Skipping Zookeeper/broker start (--skip-start)")
        if not wait_for_kafka(retries=3, delay=1):
            print("Kafka is not running. Start it first.")
            sys.exit(1)

    print("\nCreating topics (if not already existing) ...")
    time.sleep(2)
    for name, opts in TOPICS.items():
        recreate_topic(kafka_dir, name, opts["partitions"], opts["replication"], delete_first=args.recreate)

    print("\nSetup complete! Kafka is running on 127.0.0.1:9092")
    print("Topics configured: " + ", ".join(TOPICS.keys()))


if __name__ == "__main__":
    main()
