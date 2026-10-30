# scripts/run_restart_demo_live.py
import subprocess
import sys
import time
import socket
import os
import shutil
import pathlib

BASE_DIR = pathlib.Path(__file__).parent.parent
JAVA = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot\bin\java.exe"
KAFKA = r"C:\kafka"
LOG4J = "-Dlog4j.configuration=file:C:/kafka/config/log4j.properties"
LOGDIR = "-Dkafka.logs.dir=C:/kafka/logs"
DATA_DIR = pathlib.Path(KAFKA) / "data"
RESULTS_DIR = BASE_DIR / "results" / "restart_demo"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def wait_for_port(port, timeout=45):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return True
        except Exception:
            time.sleep(2)
    return False

def kill_proc(p):
    if p and p.poll() is None:
        try:
            p.terminate()
            p.wait(timeout=3)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

print("[1] Cleaning Kafka data directory...")
if DATA_DIR.exists():
    try:
        shutil.rmtree(DATA_DIR)
    except Exception as e:
        print(f"Warning: rmtree failed: {e}")

print("[2] Starting Zookeeper...")
zk_p = subprocess.Popen([
    JAVA, "-Xmx512M", LOG4J, LOGDIR,
    "-cp", f"{KAFKA}\\libs\\*",
    "org.apache.zookeeper.server.quorum.QuorumPeerMain",
    f"{KAFKA}\\config\\zookeeper.properties"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if not wait_for_port(2181, 30):
    kill_proc(zk_p)
    raise RuntimeError("Zookeeper failed to start on port 2181")
print("  Zookeeper running on port 2181.")

print("[3] Starting Kafka broker...")
kf_p = subprocess.Popen([
    JAVA, "-Xmx1G", "-Xms1G", LOG4J, LOGDIR,
    "-cp", f"{KAFKA}\\libs\\*",
    "kafka.Kafka",
    f"{KAFKA}\\config\\server.properties"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if not wait_for_port(9092, 45):
    kill_proc(kf_p)
    kill_proc(zk_p)
    raise RuntimeError("Kafka broker failed to start on port 9092")
print("  Kafka broker running on port 9092.")
time.sleep(5)

try:
    print("[4] Creating topics (4 partitions)...")
    res = subprocess.run([sys.executable, "setup/setup_kafka.py", "--skip-start", "--partitions", "4"],
                         cwd=str(BASE_DIR), capture_output=True, text=True)
    print("  Setup output:\n", res.stdout)

    print("[5] Launching pipeline processes...")
    stage1 = subprocess.Popen([sys.executable, "stage1_clean_bloom/stage1.py", "--config", "config.yaml"], cwd=str(BASE_DIR))
    stage2 = subprocess.Popen([sys.executable, "stage2_stream_analytics/stage2.py", "--config", "config.yaml", "--group-id", "stage2-group", "--run-id", "DEMO"], cwd=str(BASE_DIR))
    stage3 = subprocess.Popen([sys.executable, "stage3_lsh/stage3.py", "--config", "config.yaml", "--run-id", "DEMO"], cwd=str(BASE_DIR))
    final  = subprocess.Popen([sys.executable, "final_consumer/final_consumer.py", "--config", "config.yaml"], cwd=str(BASE_DIR))
    prod   = subprocess.Popen([sys.executable, "producer/producer.py", "--config", "config.yaml", "--rate", "300", "--key", "pickup_zone", "--max-events", "25000"], cwd=str(BASE_DIR))

    print("[6] Running pipeline for 45s to establish steady state...")
    time.sleep(45)

    print("[7] Killing Stage 2 process (simulating worker crash)...")
    kill_proc(stage2)
    stage2 = None
    time.sleep(2)

    print("[8] Capturing Step 1 (lag immediately after kill)...")
    step1_file = RESULTS_DIR / "step1_lag_after_kill.txt"
    with open(step1_file, "w", encoding="utf-8") as f:
        subprocess.run([sys.executable, "demo_task5.py"], cwd=str(BASE_DIR), stdout=f, stderr=subprocess.STDOUT)
    print(f"  Saved {step1_file}")

    print("[9] Waiting 15s while Producer keeps streaming to taxi.clean...")
    time.sleep(15)

    print("[10] Capturing Step 2 (lag accumulated over 15s)...")
    step2_file = RESULTS_DIR / "step2_lag_growing.txt"
    with open(step2_file, "w", encoding="utf-8") as f:
        subprocess.run([sys.executable, "demo_task5.py"], cwd=str(BASE_DIR), stdout=f, stderr=subprocess.STDOUT)
    print(f"  Saved {step2_file}")

    print("[11] Restarting Stage 2 with same group_id (stage2-group)...")
    stage2 = subprocess.Popen([sys.executable, "stage2_stream_analytics/stage2.py", "--config", "config.yaml", "--group-id", "stage2-group", "--run-id", "DEMO"], cwd=str(BASE_DIR))

    print("[12] Waiting 75s (session timeout of crashed member + drain)...")
    time.sleep(75)

    print("[13] Capturing Step 3 (lag draining / caught up)...")
    step3_file = RESULTS_DIR / "step3_lag_draining.txt"
    with open(step3_file, "w", encoding="utf-8") as f:
        subprocess.run([sys.executable, "demo_task5.py"], cwd=str(BASE_DIR), stdout=f, stderr=subprocess.STDOUT)
    print(f"  Saved {step3_file}")

finally:
    print("[14] Cleaning up all processes...")
    for p in [prod, stage1, stage2, stage3, final]:
        if p:
            kill_proc(p)
    kill_proc(kf_p)
    kill_proc(zk_p)
    print("All processes stopped. Demo complete!")