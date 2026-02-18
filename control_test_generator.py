import time
import threading
from datetime import datetime
from db import get_db_connection

# ==========================================
# CONFIG
# ==========================================

STEP_MINUTES = 5

TEST_SEQUENCE = [
    50,
    100,
    200,
    300
]

WRITE_INTERVAL = 30  # mp

# ==========================================
# DB helpers
# ==========================================

def get_active_pods():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT pod_id
        FROM plants
        WHERE alteo_api_control = TRUE
    """)

    pods = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return pods


def write_test_setpoint(pod, setpoint):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO alteo_controls_inbox (
            pod,
            heartbeat,
            sum_setpoint,
            scheduled_reference,
            received_at
        )
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (pod)
        DO UPDATE SET
            heartbeat = EXCLUDED.heartbeat,
            sum_setpoint = EXCLUDED.sum_setpoint,
            scheduled_reference = EXCLUDED.scheduled_reference,
            received_at = NOW();
    """, (
        pod,
        1,              # dummy heartbeat
        setpoint,
        setpoint        # scheduled_reference is optional
    ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[TEST CTRL] POD {pod} → forced {setpoint} kW")


# ==========================================
# MAIN LOOP
# ==========================================

def main():
    print("[TEST CTRL] Starting control test generator")

    pods = get_active_pods()
    print(f"[TEST CTRL] Found {len(pods)} pods")

    start_time = time.time()

    while True:
        elapsed_minutes = (time.time() - start_time) / 60
        step_index = int(elapsed_minutes // STEP_MINUTES)

        if step_index >= len(TEST_SEQUENCE):
            step_index = len(TEST_SEQUENCE) - 1

        setpoint = TEST_SEQUENCE[step_index]

        for pod in pods:
            write_test_setpoint(pod, setpoint)

        time.sleep(WRITE_INTERVAL)


if __name__ == "__main__":
    main()
