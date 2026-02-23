import asyncio
import json
import os
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from db import get_db_connection, release_db_connection
from psycopg2.extras import RealDictCursor


USE_HEARTBEAT = False

API_URL = "https://ams-partner-api.azure-api.net/plant-control/api/setpoint"

CYCLE_TIME = 2
MAX_WORKERS = 8

executor = ThreadPoolExecutor(max_workers=8)

def get_api_key():
    key = os.getenv("ALTEO_API_KEY")
    if not key:
        raise RuntimeError("ALTEO_API_KEY environment variable is not set")
    return key

# -------------------------------------------------
# DB helpers
# -------------------------------------------------

def get_latest_plant_data():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT DISTINCT ON (p.id)
                p.id AS plant_id,
                pd.pod_id,
                pd.measured_at,
                pd.sum_active_power,
                pd.cos_phi,
                pd.available_power_min,
                pd.available_power_max,
                pd.reference_power
            FROM plants p
            JOIN plant_data_term1 pd
                ON pd.plant_id = p.id
            WHERE p.alteo_api_control = true
            ORDER BY p.id, pd.measured_at DESC
        """)

        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        release_db_connection(conn)
    


def get_latest_ess_data(plant_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT *
            FROM ess_data_term1
            WHERE plant_id = %s
            ORDER BY measured_at DESC
            LIMIT 1
        """, (plant_id,))

        row = cur.fetchone()
        return row
    finally:
        cur.close()
        release_db_connection(conn)

def get_latest_environment_temp(plant_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT e.temperature
            FROM plant_environment_sensors pes
            JOIN environment_data_term1 e
            ON e.sensor_id = pes.sensor_id
            WHERE pes.plant_id = %s
            ORDER BY e.measured_at DESC
            LIMIT 1
        """, (plant_id,))

        row = cur.fetchone()
    finally:
        cur.close()
        release_db_connection(conn)

    return row["temperature"] if row else None

def get_24h_env_temp_avg_min_max(plant_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                AVG(e.temperature) AS avg_temp,
                MIN(e.temperature) AS min_temp,
                MAX(e.temperature) AS max_temp
            FROM environment_data_term1 e
            JOIN plant_environment_sensors pes
            ON pes.sensor_id = e.sensor_id
            WHERE pes.plant_id = %s
            AND e.measured_at >= NOW() - INTERVAL '5 minutes'
        """, (plant_id,))

        row = cur.fetchone()
    finally:
        cur.close()
        release_db_connection(conn)

    if row and row["avg_temp"] is not None:
        return row["avg_temp"], row["min_temp"], row["max_temp"]

    return None, None, None


def get_24h_avg_min_max(plant_id, column):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f"""
            SELECT
                AVG(column) AS avg_val,
                MIN(column) AS min_val,
                MAX(column) AS max_val
            FROM ess_data_term1
            WHERE plant_id = %s
            AND measured_at >= NOW() - INTERVAL '5 minutes'
        """, (plant_id,))

        row = cur.fetchone()
    finally:
        cur.close()
        release_db_connection(conn)

    if row and row[0] is not None:
        return row[0], row[1], row[2]

    return None, None, None

def update_ess_24h_stats_by_id(
    ess_row_id,
    batt_avg,
    batt_min,
    batt_max,
    cont_avg,
    cont_min,
    cont_max
):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute(
            """
            UPDATE ess_data_term1
            SET
                batt_temp_24h_avg = %s,
                batt_temp_24h_min = %s,
                batt_temp_24h_max = %s,
                container_temp_24h_avg = %s,
                container_temp_24h_min = %s,
                container_temp_24h_max = %s
            WHERE id = %s
            """,
            (
                batt_avg,
                batt_min,
                batt_max,
                cont_avg,
                cont_min,
                cont_max,
                ess_row_id
            )
        )

        conn.commit()
    finally:
        cur.close()
        release_db_connection(conn)


def get_last_heartbeat(pod):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT heartbeat
            FROM alteo_controls_inbox
            WHERE pod = %s
            ORDER BY received_at DESC
            LIMIT 1
        """, (pod,))

        row = cur.fetchone()
    finally:
        cur.close()
        release_db_connection(conn)
    return row["heartbeat"] if row else None


def store_alteo_response(pod, payload, response, status):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            INSERT INTO alteo_send_log (
                pod,
                request_json,
                response_json,
                status_code
            ) VALUES (%s,%s,%s,%s)
        """, (
            pod,
            json.dumps(payload),
            json.dumps(response),
            status
        ))

        conn.commit()
    finally:
        cur.close()
        release_db_connection(conn)


def update_heartbeat_inbox(pod, heartbeat, sum_setpoint, scheduled_reference):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO alteo_controls_inbox (
                pod,
                heartbeat,
                sum_setpoint,
                scheduled_reference
            )
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (pod)
            DO UPDATE SET
                heartbeat = EXCLUDED.heartbeat,
                sum_setpoint = EXCLUDED.sum_setpoint,
                scheduled_reference = EXCLUDED.scheduled_reference,
                received_at = NOW()
            WHERE alteo_controls_inbox.heartbeat IS NULL
            OR alteo_controls_inbox.heartbeat < EXCLUDED.heartbeat;
        """, (
            pod,
            heartbeat,
            sum_setpoint,
            scheduled_reference
        ))
        conn.commit()
    finally:
        cur.close()
        release_db_connection(conn)


# -------------------------------------------------
# Payload builder
# -------------------------------------------------

def build_payload(
    measurement, ess_data, heartbeat_mirrored,
    env_temp,
    batt_avg_24h, batt_min_24h, batt_max_24h,
    cont_avg_24h, cont_min_24h, cont_max_24h,
    env_avg_24h, env_min_24h, env_max_24h
):
    pod = measurement["pod_id"]
    measured_at = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    values = [
        {
            "measurement": "heartbeatMirrored",
            "measuredAt": measured_at,
            "value": heartbeat_mirrored,
            "quality": 1
        },
        {
            "measurement": "availablePowerMin",
            "measuredAt": measured_at,
            "value": measurement["available_power_min"],
            "quality": 1
        },
        {
            "measurement": "availablePowerMax",
            "measuredAt": measured_at,
            "value": measurement["available_power_max"],
            "quality": 1
        },
        {
            "measurement": "sumActivePower",
            "measuredAt": measured_at,
            "value": measurement["sum_active_power"],
            "quality": 1
        },
        {
            "measurement": "cosPhi",
            "measuredAt": measured_at,
            "value": measurement["cos_phi"],
            "quality": 1
        },
        {
            "measurement": "referencePower",
            "measuredAt": measured_at,
            "value": measurement["reference_power"],
            "quality": 1
        }
    ]

    # -------- ESS EXTENSION --------
    if ess_data:
        values.extend([
        {"measurement": "availableCapacityCharge", "measuredAt": measured_at, "value": ess_data["available_capacity_charge"], "quality": 1},
        {"measurement": "availableCapacityDischarge", "measuredAt": measured_at, "value": ess_data["available_capacity_discharge"], "quality": 1},

        {"measurement": "averageBatterycellTemp", "measuredAt": measured_at, "value": batt_avg_24h, "quality": 1},
        {"measurement": "averageBatterycellTempMIN", "measuredAt": measured_at, "value": batt_min_24h, "quality": 1},
        {"measurement": "averageBatterycellTempMAX", "measuredAt": measured_at, "value": batt_max_24h, "quality": 1},

        {"measurement": "averageContainerInsideTemp", "measuredAt": measured_at, "value": cont_avg_24h, "quality": 1},
        {"measurement": "averageContainerInsideTempMIN", "measuredAt": measured_at, "value": cont_min_24h, "quality": 1},
        {"measurement": "averageContainerInsideTempMAX", "measuredAt": measured_at, "value": cont_max_24h, "quality": 1},

        {"measurement": "averageCurrentSOC", "measuredAt": measured_at, "value": ess_data["average_current_soc"], "quality": 1},
        {"measurement": "allowedMinSOC", "measuredAt": measured_at, "value": ess_data["allowed_min_soc"], "quality": 1},
        {"measurement": "allowedMaxSOC", "measuredAt": measured_at, "value": ess_data["allowed_max_soc"], "quality": 1},
    ])

    if env_avg_24h is not None:
        values.extend([
            {
                "measurement": "averageEnvironmentTemp",
                "measuredAt": measured_at,
                "value": env_avg_24h,
                "quality": 1
            },
            {
                "measurement": "averageEnvironmentTempMIN",
                "measuredAt": measured_at,
                "value": env_min_24h,
                "quality": 1
            },
            {
                "measurement": "averageEnvironmentTempMAX",
                "measuredAt": measured_at,
                "value": env_max_24h,
                "quality": 1
            }
        ])  

    return [{"pod": pod, "values": values}]


# -------------------------------------------------
# Sender loop
# -------------------------------------------------

def send_sync(measurement):

    pod = measurement["pod_id"]

    try:
        # ---- HEARTBEAT ----
        heartbeat = get_last_heartbeat(pod) or 1

        # ---- ESS ----
        ess_data = get_latest_ess_data(measurement["plant_id"])

        # ---- ENV ----
        env_avg_24h, env_min_24h, env_max_24h = \
            get_24h_env_temp_avg_min_max(measurement["plant_id"])

        batt_avg_24h = batt_min_24h = batt_max_24h = None
        cont_avg_24h = cont_min_24h = cont_max_24h = None

        if ess_data:
            batt_avg_24h, batt_min_24h, batt_max_24h = \
                get_24h_avg_min_max(
                    measurement["plant_id"],
                    "avg_batt_temp"
                )

            cont_avg_24h, cont_min_24h, cont_max_24h = \
                get_24h_avg_min_max(
                    measurement["plant_id"],
                    "avg_container_temp"
                )

        payload = build_payload(
            measurement,
            ess_data,
            heartbeat,
            None,
            batt_avg_24h,
            batt_min_24h,
            batt_max_24h,
            cont_avg_24h,
            cont_min_24h,
            cont_max_24h,
            env_avg_24h,
            env_min_24h,
            env_max_24h
        )

        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": get_api_key()
        }

        resp = requests.post(
            API_URL,
            headers=headers,
            json=payload,
            timeout=5
        )

        status = resp.status_code

        try:
            response_data = resp.json()
        except Exception:
            response_data = {"raw_text": resp.text}

        if status == 200:
            controls = response_data.get("controls", [])
            if controls:
                c = controls[0]
                update_heartbeat_inbox(
                    pod,
                    c.get("heartbeat"),
                    c.get("sumSetPoint"),
                    c.get("scheduledReference")
                )

        store_alteo_response(
            pod,
            payload,
            response_data,
            status
        )

    except Exception as e:
        print(f"[SENDER ERROR] {pod}: {e}")


async def main():
    print("[SENDER] High performance mode")

    loop = asyncio.get_running_loop()

    while True:
        start = time.monotonic()

        measurements = await loop.run_in_executor(
            executor,
            get_latest_plant_data
        )

        tasks = [
            loop.run_in_executor(executor, send_sync, m)
            for m in measurements
        ]

        await asyncio.gather(*tasks)

        elapsed = time.monotonic() - start
        sleep_time = max(0, CYCLE_TIME - elapsed)

        print(f"[SENDER] Cycle time: {elapsed:.2f}s")

        await asyncio.sleep(sleep_time)



if __name__ == "__main__":
    asyncio.run(main())
