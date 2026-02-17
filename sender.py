import asyncio
import json
import os
import time
import requests
from datetime import timezone

from psycopg2.extras import RealDictCursor

from db import get_db_connection


API_URL = "https://apim-ap-test.azure-api.net/plant-control/api/setpoint"
#API_URL = "http://100.107.240.37:9000/setpoint"
CYCLE_TIME = 2

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

    cur.execute("""
        SELECT DISTINCT ON (plant_id)
            plant_id,
            pod_id,
            measured_at,
            sum_active_power,
            cos_phi,
            available_power_min,
            available_power_max,
            reference_power
        FROM plant_data_term1
        ORDER BY plant_id, measured_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_latest_ess_data(plant_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT *
        FROM ess_data_term1
        WHERE plant_id = %s
        ORDER BY measured_at DESC
        LIMIT 1
    """, (plant_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_latest_environment_temp(plant_id):
    conn = get_db_connection()
    cur = conn.cursor()

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
    cur.close()
    conn.close()

    return row[0] if row else None

def get_24h_env_temp_avg_min_max(plant_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            AVG(e.temperature),
            MIN(e.temperature),
            MAX(e.temperature)
        FROM environment_data_term1 e
        JOIN plant_environment_sensors pes
          ON pes.sensor_id = e.sensor_id
        WHERE pes.plant_id = %s
          AND e.measured_at >= NOW() - INTERVAL '5 minutes'
    """, (plant_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if row and row[0] is not None:
        return row[0], row[1], row[2]

    return None, None, None


def get_24h_avg_min_max(plant_id, column):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT
            AVG({column}),
            MIN({column}),
            MAX({column})
        FROM ess_data_term1
        WHERE plant_id = %s
          AND measured_at >= NOW() - INTERVAL '5 minutes'
    """, (plant_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()

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
    cur = conn.cursor()

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
    cur.close()
    conn.close()


def get_last_heartbeat(pod):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT heartbeat
        FROM alteo_control_inbox
        WHERE pod = %s
        ORDER BY received_at DESC
        LIMIT 1
    """, (pod,))

    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def store_alteo_response(pod, payload, response, status):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO alteo_send_log (
            pod,
            request_payload,
            response_payload,
            http_status
        ) VALUES (%s,%s,%s,%s)
    """, (
        pod,
        json.dumps(payload),
        json.dumps(response),
        status
    ))

    conn.commit()
    cur.close()
    conn.close()


def update_heartbeat_inbox(pod, heartbeat, sum_setpoint, scheduled_reference):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO alteo_control_inbox (
            pod,
            heartbeat,
            sum_setpoint,
            scheduled_reference
        ) VALUES (%s,%s,%s,%s)
    """, (
        pod,
        heartbeat,
        sum_setpoint,
        scheduled_reference
    ))

    conn.commit()
    cur.close()
    conn.close()


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
        measurement["measured_at"]
        .astimezone(timezone.utc)
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

async def send_once(measurement):

    def safe(v):
        return v if v is not None else 0.0
    
    pod = measurement["pod_id"]
    heartbeat = get_last_heartbeat(pod)

    if heartbeat is None or heartbeat <= 0:
        print(f"[SENDER] POD {pod}: no valid heartbeat, skipping")
        return

    ess_data = get_latest_ess_data(measurement["plant_id"])
    env_temp = get_latest_environment_temp(measurement["plant_id"])

    env_avg_24h = env_min_24h = env_max_24h = None

    env_avg_24h, env_min_24h, env_max_24h = get_24h_env_temp_avg_min_max(
        measurement["plant_id"]
    )


    batt_avg_24h = batt_min_24h = batt_max_24h = None
    cont_avg_24h = cont_min_24h = cont_max_24h = None

    if ess_data:
        batt_avg_24h, batt_min_24h, batt_max_24h = get_24h_avg_min_max(
            measurement["plant_id"],
            "avg_batt_temp"
        )

        cont_avg_24h, cont_min_24h, cont_max_24h = get_24h_avg_min_max(
            measurement["plant_id"],
            "avg_container_temp"
        )

    payload = build_payload(
        measurement,
        ess_data,
        heartbeat,
        env_temp,
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

# ---- STORE 24h ALTEO STATS INTO ess_data_term1 ----
    if ess_data and (
        batt_avg_24h is not None
        or cont_avg_24h is not None
    ):
        update_ess_24h_stats_by_id(
            ess_data["id"],
            safe(batt_avg_24h),
            safe(batt_min_24h),
            safe(batt_max_24h),
            safe(cont_avg_24h),
            safe(cont_min_24h),
            safe(cont_max_24h)
        )


    headers = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": get_api_key()
    }

    try:
        resp = requests.post(API_URL, headers=headers, json=payload, timeout=5)
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
            print(f"[SEND] POD {pod} OK")
        else:
            print(f"[SEND] POD {pod} FAILED ({status})")

        store_alteo_response(pod, payload, response_data, status)

    except Exception as e:
        print(f"[ERROR] POD {pod}: {e}")


async def main():
    print("[SENDER] Started")

    while True:
        measurements = get_latest_plant_data()
        tasks = [send_once(m) for m in measurements]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(CYCLE_TIME)


if __name__ == "__main__":
    asyncio.run(main())
