import asyncio
import json
import requests
import os
from datetime import datetime, timezone
from db import get_db_connection
from psycopg2.extras import RealDictCursor

API_URL = "https://apim-ap-test.azure-api.net/plant-control/api/setpoint"
API_KEY = os.getenv("ALTEO_API_KEY")  #.env

CYCLE_TIME = 2


def get_latest_plant_data():
    """Lekéri a legfrissebb adatokat minden plant-hez"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT ON (plant_id)
            plant_id, measured_at,
            sum_active_power, cos_phi,
            available_power_min, available_power_max, reference_power
        FROM plant_data_term1
        ORDER BY plant_id, measured_at DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_last_heartbeat(pod_id):
    """Lekéri az utolsó heartbeat-et az adott POD-hoz"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT heartbeat
        FROM alteo_controls_inbox
        WHERE pod = %s
        ORDER BY received_at DESC
        LIMIT 1;
    """, (pod_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def store_alteo_response(pod, request_json, response_json, status_code):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO alteo_send_log (pod, request_json, response_json, status_code)
        VALUES (%s, %s, %s, %s);
    """, (
        pod,
        json.dumps(request_json, ensure_ascii=False),
        json.dumps(response_json, ensure_ascii=False),
        status_code
    ))
    conn.commit()
    cur.close()
    conn.close()


def update_heartbeat_inbox(pod, heartbeat, sum_setpoint, scheduled_reference):
    """Frissíti / beszúrja az új heartbeat értéket"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO alteo_controls_inbox (pod, heartbeat, sum_setpoint, scheduled_reference)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pod)
        DO UPDATE SET
          heartbeat = EXCLUDED.heartbeat,
          sum_setpoint = EXCLUDED.sum_setpoint,
          scheduled_reference = EXCLUDED.scheduled_reference,
          received_at = NOW();
    """, (pod, heartbeat, sum_setpoint, scheduled_reference))
    conn.commit()
    cur.close()
    conn.close()


def build_payload(pod, measurement, heartbeat_mirrored):
    measured_at = (
        measurement["measured_at"]
        .astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    return [
        {
            "pod": pod,
            "values": [
                {"measurement": "heartbeatMirrored", "measuredAt": measured_at, "value": heartbeat_mirrored, "quality": 1},
                {"measurement": "availablePowerMin", "measuredAt": measured_at, "value": measurement["available_power_min"], "quality": 1},
                {"measurement": "availablePowerMax", "measuredAt": measured_at, "value": measurement["available_power_max"], "quality": 1},
                {"measurement": "sumActivePower", "measuredAt": measured_at, "value": measurement["sum_active_power"], "quality": 1},
                {"measurement": "cosPhi", "measuredAt": measured_at, "value": measurement["cos_phi"], "quality": 1},
                {"measurement": "referencePower", "measuredAt": measured_at, "value": measurement["reference_power"], "quality": 1},
            ]
        }
    ]


async def send_to_alteo(pod, measurement):
    heartbeat_mirrored = get_last_heartbeat(pod) or 0
    payload = build_payload(pod, measurement, heartbeat_mirrored)

    headers = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": API_KEY,
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
                hb = controls[0].get("heartbeat")
                sp = controls[0].get("sumSetPoint")
                sr = controls[0].get("scheduledReference")
                update_heartbeat_inbox(pod, hb, sp, sr)
            print(f"[SEND] POD {pod} OK, status={status}")
        else:
            print(f"[SEND] POD {pod} FAILED, status={status}")

        store_alteo_response(pod, payload, response_data, status)

    except Exception as e:
        print(f"[ERR] POD {pod} – {type(e).__name__}: {e}")


async def main():
    print("[SENDER] Starting ALTEO sender loop...")
    while True:
        start = datetime.now()
        measurements = get_latest_plant_data()

        tasks = []
        for m in measurements:
            pod = m.get("pod_id") or f"plant_{m['plant_id']}"
            tasks.append(asyncio.create_task(send_to_alteo(pod, m)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = (datetime.now() - start).total_seconds()
        await asyncio.sleep(max(0, CYCLE_TIME - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
