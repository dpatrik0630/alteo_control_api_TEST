import asyncio
import json
import os
import requests
from datetime import datetime
from db import get_db_connection

API_URL = "https://apim-ap-test.azure-api.net/plant-control/api/setpoint"
API_KEY = os.getenv("ALTEO_API_KEY")
CHECK_INTERVAL = 30  # mp-ként próbálja újra, ha nem volt frissítés

def update_heartbeat_inbox(pod, heartbeat, sum_setpoint, scheduled_reference, usesetpoint):
    """Frissíti vagy létrehozza a heartbeat rekordot"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO alteo_controls_inbox (pod, heartbeat, sum_setpoint, scheduled_reference, usesetpoint)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (pod)
        DO UPDATE SET
          heartbeat = EXCLUDED.heartbeat,
          sum_setpoint = EXCLUDED.sum_setpoint,
          scheduled_reference = EXCLUDED.scheduled_reference,
          usesetpoint = EXCLUDED.usesetpoint,
          received_at = NOW();
    """, (pod, heartbeat, sum_setpoint, scheduled_reference, usesetpoint))
    conn.commit()
    cur.close()
    conn.close()


async def fetch_initial_heartbeat():
    """Üres [] hívás az ALTEO API-ra a kezdeti heartbeat megszerzéséhez"""
    headers = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": API_KEY
    }
    payload = []

    try:
        print("[CTRL_FETCHER] Sending initial [] request to ALTEO...")
        resp = requests.post(API_URL, headers=headers, json=payload, timeout=5)
        if resp.status_code != 200:
            print(f"[CTRL_FETCHER] Bad response: {resp.status_code}")
            return

        data = resp.json()
        controls = data.get("controls", [])
        if not controls:
            print("[CTRL_FETCHER] No controls received yet.")
            return

        for ctrl in controls:
            pod = ctrl.get("pod")
            hb = ctrl.get("heartbeat")
            sp = ctrl.get("sumSetPoint")
            sr = ctrl.get("scheduledReference")
            usp = ctrl.get("useSetPoint", 0)
            update_heartbeat_inbox(pod, hb, sp, sr, usp)
            print(f"[CTRL_FETCHER] Initial heartbeat stored for {pod}: {hb}")

    except Exception as e:
        print(f"[CTRL_FETCHER] Error: {e}")


async def main():
    print("[CTRL_FETCHER] Starting heartbeat fetcher loop...")
    while True:
        await fetch_initial_heartbeat()
        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
