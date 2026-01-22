import time
import struct
import asyncio
from datetime import datetime, timezone

from pyModbusTCP.client import ModbusClient
from db import get_db_connection
from breaker import should_skip, on_failure, on_success

TARGET_PERIOD = 1.0

print("[ESS] poll_ess_hithium OPTIMIZED started")


# ---------- HELPERS ----------

def avg(values):
    return sum(values) / len(values) if values else None


def regs_to_float32(regs):
    raw = (regs[0] << 16) | regs[1]
    return struct.unpack(">f", raw.to_bytes(4, "big"))[0]


def calculate_capacity(total_kwh, soc, min_soc=0, max_soc=100):
    current = total_kwh * soc / 100.0
    charge = total_kwh * max_soc / 100.0 - current
    discharge = current - total_kwh * min_soc / 100.0
    return max(charge, 0), max(discharge, 0)


# ---------- SYNC ESS POLL ----------

def poll_ess_unit(ess, cur):
    plant_id = ess["plant_id"]
    print(f"[ESS] Polling plant_id={plant_id}")

    client = ModbusClient(
        host=ess["ip_address"],
        port=ess["port"],
        auto_open=True,
        timeout=1.0
    )

    if not client.open():
        raise Exception("Modbus connection failed")

    try:
        # --- SOC ---
        soc_raw = client.read_input_registers(1, 1)
        if not soc_raw:
            raise Exception("SOC read failed")
        soc = soc_raw[0] / 10.0

        # --- TOTAL CAPACITY (float32) ---
        cap_regs = client.read_input_registers(2, 2)
        if not cap_regs:
            raise Exception("Capacity read failed")
        total_capacity = regs_to_float32(cap_regs)

        # --- BATTERY TEMPERATURES ---
        batt_avg_vals = client.read_input_registers(100, 5)
        batt_min_vals = client.read_input_registers(200, 5)
        batt_max_vals = client.read_input_registers(300, 5)

        if not batt_avg_vals or not batt_min_vals or not batt_max_vals:
            raise Exception("Battery temp read failed")

        batt_avg = avg([v / 10.0 for v in batt_avg_vals])
        batt_min = avg([v / 10.0 for v in batt_min_vals])
        batt_max = avg([v / 10.0 for v in batt_max_vals])

        # --- CONTAINER TEMPERATURES ---
        cont_vals = client.read_input_registers(400, 5)
        if not cont_vals:
            raise Exception("Container temp read failed")

        cont_vals = [v / 10.0 for v in cont_vals]
        cont_avg = avg(cont_vals)
        cont_min = min(cont_vals)
        cont_max = max(cont_vals)

        # --- CAPACITY CALC ---
        charge_kwh, discharge_kwh = calculate_capacity(total_capacity, soc)

        now = datetime.now(timezone.utc)

        # --- DB INSERT ---
        cur.execute(
            """
            INSERT INTO ess_data_term1 (
                plant_id,
                measured_at,
                avg_batt_temp,
                min_batt_temp,
                max_batt_temp,
                avg_container_temp,
                min_container_temp,
                max_container_temp,
                available_capacity_charge,
                available_capacity_discharge,
                average_current_soc
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                plant_id,
                now,
                batt_avg,
                batt_min,
                batt_max,
                cont_avg,
                cont_min,
                cont_max,
                charge_kwh,
                discharge_kwh,
                soc
            )
        )

    finally:
        client.close()


# ---------- ASYNC WRAPPER ----------

async def poll_single_ess_async(ess, cur):
    plant_id = ess["plant_id"]

    if should_skip(plant_id):
        return

    try:
        await asyncio.to_thread(poll_ess_unit, ess, cur)
        on_success(plant_id)
    except Exception as e:
        print(f"[ESS][ERROR] Plant {plant_id} → {e}")
        on_failure(plant_id)


# ---------- ASYNC MAIN LOOP ----------

async def main_async():
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT plant_id, ip_address, port
        FROM ess_units
        WHERE active = true
    """)

    ess_units = [
        {"plant_id": r[0], "ip_address": r[1], "port": r[2]}
        for r in cur.fetchall()
    ]

    print(f"[ESS] Found {len(ess_units)} active ESS units")

    while True:
        cycle_start = time.monotonic()

        tasks = [
            poll_single_ess_async(ess, cur)
            for ess in ess_units
        ]

        await asyncio.gather(*tasks)

        elapsed = time.monotonic() - cycle_start
        sleep_time = TARGET_PERIOD - elapsed

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


# ---------- ENTRYPOINT ----------

if __name__ == "__main__":
    asyncio.run(main_async())
