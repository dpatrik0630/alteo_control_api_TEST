import time
import struct
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

from pyModbusTCP.client import ModbusClient
from db import get_db_connection
from breaker import should_skip, on_failure, on_success

TARGET_PERIOD = 2.0
print("[ESS] poll_ess_hithium started")


# ---------- Modbus helpers ----------

def read_uint16(client, addr):
    regs = client.read_input_registers(addr, 1)
    if not regs:
        raise Exception(f"Modbus read failed at {addr}")
    return regs[0]


def read_int16(client, addr):
    regs = client.read_input_registers(addr, 1)
    if not regs:
        raise Exception(f"Modbus read failed at {addr}")
    val = regs[0]
    return val - 0x10000 if val & 0x8000 else val


def read_float32(client, addr):
    regs = client.read_input_registers(addr, 2)
    if not regs or len(regs) < 2:
        raise Exception(f"Modbus float read failed at {addr}")
    raw = (regs[0] << 16) | regs[1]
    return struct.unpack(">f", raw.to_bytes(4, "big"))[0]


def avg(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def calculate_capacity(total_kwh, soc, min_soc=0, max_soc=100):
    current = total_kwh * soc / 100.0
    charge = total_kwh * max_soc / 100.0 - current
    discharge = current - total_kwh * min_soc / 100.0
    return max(charge, 0), max(discharge, 0)


# ---------- ESS polling ----------

def poll_ess_unit(ess, cur):
    print(f"[ESS] Polling ESS plant_id={ess['plant_id']} ip={ess['ip_address']}:{ess['port']}")

    client = ModbusClient(
        host=ess["ip_address"],
        port=ess["port"],
        auto_open=True,
        timeout=3
    )

    if not client.open():
        raise Exception("Modbus connection failed")

    # --- CORE VALUES ---
    soc = read_uint16(client, 1) / 10.0
    total_capacity = read_float32(client, 2)
    pcs_active_power = read_float32(client, 4)

    # --- BATTERY TEMPS ---
    batt_avg = avg([read_int16(client, 100 + i) / 10.0 for i in range(5)])
    batt_min = avg([read_int16(client, 200 + i) / 10.0 for i in range(5)])
    batt_max = avg([read_int16(client, 300 + i) / 10.0 for i in range(5)])

    # --- CONTAINER TEMPS ---
    cont_avg = avg([read_int16(client, 400 + i) / 10.0 for i in range(5)])
    cont_min = min([read_int16(client, 400 + i) / 10.0 for i in range(5)])
    cont_max = max([read_int16(client, 400 + i) / 10.0 for i in range(5)])

    charge_kwh, discharge_kwh = calculate_capacity(
        total_capacity,
        soc
    )

    now = datetime.now(timezone.utc)

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
            ess["plant_id"],
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
    client.close()


# ---------- MAIN LOOP ----------

def main():
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT plant_id, ip_address, port
        FROM ess_units
        WHERE active = true
    """)
    ess_units = cur.fetchall()

    print(f"[ESS] Found {len(ess_units)} active ESS units")

    ess_units = [
        {
            "plant_id": e[0],
            "ip_address": e[1],
            "port": e[2]
        }
        for e in ess_units
    ]

    while True:
        cycle_start = time.monotonic()

        for ess in ess_units:
            plant_id = ess["plant_id"]

            if should_skip(plant_id):
                continue

            try:
                poll_ess_unit(ess, cur)
                on_success(plant_id)
            except Exception as e:
                print(f"[ESS][ERROR] Plant {plant_id} → {e}")
                on_failure(plant_id)

        elapsed = time.monotonic() - cycle_start
        sleep_time = TARGET_PERIOD - elapsed

        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
