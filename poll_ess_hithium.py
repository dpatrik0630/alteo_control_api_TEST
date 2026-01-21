# poll_ess_hithium.py

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from pyModbusTCP.client import ModbusClient

from db import get_db_connection
from breaker import should_skip, on_success, on_failure


POLL_INTERVAL = 2
MODBUS_TIMEOUT = 2
HITHIUM_MAP_PATH = Path("register_maps/ess/hithium.json")


# ---------- helpers ----------

def load_hithium_map():
    with open(HITHIUM_MAP_PATH, "r") as f:
        return json.load(f)


def aggregate(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None, None, None
    return sum(vals) / len(vals), min(vals), max(vals)


def calculate_capacity(total_kwh, soc, min_soc, max_soc):
    current = total_kwh * soc / 100
    charge = total_kwh * max_soc / 100 - current
    discharge = current - total_kwh * min_soc / 100
    return max(charge, 0), max(discharge, 0)


def read_registers(client, cfg):
    if cfg.get("fc") == 3:
        regs = client.read_holding_registers(cfg["address"], cfg["quantity"])
    else:
        regs = client.read_input_registers(cfg["address"], cfg["quantity"])

    if regs is None:
        raise Exception(f"Modbus read failed at address {cfg['address']}")

    print(f"[ESS] {cfg['address']} → {regs}")

    values = []
    for r in regs:
        v = r
        if cfg.get("signed") and v >= 32768:
            v -= 65536
        if "gain" in cfg:
            v = v / cfg["gain"]
        values.append(v)

    return values


# ---------- ESS polling (sync) ----------

def poll_single_ess(ess, hithium_map):
    client = ModbusClient(
        host=ess["ip_address"],
        port=ess["port"],
        unit_id=ess["slave_id"],
        auto_open=True,
        auto_close=True,
        timeout=MODBUS_TIMEOUT
    )

    raw = {}
    for key, cfg in hithium_map.items():
        try:
            raw[key] = read_registers(client, cfg)
        except Exception as e:
            print(f"[ESS] {ess['plant_id']} register {key} failed: {e}")
            raw[key] = []

    for k in ("totalCapacity", "averageCurrentSOC", "allowedMinSOC", "allowedMaxSOC"):
        if not raw.get(k) or not raw[k]:
            raise Exception(f"[ESS] Missing required value: {k}")


    avg_batt, min_batt, max_batt = aggregate(raw["averageBatterycellTemp"])
    avg_cont, min_cont, max_cont = aggregate(raw["averageContainerInsideTemp"])

    charge_kwh, discharge_kwh = calculate_capacity(
        raw["totalCapacity"][0],
        raw["averageCurrentSOC"][0],
        raw["allowedMinSOC"][0],
        raw["allowedMaxSOC"][0]
    )

    return {
        "avg_batt": avg_batt,
        "min_batt": min_batt,
        "max_batt": max_batt,
        "avg_cont": avg_cont,
        "min_cont": min_cont,
        "max_cont": max_cont,
        "soc": raw["averageCurrentSOC"][0],
        "min_soc": raw["allowedMinSOC"][0],
        "max_soc": raw["allowedMaxSOC"][0],
        "charge_kwh": charge_kwh,
        "discharge_kwh": discharge_kwh,
    }


# ---------- async wrapper ----------

async def poll_ess(ess, hithium_map):
    '''if should_skip(ess["plant_id"]):
        return'''
    print(f"[ESS] Polling ESS for plant {ess['plant_id']} at {ess['ip_address']}:{ess['port']}")

    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None,
            poll_single_ess,
            ess,
            hithium_map
        )

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
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
                average_current_soc,
                allowed_min_soc,
                allowed_max_soc
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ess["plant_id"],
            datetime.now(timezone.utc),
            data["avg_batt"],
            data["min_batt"],
            data["max_batt"],
            data["avg_cont"],
            data["min_cont"],
            data["max_cont"],
            data["charge_kwh"],
            data["discharge_kwh"],
            data["soc"],
            data["min_soc"],
            data["max_soc"],
        ))
        conn.commit()
        cur.close()
        conn.close()

        on_success(ess["plant_id"])

    except Exception as e:
        print(f"[ESS] Plant {ess['plant_id']} error: {e}")
        on_failure(ess["plant_id"])


# ---------- main loop ----------

async def main():
    hithium_map = load_hithium_map()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            e.plant_id,
            e.ip_address,
            e.port,
            e.slave_id
        FROM ess_units e
        WHERE e.active = true
    """)
    ess_units = [
        {
            "plant_id": r[0],
            "ip_address": r[1],
            "port": r[2],
            "slave_id": r[3],
        }
        for r in cur.fetchall()
    ]
    cur.close()
    conn.close()

    print(f"[ESS] Polling {len(ess_units)} ESS units")

    while True:
        tasks = [poll_ess(e, hithium_map) for e in ess_units]
        await asyncio.gather(*tasks)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
