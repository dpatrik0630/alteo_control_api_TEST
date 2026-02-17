import asyncio
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from pyModbusTCP.client import ModbusClient
from psycopg2.extras import execute_values

from db import get_db_connection
from breaker import should_skip, on_success, on_failure


# =========================================================
# CONFIG
# =========================================================

CYCLE_TIME = 2.0
MAX_PARALLEL_POLLS = 10


# =========================================================
# LOAD REGISTER MAP
# =========================================================

def load_register_map(device_type: str, manufacturer: str):
    base = Path(__file__).parent / "register_maps"

    if device_type == "PCC_meter":
        path = base / "PCC_meter" / f"{manufacturer.lower()}.json"
    else:
        raise ValueError(f"Unsupported device type: {device_type}")

    if not path.exists():
        raise FileNotFoundError(f"Missing register map: {path}")

    with open(path, "r") as f:
        return json.load(f)


# =========================================================
# CONVERSIONS
# =========================================================

def convert_registers_to_scaled_value(registers, gain, signed=True):
    if not registers:
        return None

    if len(registers) == 1:
        raw = registers[0]
        if signed and raw & 0x8000:
            raw -= 0x10000

    elif len(registers) == 2:
        raw = (registers[0] << 16) | registers[1]
        if signed and raw & 0x80000000:
            raw -= 0x100000000
    else:
        return None

    return raw / gain


def normalize_cosphi(raw_cosphi: float, manufacturer: str):
    if raw_cosphi is None:
        return None

    # Fronius PF → cosφ (előjel nélkül)
    if manufacturer.lower() == "fronius":
        cosphi = abs(raw_cosphi)

    # Huawei már cosφ (-1…1)
    else:
        cosphi = raw_cosphi

    # Biztonsági clamp
    return max(-1.0, min(1.0, cosphi))



# =========================================================
# MODBUS – SYNC (THREAD)
# =========================================================

def read_cos_phi(client, manufacturer, meta):
    if manufacturer.lower() == "fronius":
        regs = client.read_holding_registers(40092 - 1, 2)
        if not regs:
            return None

        pf_raw = regs[0] if regs[0] < 0x8000 else regs[0] - 0x10000
        pf_sf = regs[1] if regs[1] < 0x8000 else regs[1] - 0x10000
        return pf_raw * (10 ** pf_sf)

    regs = client.read_holding_registers(meta["address"], meta["quantity"])
    return convert_registers_to_scaled_value(
        regs,
        meta["gain"],
        meta.get("signed", True)
    )


def poll_device_sync(ip, port, slave_id, manufacturer, register_map):
    client = ModbusClient(
        host=ip,
        port=port,
        unit_id=slave_id,
        auto_open=True,
        auto_close=True,
        timeout=1.0
    )

    data = {}

    sap_meta = register_map["sum_active_power"]
    sap_regs = client.read_holding_registers(
        sap_meta["address"], sap_meta["quantity"]
    )
    data["sum_active_power"] = convert_registers_to_scaled_value(
        sap_regs,
        sap_meta["gain"],
        sap_meta.get("signed", True)
    )

    cosphi_meta = register_map["cos_phi"]
    data["cos_phi"] = read_cos_phi(client, manufacturer, cosphi_meta)

    return data


# =========================================================
# DB HELPERS
# =========================================================

async def get_plants():
    loop = asyncio.get_running_loop()

    def _fetch():
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                id,
                pod_id,
                ip_address,
                port,
                meter_slave_id,
                logger_manufacturer
            FROM plants
            WHERE alteo_api_control = TRUE;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    return await loop.run_in_executor(None, _fetch)


async def store_term1_data(records):
    if not records:
        return

    loop = asyncio.get_running_loop()

    def _insert():
        conn = get_db_connection()
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO plant_data_term1 (
                plant_id,
                pod_id,
                measured_at,
                sum_active_power,
                cos_phi,
                available_power_min,
                available_power_max,
                reference_power,
                ghi,
                panel_temp
            ) VALUES %s
            ON CONFLICT (plant_id, measured_at) DO NOTHING
        """, [
            (
                r["plant_id"],
                r["pod_id"],
                r["timestamp"],
                r["sum_active_power"],
                r["cos_phi"],
                r["available_power_min"],
                r["available_power_max"],
                r["reference_power"],
                r["ghi"],
                r["panel_temp"]
            ) for r in records
        ])
        conn.commit()
        cur.close()
        conn.close()

    await loop.run_in_executor(None, _insert)


# =========================================================
# COLLECTOR
# =========================================================

SEM = asyncio.Semaphore(MAX_PARALLEL_POLLS)

async def collect_plant_data(plant):
    pid = plant["id"]

    if should_skip(pid):
        return None

    async with SEM:
        try:
            logger_data = await asyncio.to_thread(
                poll_device_sync,
                plant["ip"],
                plant["port"],
                plant["meter_slave_id"],
                plant["manufacturer"],
                plant["register_map"]
            )

            cosphi = normalize_cosphi(
                logger_data.get("cos_phi"),
                plant["manufacturer"]
            )

            record = {
                "plant_id": pid,
                "pod_id": plant["pod_id"],
                "timestamp": datetime.now(timezone.utc).replace(microsecond=0),
                "sum_active_power": logger_data.get("sum_active_power"),
                "cos_phi": cosphi,
                "available_power_min": 0.0,
                "available_power_max": abs(logger_data["sum_active_power"])
                    if logger_data.get("sum_active_power") is not None else None,
                "reference_power": abs(logger_data["sum_active_power"])
                    if logger_data.get("sum_active_power") is not None else None,
                "ghi": None,
                "panel_temp": None,
            }

            on_success(pid)
            return record

        except Exception as e:
            print(f"[ERR] Plant {pid}: {e}")
            on_failure(pid)
            return None


# =========================================================
# MAIN LOOP
# =========================================================

async def main():
    rows = await get_plants()

    plants = []
    for r in rows:
        plants.append({
            "id": r[0],
            "pod_id": r[1],
            "ip": r[2],
            "port": r[3],
            "meter_slave_id": r[4],
            "manufacturer": r[5],
            "register_map": load_register_map("PCC_meter", r[5]),
        })

    print(f"[ALTEO_TERM1] Started for {len(plants)} plants")

    while True:
        start = datetime.now()

        tasks = [
            asyncio.create_task(collect_plant_data(p))
            for p in plants
        ]

        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r]

        if valid:
            await store_term1_data(valid)

        elapsed = (datetime.now() - start).total_seconds()
        await asyncio.sleep(max(0, CYCLE_TIME - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
