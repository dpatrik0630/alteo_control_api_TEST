import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from pyModbusTCP.client import ModbusClient
from db import get_db_connection
from breaker import should_skip, on_success, on_failure
from psycopg2.extras import execute_values


def load_register_map(device_type: str, manufacturer: str, model: str = None):
    """Betölti a megfelelő regisztermapot a JSON-ból"""
    base = Path(__file__).parent / "register_maps"
    if device_type == "logger":
        path = base / "logger" / f"{manufacturer.lower()}.json"
    elif device_type == "ess":
        fname = f"{manufacturer.lower()}_{model.lower()}.json" if model else f"{manufacturer.lower()}.json"
        path = base / "ess" / fname
    else:
        raise ValueError("Ismeretlen eszköztípus")
    if not path.exists():
        raise FileNotFoundError(f"Hiányzó regisztermap: {path}")
    with open(path, "r") as f:
        return json.load(f)


def convert_registers_to_scaled_value(registers, gain, signed=True):
    """Konvertálja a Modbus regisztereket skálázott float értékre"""
    if not registers:
        return None
    if len(registers) == 2:
        raw = (registers[0] << 16) | registers[1]
    else:
        raw = registers[0]
    if signed and raw & 0x80000000:
        raw = -((~raw & 0xFFFFFFFF) + 1)
    return raw / gain


async def get_plants_and_ess():
    """Lekéri az aktív plant-eket és a hozzájuk tartozó ESS-eket"""
    loop = asyncio.get_running_loop()

    def _fetch():
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                p.id AS plant_id,
                p.pod_id,
                p.name,
                p.ip_address,
                p.port,
                p.logger_slave_id,
                p.logger_manufacturer,
                e.id AS ess_id,
                e.slave_id AS ess_slave_id,
                e.manufacturer AS ess_manufacturer,
                e.model AS ess_model
            FROM plants p
            LEFT JOIN ess_units e ON e.plant_id = p.id AND e.active = TRUE
            WHERE p.alteo_api_control = TRUE;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    return await loop.run_in_executor(None, _fetch)


async def store_term1_data(records):
    """Batch INSERT a plant_data_term1 táblába"""
    if not records:
        return
    loop = asyncio.get_running_loop()

    def _insert():
        conn = get_db_connection()
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO plant_data_term1 (
                plant_id, measured_at, sum_active_power, cos_phi,
                available_power_min, available_power_max, reference_power,
                ghi, panel_temp
            ) VALUES %s
        """, [
            (
                r["plant_id"], r["timestamp"],
                r["sum_active_power"], r["cos_phi"],
                r["available_power_min"], r["available_power_max"],
                r["reference_power"], r["ghi"], r["panel_temp"]
            ) for r in records
        ])
        conn.commit()
        cur.close()
        conn.close()

    await loop.run_in_executor(None, _insert)


async def poll_device(ip, port, slave_id, register_map):
    """Általános Modbus lekérdezés"""
    client = ModbusClient(host=ip, port=port, unit_id=slave_id, auto_open=True, auto_close=True, timeout=1.5)
    data = {}
    for key, meta in register_map.items():
        regs = client.read_holding_registers(meta["address"], meta["quantity"])
        data[key] = convert_registers_to_scaled_value(regs, meta["gain"], meta.get("signed", True))
    return data


async def collect_plant_data(plant):
    """Egy plant adatainak lekérése"""
    ip, port, pid = plant["ip"], plant["port"], plant["id"]

    try:
        logger_map = load_register_map("logger", plant["logger_manufacturer"])
        logger_data = await poll_device(ip, port, plant["logger_slave_id"], logger_map)

        """ess_results = []
        for ess in plant["ess_list"]:
            ess_map = load_register_map("ess", ess["manufacturer"], ess["model"])
            ess_data = await poll_device(ip, port, ess["slave_id"], ess_map)
            ess_results.append(ess_data)"""

        record = {
            "plant_id": pid,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0),
            "sum_active_power": logger_data.get("sum_active_power"),
            "cos_phi": logger_data.get("cos_phi"),
            "available_power_min": 0.0,
            "available_power_max": abs(logger_data["sum_active_power"]) if logger_data.get("sum_active_power") else None,
            "reference_power": abs(logger_data["sum_active_power"]) if logger_data.get("sum_active_power") else None,
            "ghi": None,
            "panel_temp": None,
        }

        on_success(pid)
        return record

    except Exception as e:
        print(f"[ERR] Plant {pid} – {e}")
        on_failure(pid)
        return None


async def main():
    rows = await get_plants_and_ess()
    plants = {}

    for r in rows:
        pid = r[0]
        if pid not in plants:
            plants[pid] = {
                "id": r[0],
                "pod_id": r[1],
                "ip": r[3],
                "port": r[4],
                "logger_slave_id": r[5],
                "logger_manufacturer": r[6],
                "ess_list": [],
            }
        if r[7]:
            plants[pid]["ess_list"].append({
                "id": r[7],
                "slave_id": r[8],
                "manufacturer": r[9],
                "model": r[10],
            })

    print(f"[ALTEO_TERM1] Running for {len(plants)} plants...")

    while True:
        start = datetime.now()
        tasks = [asyncio.create_task(collect_plant_data(p)) for p in plants.values() if not should_skip(p["id"])]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            valid = [r for r in results if isinstance(r, dict)]
            if valid:
                await store_term1_data(valid)
        elapsed = (datetime.now() - start).total_seconds()
        await asyncio.sleep(max(0, 2 - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
