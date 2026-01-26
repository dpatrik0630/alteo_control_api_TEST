import asyncio
import json
import math
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
                plant_id, pod_id, measured_at, sum_active_power, cos_phi,
                available_power_min, available_power_max, reference_power,
                ghi, panel_temp
            ) VALUES %s
        """, [
            (
                r["plant_id"], r["pod_id"], r["timestamp"],
                r["sum_active_power"], r["phi_deg"],
                r["available_power_min"], r["available_power_max"],
                r["reference_power"], r["ghi"], r["panel_temp"]
            ) for r in records
        ])
        conn.commit()
        cur.close()
        conn.close()

    await loop.run_in_executor(None, _insert)

def cosphi_to_phi_deg(cos_phi: float, manufacturer: str):
    """
    cosφ → φ fokban
    Huawei: előjel a cosφ-ból
    Fronius: mindig + (induktív)
    """
    if cos_phi is None:
        return None

    # numerikus védelem
    cos_phi = max(-1.0, min(1.0, cos_phi))

    # abs értékből számoljuk a szöget
    phi = math.degrees(math.acos(abs(cos_phi)))

    if manufacturer.lower() == "huawei":
        return phi if cos_phi >= 0 else -phi
    else:
        # fronius → induktívnak tekintjük
        return phi


def read_cos_phi(client, manufacturer, cosphi_meta):
    """Gyártófüggő cosφ olvasás"""
    if manufacturer.lower() == "fronius":
        regs = client.read_holding_registers(40092 - 1, 2)
        if not regs or len(regs) < 2:
            return None
        pf_raw = regs[0] if regs[0] < 0x8000 else regs[0] - 0x10000
        pf_sf = regs[1] if regs[1] < 0x8000 else regs[1] - 0x10000
        return pf_raw * (10 ** pf_sf)
    else:
        regs = client.read_holding_registers(cosphi_meta["address"], cosphi_meta["quantity"])
        return convert_registers_to_scaled_value(regs, cosphi_meta["gain"], cosphi_meta.get("signed", True))


async def poll_device(ip, port, slave_id, manufacturer, register_map):
    """Általános Modbus lekérdezés, gyártófüggő cosφ logikával"""
    client = ModbusClient(host=ip, port=port, unit_id=slave_id, auto_open=True, auto_close=True, timeout=1.5)
    data = {}

    # sum_active_power minden gyártónál ugyanúgy jön
    sap_meta = register_map.get("sum_active_power")
    sap_regs = client.read_holding_registers(sap_meta["address"], sap_meta["quantity"])
    data["sum_active_power"] = convert_registers_to_scaled_value(sap_regs, sap_meta["gain"], sap_meta.get("signed", True))

    # cos_phi külön logikával
    cosphi_meta = register_map.get("cos_phi")
    data["cos_phi"] = read_cos_phi(client, manufacturer, cosphi_meta)

    return data


async def collect_plant_data(plant):
    """Egy plant adatainak lekérése"""
    ip, port, pid = plant["ip"], plant["port"], plant["id"]

    try:
        logger_data = await poll_device(ip, port, plant["logger_slave_id"], plant["logger_manufacturer"], plant["register_map"])

        # --- cosφ validálás (-1 és 1 közé kell essen) ---
        cos_phi = logger_data.get("cos_phi")
        #if cos_phi is not None and not (-1 <= cos_phi <= 1):
        #    print(f"[WARN] Plant {pid} → Invalid cosφ value {cos_phi:.4f}, setting to None")
        #    cos_phi = None
        phi_deg = cosphi_to_phi_deg(
            cos_phi,
            plant["logger_manufacturer"]
)


        pod_id = plant.get("pod_id")

        record = {
            "plant_id": pid,
            "pod_id": pod_id,
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
    # ✅ Egyszer töltjük be a plant listát
    rows = await get_plants_and_ess()
    plants = {}

    for r in rows:
        pid = r[0]
        if pid not in plants:
            manufacturer = r[6]
            plants[pid] = {
                "id": r[0],
                "pod_id": r[1],
                "ip": r[3],
                "port": r[4],
                "logger_slave_id": r[5],
                "logger_manufacturer": manufacturer,
                "register_map": load_register_map("logger", manufacturer),  # előre betöltve
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
