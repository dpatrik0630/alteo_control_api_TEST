print("CONTROL EXECUTOR FILE LOADED BEFORE IMPORTS", flush=True)
import time
import threading
import json
from pathlib import Path
from db import get_db_connection
from breaker import should_skip, on_failure, on_success
from pyModbusTCP.client import ModbusClient
import sys
sys.stdout.reconfigure(line_buffering=True)


print("========== CONTROL EXECUTOR VERSION 2 ==========")


# ==============================
# CONFIG
# ==============================

CONTROL_INTERVAL = 1.5
DEADBAND_KW = 1.0
KP = 0.3
MIN_WRITE_INTERVAL = 4.0

BASE_DIR = Path(__file__).parent


# ==============================
# REGISTER MAP LOADER
# ==============================

def load_register_map(category: str, manufacturer: str):
    path = BASE_DIR / "register_maps" / category / f"{manufacturer.lower()}.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        return json.load(f)


LOGGER_MAPS = {
    "huawei": load_register_map("logger", "huawei"),
    "fronius": load_register_map("logger", "fronius"),
}


# ==============================
# POD STATE
# ==============================

class PodControlState:
    def __init__(self, pod_id):
        self.pod_id = pod_id
        self.last_cmd_kw = 0.0
        self.last_write_ts = 0.0


# ==============================
# DB HELPERS
# ==============================

def get_latest_target_kw(cur, pod_id):
    cur.execute("""
        SELECT sum_setpoint
        FROM alteo_controls_inbox
        WHERE pod = %s
        ORDER BY received_at DESC
        LIMIT 1
    """, (pod_id,))
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


def get_latest_pcc_kw(cur, pod_id):
    cur.execute("""
        SELECT sum_active_power
        FROM plant_data_term1
        WHERE pod_id = %s
        ORDER BY measured_at DESC
        LIMIT 1
    """, (pod_id,))
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


def get_latest_ess_state(cur, pod_id):
    cur.execute("""
        SELECT
            e.ip_address,
            e.port,
            d.available_capacity_charge,
            d.available_capacity_discharge
        FROM ess_data_term1 d
        JOIN ess_units e ON e.plant_id = d.plant_id
        JOIN plants p ON p.id = d.plant_id
        WHERE p.pod_id = %s
        ORDER BY d.measured_at DESC
        LIMIT 1
    """, (pod_id,))
    return cur.fetchone()


def get_logger_info(cur, pod_id):
    cur.execute("""
        SELECT
            logger_manufacturer,
            ip_address,
            port,
            logger_slave_id,
            normal_power
        FROM plants
        WHERE pod_id = %s
          AND plant_type = 'PV_ONLY'
        LIMIT 1
    """, (pod_id,))
    return cur.fetchone()

# ==============================
# MODBUS WRITES
# ==============================

def write_ess_setpoint(ip, port, kw):
    # Hithium: float32 big endian, kW
    import struct
    payload = struct.pack(">f", float(kw))
    regs = struct.unpack(">HH", payload)

    client = ModbusClient(
        host=ip,
        port=port,
        auto_open=True,
        auto_close=True,
        timeout=1.0
    )

    if not client.write_multiple_registers(1000, list(regs)):
        raise Exception("ESS Modbus write failed")


def apply_huawei_pv_limit(logger, target_kw):
    meta = LOGGER_MAPS["huawei"]["controls"]["activePowerAdjustment"]

    raw = int(target_kw * meta["gain"])
    if meta.get("signed") and raw < 0:
        raw = (1 << 32) + raw

    regs = [(raw >> 16) & 0xFFFF, raw & 0xFFFF]

    client = ModbusClient(
        host=logger["ip"],
        port=logger["port"],
        unit_id=logger["slave"],
        auto_open=True,
        auto_close=True
    )

    client.write_multiple_registers(meta["address"], regs)


def get_plant_type(cur, pod_id):
    cur.execute("""
        SELECT plant_type
        FROM plants
        WHERE pod_id = %s
          AND plant_type = 'PV_ONLY'
        LIMIT 1
    """, (pod_id,))
    row = cur.fetchone()
    return row[0] if row else None



def apply_fronius_pv_limit(logger, target_kw):
    meta = LOGGER_MAPS["fronius"]["controls"]["activePowerLimitPercent"]

    percent = max(
        0,
        min(100, (target_kw / logger["rated_kw"]) * 100)
    )

    client = ModbusClient(
        host=logger["ip"],
        port=logger["port"],
        unit_id=logger["slave"],
        auto_open=True,
        auto_close=True
    )

    ena = meta["enable_register"]
    client.write_single_register(ena["address"], ena["enable_value"])
    client.write_single_register(meta["address"], int(percent))


# ==============================
# CONTROL LOOP
# ==============================

def control_loop(pod_id):
    state = PodControlState(pod_id)
    first_run = True

    while True:
        start = time.monotonic()

        print(f"[CTRL][LOOP] Checking POD={pod_id}")

        '''if should_skip(pod_id):
            time.sleep(1)
            continue'''

        conn = get_db_connection()
        cur = conn.cursor()

        try:
            target_kw = get_latest_target_kw(cur, pod_id)
            pcc_kw = get_latest_pcc_kw(cur, pod_id)

            if first_run and pcc_kw is not None:
                        state.last_cmd_kw = pcc_kw
                        first_run = False
                        print(f"[CTRL][INIT] POD {pod_id} initialized from measurement: {pcc_kw} kW")

            logger_row = get_logger_info(cur, pod_id)

            plant_type = get_plant_type(cur, pod_id)

            print(
                f"[CTRL][DATA] POD={pod_id} "
                f"target={target_kw} "
                f"pcc={pcc_kw} "
                f"type={plant_type}"
            )

            if None in (target_kw, pcc_kw, logger_row, plant_type):
                print(
                    f"[CTRL][SKIP][MISSING_DATA] "
                    f"POD={pod_id} "
                    f"target={target_kw} "
                    f"pcc={pcc_kw} "
                    f"logger={logger_row} "
                    f"type={plant_type}"
                )
                continue

            ess = get_latest_ess_state(cur, pod_id) if plant_type == "PV_ESS" else None

            manufacturer, lip, lport, lslave, pv_rated = logger_row

            if plant_type == "PV_ESS" and ess:
                ip, port, cap_ch, cap_dis = ess
            else:
                ip = port = cap_ch = cap_dis = None

            actual_kw = pcc_kw
            error = target_kw - actual_kw
            print(
                f"[CTRL][CALC] POD={pod_id} "
                f"target={target_kw} "
                f"pcc_raw={pcc_kw} "
                f"actual={actual_kw} "
                f"error={error}"
            )

            print(
                f"[CTRL][STATE] POD={pod_id} "
                f"error={error}"
            )

            # ---- DEAD BAND ----
            if abs(error) < DEADBAND_KW:
                print(
                    f"[CTRL][SKIP][DEADBAND] POD={pod_id} "
                    f"error={error}"
                )
                continue

            now = time.monotonic()

            # ================= CONTROL LOGIC =================

            # ---- PV + ESS ----
            if plant_type == "PV_ESS" and ess:
                print(f"[CTRL][BRANCH] PV_ESS for POD={pod_id}")
                if (error > 0 and cap_dis > 0) or (error < 0 and cap_ch > 0):
                    new_cmd = state.last_cmd_kw + KP * error

                    if now - state.last_write_ts < MIN_WRITE_INTERVAL:
                        continue

                    write_ess_setpoint(ip, port, new_cmd)

                    state.last_cmd_kw = new_cmd
                    state.last_write_ts = now

                    print(f"[CTRL][ESS] POD {pod_id} → {new_cmd:.1f} kW")

                elif error < 0:
                    logger = {
                        "ip": lip,
                        "port": lport,
                        "slave": lslave,
                        "rated_kw": pv_rated
                    }

                    if manufacturer.lower() == "huawei":
                        apply_huawei_pv_limit(logger, target_kw)
                        print(f"[CTRL][PV][Huawei] POD {pod_id} limit {target_kw:.1f} kW")

                    elif manufacturer.lower() == "fronius":
                        apply_fronius_pv_limit(logger, target_kw)
                        print(f"[CTRL][PV][Fronius] POD {pod_id} limit {target_kw:.1f} kW")


            # ---- PV ONLY (Closed Loop / Lágy szabályozással) ----
            elif plant_type == "PV_ONLY":
                print(f"[CTRL][BRANCH] PV_ONLY for POD={pod_id}", flush=True)
                
                # Kiszámoljuk a korrekciót (P-tag)
                # Ha a mérés (actual) több mint a cél (target), az error negatív lesz, 
                # így a new_limit csökkenni fog.
                adjustment = KP * error
                new_limit = state.last_cmd_kw + adjustment
                
                # Szigorú korlátok: 0 és a névleges teljesítmény között
                new_limit = max(0.0, min(pv_rated, new_limit))
                
                logger = {
                    "ip": lip,
                    "port": lport,
                    "slave": lslave,
                    "rated_kw": pv_rated
                }

                if manufacturer.lower() == "huawei":
                    apply_huawei_pv_limit(logger, new_limit)
                    print(f"[CTRL][PV_ONLY][SOFT] POD {pod_id} -> limit: {new_limit:.2f} kW (target: {target_kw}, actual: {actual_kw})", flush=True)
                
                elif manufacturer.lower() == "fronius":
                    apply_fronius_pv_limit(logger, new_limit)
                    print(f"[CTRL][PV_ONLY][SOFT] POD {pod_id} -> limit: {new_limit:.2f} kW", flush=True)

                # Elmentjük a parancsot a következő körhöz
                state.last_cmd_kw = new_limit
                    
            elif plant_type == "PV_ONLY" and error > 0:
                print(f"[CTRL][PV_ONLY] POD {pod_id} cannot increase power (no ESS)")
            on_success(pod_id)

        except Exception as e:
            print(f"[CTRL][ERROR] POD {pod_id} hiba: {e}", flush=True)
            # Itt opcionálisan használhatod a breaker-t:
            # on_failure(pod_id)

        finally:
            cur.close()
            conn.close()

        elapsed = time.monotonic() - start
        time.sleep(max(0, CONTROL_INTERVAL - elapsed))




# ==============================
# MAIN
# ==============================

def main():
    print("MAIN STARTED BEFORE DB", flush=True)
    conn = get_db_connection()
    cur = conn.cursor()
    print("DB CONNECTED", flush=True)

    cur.execute("""
        SELECT DISTINCT pod_id
        FROM plants
        WHERE alteo_api_control = TRUE
    """)
    pods = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    print(f"[CTRL_EXEC] Starting control loops for {len(pods)} PODs")

    for pod in pods:
        t = threading.Thread(
            target=control_loop,
            args=(pod,),
            daemon=True
        )
        t.start()

    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()
