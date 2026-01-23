import time
import psycopg2
import select
import struct
from datetime import datetime, timezone

from pyModbusTCP.client import ModbusClient

from db import get_db_connection


# =========================================================
# CONFIG
# =========================================================

ESS_POWER_SETPOINT_REG = 600
ESS_SCALE = 10

DEADBAND_KW = 1.0
MIN_WRITE_INTERVAL = 2.0

# =========================================================
# HELPERS – DB
# =========================================================

def get_latest_unapplied_control(cur, pod):
    cur.execute("""
        SELECT id, sum_setpoint
        FROM alteo_control_inbox
        WHERE pod = %s
          AND applied = false
        ORDER BY received_at DESC
        LIMIT 1
    """, (pod,))
    return cur.fetchone()


def mark_control_applied(cur, control_id, applied_kw, note=None):
    cur.execute("""
        UPDATE alteo_control_inbox
        SET applied = true,
            applied_at = NOW(),
            applied_value = %s,
            note = %s
        WHERE id = %s
    """, (applied_kw, note, control_id))


def get_latest_plant_state(cur, pod):
    cur.execute("""
        SELECT sum_active_power
        FROM plant_data_term1
        WHERE pod_id = %s
        ORDER BY measured_at DESC
        LIMIT 1
    """, (pod,))
    row = cur.fetchone()
    return row[0] if row else None


def get_latest_ess_state(cur, pod):
    cur.execute("""
        SELECT
            e.ip_address,
            e.port,
            d.average_current_soc,
            d.available_capacity_charge,
            d.available_capacity_discharge
        FROM ess_data_term1 d
        JOIN ess_units e ON e.plant_id = d.plant_id
        JOIN plants p ON p.id = d.plant_id
        WHERE p.pod_id = %s
        ORDER BY d.measured_at DESC
        LIMIT 1
    """, (pod,))
    return cur.fetchone()


# =========================================================
# HELPERS – CONTROL LOGIC
# =========================================================

def calculate_ess_setpoint(target_kw, current_kw, soc):
    """
    ESS-first logika:
    target = ALTEO sumSetPoint
    current = aktuális PV + ESS állapot
    """
    if soc is None:
        raise Exception("SOC unknown")

    delta = target_kw - current_kw
    return delta


def within_deadband(new_kw, last_kw):
    if last_kw is None:
        return False
    return abs(new_kw - last_kw) < DEADBAND_KW


# =========================================================
# HELPERS – MODBUS
# =========================================================

def write_ess_power(ip, port, kw):
    raw = int(kw * ESS_SCALE)

    # signed int32
    if raw < 0:
        raw = (1 << 32) + raw

    regs = [(raw >> 16) & 0xFFFF, raw & 0xFFFF]

    client = ModbusClient(
        host=ip,
        port=port,
        auto_open=True,
        auto_close=True,
        timeout=1.5
    )

    ok = client.write_multiple_registers(ESS_POWER_SETPOINT_REG, regs)
    if not ok:
        raise Exception("Modbus write failed")


# =========================================================
# MAIN CONTROL HANDLER
# =========================================================

def handle_control_for_pod(pod):
    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # --- Advisory lock (POD szint) ---
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (pod,))
        locked = cur.fetchone()[0]
        if not locked:
            return

        ctrl = get_latest_unapplied_control(cur, pod)
        if not ctrl:
            return

        control_id, target_kw = ctrl

        current_kw = get_latest_plant_state(cur, pod)
        ess = get_latest_ess_state(cur, pod)

        if current_kw is None or ess is None:
            mark_control_applied(cur, control_id, None, "Missing state data")
            conn.commit()
            return

        ip, port, soc, cap_ch, cap_dis = ess

        ess_kw = calculate_ess_setpoint(
            target_kw=target_kw,
            current_kw=current_kw,
            soc=soc
        )

        # --- SOC védelem ---
        if ess_kw > 0 and cap_dis <= 0:
            mark_control_applied(cur, control_id, 0, "No discharge capacity")
            conn.commit()
            return

        if ess_kw < 0 and cap_ch <= 0:
            mark_control_applied(cur, control_id, 0, "No charge capacity")
            conn.commit()
            return

        # --- Modbus write ---
        write_ess_power(ip, port, ess_kw)

        mark_control_applied(cur, control_id, ess_kw)
        conn.commit()

        print(f"[CTRL] POD {pod} → ESS setpoint {ess_kw:.1f} kW")

    except Exception as e:
        conn.rollback()
        print(f"[CTRL][ERROR] POD {pod}: {e}")

    finally:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (pod,))
        cur.close()
        conn.close()


# =========================================================
# LISTEN / NOTIFY LOOP
# =========================================================

def main():
    conn = psycopg2.connect(
        dbname="...",
        user="...",
        password="...",
        host="...",
        port="..."
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    cur.execute("LISTEN alteo_control;")

    print("[CTRL_EXEC] Waiting for ALTEO controls...")

    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue

        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            pod = notify.payload
            handle_control_for_pod(pod)


if __name__ == "__main__":
    main()
