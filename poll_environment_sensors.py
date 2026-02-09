import asyncio
from datetime import datetime, timezone

from pyModbusTCP.client import ModbusClient
from db import get_db_connection

POLL_INTERVAL = 30.0  # mp


def poll_sensor_sync(sensor):
    client = ModbusClient(
        host=sensor["ip_address"],
        port=sensor["port"],
        unit_id=sensor["slave_id"],
        auto_open=True,
        auto_close=True,
        timeout=1.0
    )

    regs = client.read_input_registers(0, 1)
    if not regs:
        raise Exception("No data from environment sensor")

    raw = regs[0]
    if raw > 32767:
        raw -= 65536

    return raw / 10.0

async def poll_once(sensor):
    temp = await asyncio.to_thread(poll_sensor_sync, sensor)

    sensor_id = sensor["id"]
    measured_at = datetime.now(timezone.utc).replace(microsecond=0)

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO environment_data_term1 (
            sensor_id,
            measured_at,
            temperature
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (sensor_id, measured_at) DO NOTHING
        """,
        (sensor_id, measured_at, temp)
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"[ENV] Sensor {sensor_id} → {temp:.1f} °C")


def get_24h_env_temp_avg_min_max(plant_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            AVG(e.temperature),
            MIN(e.temperature),
            MAX(e.temperature)
        FROM environment_data_term1 e
        JOIN plant_environment_sensors pes
          ON pes.sensor_id = e.sensor_id
        WHERE pes.plant_id = %s
          AND e.measured_at >= NOW() - INTERVAL '24 hours'
    """, (plant_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if row and row[0] is not None:
        return row[0], row[1], row[2]

    return None, None, None


async def main():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            ip_address,
            port,
            slave_id
        FROM environment_sensors
        WHERE active = TRUE
    """)

    sensors = [
        {
            "id": r[0],
            "ip_address": r[1],
            "port": r[2],
            "slave_id": r[3],
        }
        for r in cur.fetchall()
    ]

    cur.close()
    conn.close()

    print(f"[ENV] Started polling {len(sensors)} environment sensors")

    while True:
        await asyncio.gather(
            *[poll_once(s) for s in sensors],
            return_exceptions=True
        )
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
