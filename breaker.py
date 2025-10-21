from datetime import datetime, timedelta

# Hibás plant-ek nyilvántartása
_failed_plants = {}
COOLDOWN_MINUTES = 5

def should_skip(plant_id):
    """Megmondja, hogy kihagyjuk-e a plantet a következő ciklusban"""
    if plant_id not in _failed_plants:
        return False
    last_fail = _failed_plants[plant_id]
    if datetime.now() - last_fail < timedelta(minutes=COOLDOWN_MINUTES):
        return True
    else:
        del _failed_plants[plant_id]
        return False

def on_failure(plant_id):
    """Regisztrálja a plant hibáját"""
    _failed_plants[plant_id] = datetime.now()
    print(f"[BREAKER] Plant {plant_id} marked failed at {_failed_plants[plant_id]}")

def on_success(plant_id):
    """Ha újra sikeres, eltávolítjuk a feketelistáról"""
    if plant_id in _failed_plants:
        del _failed_plants[plant_id]
        print(f"[BREAKER] Plant {plant_id} recovered.")
