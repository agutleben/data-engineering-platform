# data_generator/generate_events.py
from faker import Faker
import pandas as pd
import random
import uuid
import os
from datetime import datetime, timedelta
from pathlib import Path

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
NB_EVENTS    = 1_000_000   # on commence par 1M, on montera à 10M
NB_USERS     = 50_000
NB_PRODUCTS  = 5_000
DAYS_BACK    = 90           # événements sur les 90 derniers jours
OUTPUT_DIR   = Path("output")
BATCH_SIZE   = 100_000      # écriture par batch pour ne pas exploser la RAM

EVENT_TYPES  = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.70, 0.20, 0.10]   # 70% views, 20% add_to_cart, 10% achats

CATEGORIES = ["electronics", "clothing", "books", "sports", "home", "beauty", "toys"]

# ── Data pools (générés une fois, réutilisés) ─────────────────────────────────
print("Génération des pools utilisateurs et produits...")
USER_IDS    = [str(uuid.uuid4()) for _ in range(NB_USERS)]
PRODUCT_IDS = [f"PROD-{str(i).zfill(5)}" for i in range(NB_PRODUCTS)]
PRODUCT_META = {
    pid: {
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(5.0, 800.0), 2),
    }
    for pid in PRODUCT_IDS
}

# ── Générateur d'événement ────────────────────────────────────────────────────
def generate_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    product_id = random.choice(PRODUCT_IDS)
    meta       = PRODUCT_META[product_id]

    # timestamp aléatoire dans les DAYS_BACK derniers jours
    days_ago  = random.randint(0, DAYS_BACK)
    hours_ago = random.randint(0, 23)
    ts = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago,
                                        minutes=random.randint(0, 59))

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "user_id":     random.choice(USER_IDS),
        "product_id":  product_id,
        "category":    meta["category"],
        "amount":      meta["price"] if event_type == "purchase" else 0.0,
        "event_date":  ts.date().isoformat(),
        "event_ts":    ts.isoformat(),
        "session_id":  str(uuid.uuid4()),
        "device":      random.choice(["mobile", "desktop", "tablet"]),
        "country":     fake.country_code(),
    }

# ── Génération + écriture Parquet par batch ───────────────────────────────────
def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"Génération de {NB_EVENTS:,} événements...")
    total_written = 0
    batch_num     = 0

    while total_written < NB_EVENTS:
        current_batch = min(BATCH_SIZE, NB_EVENTS - total_written)

        events = [generate_event() for _ in range(current_batch)]
        df     = pd.DataFrame(events)

        # partitionnement par date dans le nom de fichier
        out_path = OUTPUT_DIR / f"events_batch_{batch_num:03d}.parquet"
        df.to_parquet(out_path, index=False, compression="snappy")

        total_written += current_batch
        batch_num     += 1
        print(f"  [{total_written:>9,} / {NB_EVENTS:,}] → {out_path.name}")

    print(f"\nTerminé ! {batch_num} fichiers Parquet dans ./{OUTPUT_DIR}/")
    print(f"Taille totale estimée : ~{batch_num * 15}MB")

if __name__ == "__main__":
    main()