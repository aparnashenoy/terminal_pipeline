"""
generate_sample_data.py

Generates a synthetic raw_terminal_data.csv for local testing of
weekly_report.py, matching the schema the ETL pipeline expects.
"""

import random
from datetime import date, timedelta

import pandas as pd

random.seed(42)

BERTHS = ["Berth 1", "Berth 2", "Berth 3", "Berth 4"]
SHIPPING_LINES = ["Maersk", "MSC", "CMA CGM", "Evergreen", "COSCO"]
OPERATION_TYPES = ["Import", "Export", "Transshipment"]

rows = []
end_date = date.today()
start_date = end_date - timedelta(days=13)  # two weeks of data

vessel_call_id = 1000
current = start_date
while current <= end_date:
    for _ in range(random.randint(2, 5)):
        containers = random.randint(300, 1800)
        rows.append(
            {
                "date": current.isoformat(),
                "vessel_call_id": f"VC{vessel_call_id}",
                "berth": random.choice(BERTHS),
                "shipping_line": random.choice(SHIPPING_LINES),
                "operation_type": random.choice(OPERATION_TYPES),
                "containers_handled": containers,
                "teu_moves": int(containers * random.uniform(1.1, 1.6)),
                "crane_moves_per_hour": round(random.uniform(20, 40), 1),
                "berth_hours": round(random.uniform(8, 30), 1),
                "avg_dwell_time_hours": round(random.uniform(10, 72), 1),
            }
        )
        vessel_call_id += 1
    current += timedelta(days=1)

df = pd.DataFrame(rows)
df.to_csv("raw_terminal_data.csv", index=False)
print(f"Wrote {len(df)} rows to raw_terminal_data.csv")