import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

n_merchants = 80
n_weeks = 52
start_date = datetime(2024, 1, 1)
output_dir = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(output_dir, exist_ok=True)

verticals = ["Grocery", "Dining", "Retail", "Travel", "Gas & Convenience"]
stages = ["Prospecting", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
reps = ["Jordan Kim", "Maya Patel", "Chris Okafor", "Sara Lind", "Devon Reyes"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# Pipeline Data
records = []
for i in range(n_merchants):
    vertical = random.choice(verticals)
    stage = random.choices(
        stages,
        weights=[10, 20, 25, 20, 15, 10]   # more deals in mid-funnel (realistic)
    )[0]
    open_date = random_date(start_date, start_date + timedelta(days=300))

    # Deal value based on vertical 
    base_value = {
        "Grocery": 45000, 
        "Dining": 30000, 
        "Retail": 60000,
        "Travel": 80000, 
        "Gas & Convenience": 25000
    }[vertical]
    deal_value = int(np.random.normal(base_value, base_value * 0.25))
    deal_value = max(deal_value, 5000) #minimum deal value is $5k

    days_in_stage = np.random.randint(3, 45)
    close_prob = {
        "Prospecting": 0.05, 
        "Qualified": 0.2, 
        "Proposal": 0.4,
        "Negotiation": 0.7, 
        "Closed Won": 1.0, 
        "Closed Lost": 0.0
    }[stage]

    records.append({
        "deal_id": f"Deal-{1000 + i}",
        "merchant_name": f"{vertical} Merchant {i+1}",
        "vertical": vertical,
        "rep_name": random.choice(reps),
        "stage": stage,
        "deal_value": deal_value,
        "open_date": open_date.strftime("%Y-%m-%d"),
        "days_in_stage": days_in_stage,
        "close_probability": close_prob,
        "weighted_value": round(deal_value * close_prob, 2)
    })

pipeline_df = pd.DataFrame(records)
pipeline_df.to_csv(f"{output_dir}/pipeline.csv", index=False)
print(f"✓ pipeline.csv — {len(pipeline_df)} deals")

#Campaign Data: impressions → clicks → redemptions
campaigns = []
merchant_ids = pipeline_df[pipeline_df["stage"] == "Closed Won"]["deal_id"].tolist()

# If not enough closed won deals, sample from all merchants
if len(merchant_ids) < 20:
    merchant_ids = pipeline_df["deal_id"].sample(20).tolist()

for week_num in range(n_weeks):
    week_start = (start_date + timedelta(weeks=week_num)).strftime("%Y-%m-%d")
    active_merchants = random.sample(merchant_ids, min(15, len(merchant_ids)))
    for deal_id in active_merchants:
        row = pipeline_df[pipeline_df["deal_id"] == deal_id].iloc[0]
        vertical = row["vertical"]
        # Impressions based on vertical
        impressions = int(np.random.normal(50000,12000))
        impressions = max(impressions, 5000) # minimum impressions
        # Click-through rate based on vertical
        ctr = np.random.normal({
            "Grocery": 0.04, 
            "Dining": 0.05, 
            "Retail": 0.035,
            "Travel": 0.03, 
            "Gas & Convenience": 0.045
        }[vertical], 0.01
        )
        ctr = max(ctr, 0.005)
        clicks = int(impressions * ctr)

        # Redemption rate: clicks that convert to offer use
        redemption_rate = np.random.normal(0.18, 0.05)
        redemption_rate = max(redemption_rate, 0.02)
        redemptions = int(clicks * redemption_rate)

        spend = round(impressions * np.random.uniform(0.008, 0.015),2) # ad spend based on impressions
        revenue_driven = round(redemptions * np.random.uniform(25, 120), 2)
        roas = round(revenue_driven / spend, 2) if spend > 0 else 0

        campaigns.append({
            "week_start": week_start,
            "deal_id": deal_id,
            "vertical": vertical,
            "impressions": impressions,
            "clicks": clicks,
            "redemptions": redemptions,
            "ctr": round(ctr, 4),
            "redemption_rate": round(redemption_rate, 4),
            "spend": spend,
            "revenue_driven": revenue_driven,
            "roas": roas
        })
campaigns_df = pd.DataFrame(campaigns)
campaigns_df.to_csv(f"{output_dir}/campaigns.csv", index=False)
print(f"✓ campaigns.csv — {len(campaigns_df)} campaign weeks")

# Weekly Revenue: Aggregate revenue per week with a realistic seasonal trend baked in
# Retail media revenue peaks Q4 (holiday), dips Q1 

weekly_records = []
for week_num in range(n_weeks):
    week_start = start_date + timedelta(weeks=week_num)
    #Seasonality: Q4 boost, Q1 dip modeled as a sine wave offset
    month = week_start.month
    seasonal_factor = {
        1: 0.78, 2: 0.82, 3: 0.88, 4: 0.92, 5: 0.95, 6: 0.97,
        7: 0.96, 8: 0.98, 9: 1.02, 10: 1.08, 11: 1.25, 12: 1.35
    }[month]
    base_revenue = 120000   # ~$6.2M annualised, scaled to your Madewell context
    revenue = round(np.random.normal(base_revenue * seasonal_factor, 8000), 2)
    revenue = max(revenue, 40000)

    target = round(base_revenue * seasonal_factor * 1.06, 2)   # target = 6% above base

    weekly_records.append({
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_num": week_num + 1,
        "revenue": revenue,
        "target": target,
        "attainment_pct": round(revenue / target * 100, 1),
        "quarter": f"Q{(week_start.month - 1) // 3 + 1}"
    })

revenue_df = pd.DataFrame(weekly_records)
revenue_df.to_csv(f"{output_dir}/weekly_revenue.csv", index=False)
print(f"✓ weekly_revenue.csv — {len(revenue_df)} weeks")

print("\nAll data generated successfully. Files saved to data/raw/")