import duckdb
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

queries = {
    "revenue_rollup":     "sql/revenue_rollup.sql",
    "pipeline_velocity":  "sql/pipeline_velocity.sql",
    "conversion_funnel":  "sql/conversion_funnel.sql",
}

con = duckdb.connect()

for name, path in queries.items():
    print(f"\n{'='*55}")
    print(f"  {name.replace('_', ' ').upper()}")
    print(f"{'='*55}")

    full_path = os.path.join(ROOT, path)
    with open(full_path, "r") as f:
        query = f.read()

    query = query.replace(
        "data/raw/",
        os.path.join(ROOT, "data", "raw") + "/"
    )

    try:
        rel = con.sql(query)
        df = rel.df()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error in {name}: {e}")