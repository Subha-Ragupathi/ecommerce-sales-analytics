# ecommerce-sales-analytics

Sales analysis project I built using a synthetic ecommerce dataset. Covers 3 years of data (2022-2024), around 10k transactions.

I wanted to practice the full workflow — data cleaning, EDA, SQL, and get something decent for my portfolio.

---

## What's in here

- `notebooks/` — EDA notebook, main analysis is in `01_EDA_Analysis.ipynb`
- `sql/` — schema setup + a bunch of queries I wrote for KPIs (RFM, cohort, YoY stuff)
- `azure/` — ETL pipeline script that simulates Bronze/Silver/Gold layers
- `dashboard/` — Power BI dashboard spec (5 pages, includes DAX measures)
- `data/` — raw and cleaned CSVs

---

## Stack

Python, Pandas, Matplotlib, Seaborn, SQL (MySQL), Azure Blob + SQL, Power BI

---

## How to run

```bash
pip install -r requirements.txt
python data/generate_data.py        # creates the dataset
jupyter notebook                    # open notebooks/01_EDA_Analysis.ipynb
cd azure && python data_pipeline.py # runs locally in sim mode without Azure creds
```

---

## A few things I found

- Electronics has the highest revenue but also the highest return rate — not surprising given price point
- Q4 is huge, like 35% of annual revenue just from Oct-Dec
- Discounts past ~15% don't really move the needle on order volume
- Mobile App channel is growing faster than Website YoY which is interesting

---

The Power BI dashboard spec is in `dashboard/powerbi_dashboard_spec.md` — covers 5 pages with DAX measures, KPI cards, and slicers.
