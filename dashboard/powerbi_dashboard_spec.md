# 📊 Power BI Dashboard Specification
## E-Commerce Sales Analytics

**Author:** Subha Ragupathi  
**Tool:** Power BI Desktop / Power BI Service  
**Data Source:** `ecommerce_sales_cleaned.csv` or Azure SQL `ecommerce_db`

---

## Dashboard Pages

---

### PAGE 1: Executive Summary

**Layout:** 2-row grid

| Visual | Type | Fields | Purpose |
|--------|------|---------|---------|
| Total Revenue | KPI Card | SUM(total_amount) | Top-level revenue |
| Total Orders | KPI Card | COUNT(order_id) | Volume metric |
| Avg Order Value | KPI Card | AVERAGE(total_amount) | Efficiency metric |
| Return Rate % | KPI Card | DIVIDE(COUNTIF(status=Returned), COUNT) | Quality metric |
| Revenue by Year | Line Chart | year, SUM(total_amount) | Trend |
| Revenue by Category | Bar Chart | category, SUM(total_amount) | Category breakdown |
| Orders by Region | Filled Map | region, COUNT(order_id) | Geo distribution |
| Order Status | Donut Chart | order_status, COUNT | Status split |

**Slicers:** Year, Quarter, Region, Category

---

### PAGE 2: Sales Trends

| Visual | Type | Fields |
|--------|------|--------|
| Monthly Revenue | Area Chart | month_name, year, SUM(total_amount) |
| YoY Comparison | Clustered Bar | year, month, SUM(total_amount) |
| Q4 vs Off-Season | Clustered Column | is_q4, SUM(total_amount) |
| Weekly Orders | Line Chart | week, COUNT(order_id) |
| Revenue Heatmap | Matrix | year (rows), month_name (cols), SUM(total_amount) |

---

### PAGE 3: Product & Category

| Visual | Type | Fields |
|--------|------|--------|
| Revenue by Category | Treemap | category, SUM(total_amount) |
| Top 10 Products | Horizontal Bar | product_name, SUM(total_amount) |
| Category vs Discount | Scatter | discount_pct (X), AVG(total_amount) (Y), category (detail) |
| Discount Distribution | Histogram | discount_pct, COUNT |
| Revenue Bucket | Donut | revenue_bucket, COUNT |

---

### PAGE 4: Customer Intelligence

| Visual | Type | Fields |
|--------|------|--------|
| Revenue by Segment | Stacked Bar | customer_segment, SUM(total_amount) |
| Segment AOV | KPI Cards × 4 | customer_segment, AVG(total_amount) |
| Region × Segment | Matrix | region (rows), customer_segment (cols), SUM |
| Rating Distribution | Column Chart | customer_rating, COUNT |
| Orders per Customer | Gauge | AVG orders per customer_id |

---

### PAGE 5: Channel & Payment

| Visual | Type | Fields |
|--------|------|--------|
| Channel Revenue Share | Donut | sales_channel, SUM(total_amount) |
| Channel AOV | Bar | sales_channel, AVG(total_amount) |
| Channel Return Rate | Bar (conditional) | sales_channel, Return Rate % |
| Payment Method | Treemap | payment_method, COUNT |
| Channel Trend | Line | month, sales_channel, SUM(total_amount) |

---

## DAX Measures

```dax
-- Total Revenue
Total Revenue = SUM(fact_orders[total_amount])

-- Average Order Value
AOV = DIVIDE([Total Revenue], DISTINCTCOUNT(fact_orders[order_id]))

-- Return Rate %
Return Rate % = 
DIVIDE(
    COUNTROWS(FILTER(fact_orders, fact_orders[order_status] = "Returned")),
    COUNTROWS(fact_orders),
    0
) * 100

-- YoY Revenue Growth
YoY Growth % = 
VAR CurrentYearRev = [Total Revenue]
VAR PriorYearRev = 
    CALCULATE([Total Revenue], DATEADD(dim_date[date_key], -1, YEAR))
RETURN
    DIVIDE(CurrentYearRev - PriorYearRev, PriorYearRev) * 100

-- Customer Lifetime Value
Avg CLV = 
AVERAGEX(
    SUMMARIZE(fact_orders, fact_orders[customer_id], "CLV", SUM(fact_orders[total_amount])),
    [CLV]
)

-- Q4 Revenue Flag
Q4 Revenue = 
CALCULATE(
    [Total Revenue],
    FILTER(fact_orders, fact_orders[quarter] = 4)
)

-- Profit (35% margin estimate)
Total Profit = SUMX(fact_orders, fact_orders[revenue] * 0.35)
```

---

## Theme & Colors

| Element | Color | Hex |
|---------|-------|-----|
| Primary | Deep Blue | `#1E3A5F` |
| Accent | Teal | `#00B4D8` |
| Positive | Green | `#2ECC71` |
| Negative | Red | `#E74C3C` |
| Neutral | Gray | `#95A5A6` |
| Background | White | `#FFFFFF` |

---

## Power BI Service Setup

1. Publish from Desktop to Power BI Service
2. Schedule data refresh (Daily at 6:00 AM UTC)
3. Configure Row-Level Security (RLS) by Region
4. Share dashboard link with stakeholders
5. Set up email subscriptions for weekly executive snapshot
