-- ============================================================
-- FILE: 03_business_insights.sql
-- PROJECT: E-Commerce Sales Analytics
-- AUTHOR: Subha Ragupathi
-- DESCRIPTION: Advanced KPI queries and business insights
-- ============================================================

USE ecommerce_db;

-- ============================================================
-- KPI 1: YEAR-OVER-YEAR REVENUE GROWTH
-- ============================================================
WITH yearly AS (
    SELECT
        YEAR(order_date)            AS yr,
        SUM(total_amount)           AS revenue,
        COUNT(order_id)             AS orders,
        COUNT(DISTINCT customer_id) AS customers
    FROM fact_orders
    WHERE order_status NOT IN ('Cancelled')
    GROUP BY YEAR(order_date)
)
SELECT
    yr,
    ROUND(revenue, 2)               AS total_revenue,
    orders,
    customers,
    ROUND(revenue / orders, 2)      AS aov,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY yr)) /
        NULLIF(LAG(revenue) OVER (ORDER BY yr), 0) * 100
    , 2)                            AS yoy_revenue_growth_pct,
    ROUND(
        (orders - LAG(orders) OVER (ORDER BY yr)) /
        NULLIF(LAG(orders) OVER (ORDER BY yr), 0) * 100
    , 2)                            AS yoy_order_growth_pct
FROM yearly
ORDER BY yr;

-- ============================================================
-- KPI 2: CUSTOMER LIFETIME VALUE (CLV) BY SEGMENT
-- ============================================================
WITH customer_summary AS (
    SELECT
        f.customer_id,
        c.customer_segment,
        COUNT(f.order_id)           AS total_orders,
        SUM(f.total_amount)         AS lifetime_value,
        AVG(f.total_amount)         AS avg_order_value,
        MIN(f.order_date)           AS first_purchase,
        MAX(f.order_date)           AS last_purchase,
        DATEDIFF(MAX(f.order_date), MIN(f.order_date)) AS customer_lifespan_days
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE f.order_status = 'Delivered'
    GROUP BY f.customer_id, c.customer_segment
)
SELECT
    customer_segment,
    COUNT(customer_id)                              AS customers,
    ROUND(AVG(lifetime_value), 2)                  AS avg_clv,
    ROUND(AVG(total_orders), 2)                    AS avg_orders,
    ROUND(AVG(avg_order_value), 2)                 AS avg_aov,
    ROUND(AVG(customer_lifespan_days), 0)          AS avg_lifespan_days,
    ROUND(MAX(lifetime_value), 2)                  AS max_clv,
    ROUND(MIN(lifetime_value), 2)                  AS min_clv
FROM customer_summary
GROUP BY customer_segment
ORDER BY avg_clv DESC;

-- ============================================================
-- KPI 3: COHORT ANALYSIS — CUSTOMER RETENTION BY YEAR
-- ============================================================
WITH first_order AS (
    SELECT
        customer_id,
        MIN(YEAR(order_date)) AS cohort_year
    FROM fact_orders
    GROUP BY customer_id
),
order_years AS (
    SELECT DISTINCT
        f.customer_id,
        fo.cohort_year,
        YEAR(f.order_date) AS order_year,
        YEAR(f.order_date) - fo.cohort_year AS year_number
    FROM fact_orders f
    JOIN first_order fo ON f.customer_id = fo.customer_id
)
SELECT
    cohort_year,
    COUNT(DISTINCT CASE WHEN year_number = 0 THEN customer_id END)  AS year_0,
    COUNT(DISTINCT CASE WHEN year_number = 1 THEN customer_id END)  AS year_1,
    COUNT(DISTINCT CASE WHEN year_number = 2 THEN customer_id END)  AS year_2,
    ROUND(COUNT(DISTINCT CASE WHEN year_number = 1 THEN customer_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN year_number = 0 THEN customer_id END), 0), 1) AS retention_yr1_pct,
    ROUND(COUNT(DISTINCT CASE WHEN year_number = 2 THEN customer_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN year_number = 0 THEN customer_id END), 0), 1) AS retention_yr2_pct
FROM order_years
GROUP BY cohort_year
ORDER BY cohort_year;

-- ============================================================
-- KPI 4: RFM SEGMENTATION (Recency, Frequency, Monetary)
-- ============================================================
WITH rfm_base AS (
    SELECT
        customer_id,
        DATEDIFF('2024-12-31', MAX(order_date))  AS recency_days,
        COUNT(order_id)                           AS frequency,
        ROUND(SUM(total_amount), 2)               AS monetary
    FROM fact_orders
    WHERE order_status = 'Delivered'
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM rfm_base
),
rfm_final AS (
    SELECT *,
        (r_score + f_score + m_score)              AS total_rfm_score,
        CONCAT(r_score, f_score, m_score)          AS rfm_segment_code
    FROM rfm_scored
)
SELECT
    CASE
        WHEN total_rfm_score >= 13 THEN '🏆 Champions'
        WHEN total_rfm_score >= 10 THEN '💎 Loyal Customers'
        WHEN total_rfm_score >= 7  THEN '🌟 Potential Loyalists'
        WHEN total_rfm_score >= 5  THEN '⚠️  At Risk'
        ELSE                            '❌ Lost Customers'
    END                             AS rfm_label,
    COUNT(customer_id)              AS customers,
    ROUND(AVG(recency_days), 0)     AS avg_recency_days,
    ROUND(AVG(frequency), 1)        AS avg_orders,
    ROUND(AVG(monetary), 2)         AS avg_clv
FROM rfm_final
GROUP BY rfm_label
ORDER BY avg_clv DESC;

-- ============================================================
-- KPI 5: Q4 (HOLIDAY SEASON) vs REST OF YEAR
-- ============================================================
SELECT
    CASE WHEN MONTH(order_date) IN (10,11,12) THEN 'Q4 (Holiday)'
         ELSE 'Q1-Q3 (Off-Season)' END              AS season,
    COUNT(order_id)                                  AS total_orders,
    ROUND(SUM(total_amount), 2)                      AS total_revenue,
    ROUND(AVG(total_amount), 2)                      AS avg_order_value,
    ROUND(AVG(discount_pct), 2)                      AS avg_discount_pct,
    ROUND(AVG(customer_rating), 2)                   AS avg_rating,
    ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER(), 2) AS revenue_share_pct
FROM fact_orders
WHERE order_status NOT IN ('Cancelled')
GROUP BY season;

-- ============================================================
-- KPI 6: PAYMENT METHOD PREFERENCES
-- ============================================================
SELECT
    payment_method,
    COUNT(order_id)                                           AS order_count,
    ROUND(SUM(total_amount), 2)                               AS total_revenue,
    ROUND(AVG(total_amount), 2)                               AS avg_order_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)        AS usage_pct,
    ROUND(AVG(customer_rating), 2)                            AS avg_rating
FROM fact_orders
GROUP BY payment_method
ORDER BY order_count DESC;

-- ============================================================
-- KPI 7: TOP CUSTOMERS BY REVENUE (Power Users)
-- ============================================================
SELECT
    f.customer_id,
    c.customer_segment,
    c.region,
    COUNT(f.order_id)                   AS total_orders,
    ROUND(SUM(f.total_amount), 2)       AS lifetime_value,
    ROUND(AVG(f.total_amount), 2)       AS avg_order_value,
    ROUND(AVG(f.customer_rating), 2)    AS avg_rating,
    MAX(f.order_date)                   AS last_order_date
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'Delivered'
GROUP BY f.customer_id, c.customer_segment, c.region
ORDER BY lifetime_value DESC
LIMIT 20;

-- ============================================================
-- KPI 8: EXECUTIVE DASHBOARD SUMMARY VIEW
-- ============================================================
CREATE OR REPLACE VIEW vw_executive_summary AS
SELECT
    'Total Revenue'         AS metric,
    CONCAT('$', FORMAT(SUM(total_amount),2)) AS value
FROM fact_orders WHERE order_status NOT IN ('Cancelled')
UNION ALL
SELECT 'Total Orders', FORMAT(COUNT(*), 0)
FROM fact_orders
UNION ALL
SELECT 'Unique Customers', FORMAT(COUNT(DISTINCT customer_id), 0)
FROM fact_orders
UNION ALL
SELECT 'Average Order Value', CONCAT('$', FORMAT(AVG(total_amount),2))
FROM fact_orders WHERE order_status NOT IN ('Cancelled')
UNION ALL
SELECT 'Return Rate %', CONCAT(ROUND(SUM(CASE WHEN order_status='Returned' THEN 1 ELSE 0 END)*100.0/COUNT(*),2),'%')
FROM fact_orders
UNION ALL
SELECT 'Avg Customer Rating', FORMAT(AVG(customer_rating),2)
FROM fact_orders WHERE customer_rating IS NOT NULL;

SELECT * FROM vw_executive_summary;
