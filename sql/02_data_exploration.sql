-- ============================================================
-- FILE: 02_data_exploration.sql
-- PROJECT: E-Commerce Sales Analytics
-- AUTHOR: Subha Ragupathi
-- DESCRIPTION: Data profiling and exploratory SQL queries
-- ============================================================

USE ecommerce_db;

-- ============================================================
-- 1. DATASET OVERVIEW
-- ============================================================
SELECT
    COUNT(*)                          AS total_orders,
    COUNT(DISTINCT customer_id)       AS unique_customers,
    MIN(order_date)                   AS first_order_date,
    MAX(order_date)                   AS last_order_date,
    ROUND(SUM(total_amount), 2)       AS total_revenue,
    ROUND(AVG(total_amount), 2)       AS avg_order_value,
    ROUND(MIN(total_amount), 2)       AS min_order_value,
    ROUND(MAX(total_amount), 2)       AS max_order_value
FROM fact_orders;

-- ============================================================
-- 2. NULL / MISSING VALUE CHECK
-- ============================================================
SELECT
    SUM(CASE WHEN order_id       IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id    IS NULL THEN 1 ELSE 0 END) AS null_customer,
    SUM(CASE WHEN unit_price     IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN total_amount   IS NULL THEN 1 ELSE 0 END) AS null_total,
    SUM(CASE WHEN customer_rating IS NULL THEN 1 ELSE 0 END) AS null_rating
FROM fact_orders;

-- ============================================================
-- 3. ORDER STATUS DISTRIBUTION
-- ============================================================
SELECT
    order_status,
    COUNT(*)                                  AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
    ROUND(SUM(total_amount), 2)               AS total_revenue,
    ROUND(AVG(customer_rating), 2)            AS avg_rating
FROM fact_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- ============================================================
-- 4. REVENUE BY CATEGORY (with running total)
-- ============================================================
SELECT
    p.category,
    COUNT(f.order_id)                                       AS total_orders,
    ROUND(SUM(f.total_amount), 2)                           AS total_revenue,
    ROUND(AVG(f.total_amount), 2)                           AS avg_order_value,
    ROUND(SUM(f.total_amount) * 100.0
          / SUM(SUM(f.total_amount)) OVER(), 2)             AS revenue_share_pct,
    ROUND(SUM(SUM(f.total_amount)) OVER
          (ORDER BY SUM(f.total_amount) DESC), 2)           AS running_total_revenue
FROM fact_orders f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- ============================================================
-- 5. MONTHLY REVENUE TREND (PIVOT-STYLE)
-- ============================================================
SELECT
    YEAR(order_date)          AS year,
    MONTH(order_date)         AS month,
    MONTHNAME(order_date)     AS month_name,
    COUNT(order_id)           AS total_orders,
    ROUND(SUM(total_amount), 2) AS monthly_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    -- Month-over-month growth
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY YEAR(order_date), MONTH(order_date)))
        / NULLIF(LAG(SUM(total_amount)) OVER (ORDER BY YEAR(order_date), MONTH(order_date)), 0) * 100
    , 2) AS mom_growth_pct
FROM fact_orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- ============================================================
-- 6. CUSTOMER SEGMENT PERFORMANCE
-- ============================================================
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id)                       AS unique_customers,
    COUNT(f.order_id)                                   AS total_orders,
    ROUND(COUNT(f.order_id) * 1.0
          / COUNT(DISTINCT c.customer_id), 2)           AS orders_per_customer,
    ROUND(SUM(f.total_amount), 2)                       AS total_revenue,
    ROUND(AVG(f.total_amount), 2)                       AS avg_order_value,
    ROUND(AVG(f.discount_pct), 2)                       AS avg_discount_pct,
    ROUND(AVG(f.customer_rating), 2)                    AS avg_rating
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- ============================================================
-- 7. SALES CHANNEL EFFECTIVENESS
-- ============================================================
SELECT
    sales_channel,
    COUNT(order_id)                                           AS total_orders,
    ROUND(SUM(total_amount), 2)                               AS total_revenue,
    ROUND(AVG(total_amount), 2)                               AS avg_order_value,
    ROUND(SUM(CASE WHEN order_status='Returned'  THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)  AS return_rate_pct,
    ROUND(SUM(CASE WHEN order_status='Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)  AS cancel_rate_pct,
    ROUND(AVG(customer_rating), 2)                            AS avg_rating
FROM fact_orders
GROUP BY sales_channel
ORDER BY total_revenue DESC;

-- ============================================================
-- 8. DISCOUNT EFFECTIVENESS ANALYSIS
-- ============================================================
SELECT
    discount_pct,
    COUNT(order_id)                 AS order_count,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(total_amount), 2)     AS avg_revenue,
    ROUND(SUM(discount_amount), 2)  AS total_discount_given,
    ROUND(AVG(customer_rating), 2)  AS avg_rating
FROM fact_orders
GROUP BY discount_pct
ORDER BY discount_pct;

-- ============================================================
-- 9. REGIONAL PERFORMANCE BREAKDOWN
-- ============================================================
SELECT
    c.region,
    p.category,
    COUNT(f.order_id)               AS orders,
    ROUND(SUM(f.total_amount), 2)   AS revenue,
    ROUND(AVG(f.total_amount), 2)   AS aov,
    RANK() OVER (
        PARTITION BY c.region
        ORDER BY SUM(f.total_amount) DESC
    )                               AS category_rank_in_region
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_products  p ON f.product_key  = p.product_key
GROUP BY c.region, p.category
ORDER BY c.region, category_rank_in_region;

-- ============================================================
-- 10. PRODUCT PERFORMANCE — TOP 10
-- ============================================================
SELECT
    p.category,
    p.product_name,
    COUNT(f.order_id)               AS total_orders,
    SUM(f.quantity)                 AS total_units_sold,
    ROUND(SUM(f.total_amount), 2)   AS total_revenue,
    ROUND(AVG(f.total_amount), 2)   AS avg_order_value,
    ROUND(AVG(f.customer_rating),2) AS avg_rating
FROM fact_orders f
JOIN dim_products p ON f.product_key = p.product_key
WHERE f.order_status = 'Delivered'
GROUP BY p.category, p.product_name
ORDER BY total_revenue DESC
LIMIT 10;
