-- Create a dedicated schema for this project
CREATE DATABASE IF NOT EXISTS olist_ecommerce;
USE olist_ecommerce;


-- 1. Customers Table
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

-- 2. Orders Table (most important for this project)
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 3. Order Items Table
CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

-- 4. Order Payments Table
CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(30),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);

USE olist_ecommerce;

-- Validation 
SELECT 'customers'   AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'orders',     COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'order_payments', COUNT(*) FROM order_payments;

-- How has revenue trended month over month?

WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
        COUNT(DISTINCT o.order_id)                        AS total_orders,
        ROUND(SUM(p.payment_value), 2)                    AS total_revenue
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
)
SELECT
    order_month,
    total_orders,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY order_month)   AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY order_month))
        * 100.0
        / LAG(total_revenue) OVER (ORDER BY order_month),
    1)                                               AS mom_growth_pct
FROM monthly_revenue
ORDER BY order_month;

-- What is our cumulative revenue at any point in the year?

WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
        ROUND(SUM(p.payment_value), 2)                    AS total_revenue
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
)
SELECT
    order_month,
    total_revenue,
    ROUND(SUM(total_revenue) OVER (
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                              AS cumulative_revenue
FROM monthly_revenue
ORDER BY order_month;

-- Which product categories drive the most revenue?

USE olist_ecommerce;

USE olist_ecommerce;

WITH category_revenue AS (
    SELECT
        p.product_category_name             AS category,
        COUNT(DISTINCT oi.order_id)         AS total_orders,
        ROUND(SUM(oi.price), 2)             AS total_revenue
    FROM order_items oi
    JOIN olist_products_dataset p
        ON oi.product_id = p.product_id
    WHERE p.product_category_name IS NOT NULL
    GROUP BY p.product_category_name
),
total AS (
    SELECT SUM(total_revenue) AS grand_total
    FROM category_revenue
)
SELECT
    cr.category,
    cr.total_orders,
    cr.total_revenue,
    RANK() OVER (ORDER BY cr.total_revenue DESC)        AS revenue_rank,
    ROUND(cr.total_revenue * 100.0 / t.grand_total, 2) AS pct_of_total
FROM category_revenue cr
CROSS JOIN total t
ORDER BY revenue_rank
LIMIT 15;

-- How do we segment customers into High, Mid, and Low value tiers?
WITH customer_value AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id)         AS total_orders,
        ROUND(SUM(p.payment_value), 2)     AS total_spent
    FROM orders o
    JOIN customers c        ON o.customer_id = c.customer_id
    JOIN order_payments p   ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
segmented AS (
    SELECT
        customer_unique_id,
        total_orders,
        total_spent,
        NTILE(3) OVER (ORDER BY total_spent DESC) AS value_tile
    FROM customer_value
)
SELECT
    customer_unique_id,
    total_orders,
    total_spent,
    value_tile,
    CASE value_tile
        WHEN 1 THEN ' High Value'
        WHEN 2 THEN ' Mid Value'
        WHEN 3 THEN ' Low Value'
    END                                    AS customer_segment
FROM segmented
ORDER BY total_spent DESC
LIMIT 30;

-- Which days of the week see the most orders? (For campaign scheduling)

SELECT
    DAYNAME(order_purchase_timestamp)   AS day_of_week,
    DAYOFWEEK(order_purchase_timestamp) AS day_number,
    COUNT(*)                            AS total_orders,
    ROUND(SUM(p.payment_value), 2)      AS total_revenue,
    RANK() OVER (
        ORDER BY COUNT(*) DESC
    )                                   AS order_volume_rank
FROM orders o
JOIN order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY
    DAYNAME(order_purchase_timestamp),
    DAYOFWEEK(order_purchase_timestamp)
ORDER BY day_number;

-- What is next month's expected order volume compared to current month?

WITH monthly_orders AS (
    SELECT
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS order_month,
        COUNT(DISTINCT order_id)                        AS total_orders
    FROM orders
    WHERE order_status = 'delivered'
    GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m')
)
SELECT
    order_month,
    total_orders,
    LAG(total_orders)  OVER (ORDER BY order_month) AS prev_month_orders,
    LEAD(total_orders) OVER (ORDER BY order_month) AS next_month_orders,
    ROUND(
        (total_orders - LAG(total_orders) OVER (ORDER BY order_month))
        * 100.0
        / LAG(total_orders) OVER (ORDER BY order_month),
    1)                                             AS mom_growth_pct
FROM monthly_orders
ORDER BY order_month;

-- What was each repeat customer's first and most recent purchase date — and how long have they been active?

WITH repeat_buyers AS (
    SELECT c.customer_unique_id
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
    HAVING COUNT(o.order_id) >= 2
),
purchase_history AS (
    SELECT
        c.customer_unique_id,
        DATE(o.order_purchase_timestamp) AS purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        ) AS purchase_rank
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND c.customer_unique_id IN (SELECT customer_unique_id FROM repeat_buyers)
)
SELECT
    customer_unique_id,
    MIN(purchase_date)                   AS first_purchase_date,
    MAX(purchase_date)                   AS latest_purchase_date,
    MAX(purchase_rank)                   AS total_purchases,
    DATEDIFF(
        MAX(purchase_date),
        MIN(purchase_date)
    )                                    AS customer_lifespan_days,
    ROUND(
        DATEDIFF(MAX(purchase_date), MIN(purchase_date))
        / NULLIF(MAX(purchase_rank) - 1, 0),
    0)                                   AS avg_days_between_purchases
FROM purchase_history
GROUP BY customer_unique_id
ORDER BY customer_lifespan_days DESC
LIMIT 30;

-- Is our revenue growth consistent or volatile? Show a smoothed trend.

WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
        ROUND(SUM(p.payment_value), 2)                    AS total_revenue
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
)
SELECT
    order_month,
    total_revenue,
    ROUND(
        AVG(total_revenue) OVER (
            ORDER BY order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
    2)                             AS rolling_3month_avg,
    ROUND(
        MAX(total_revenue) OVER (
            ORDER BY order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
    2)                             AS rolling_3month_max
FROM monthly_revenue
ORDER BY order_month;

-- Do high-value customers pay differently than low-value ones?

WITH customer_value AS (
    SELECT
        c.customer_unique_id,
        ROUND(SUM(p.payment_value), 2)    AS total_spent,
        NTILE(3) OVER (
            ORDER BY SUM(p.payment_value) DESC
        )                                 AS value_tier
    FROM orders o
    JOIN customers c        ON o.customer_id = c.customer_id
    JOIN order_payments p   ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
tier_payment AS (
    SELECT
        cv.value_tier,
        CASE cv.value_tier
            WHEN 1 THEN 'High Value'
            WHEN 2 THEN 'Mid Value'
            WHEN 3 THEN 'Low Value'
        END                               AS segment,
        p.payment_type,
        COUNT(*)                          AS usage_count
    FROM orders o
    JOIN customers c        ON o.customer_id = c.customer_id
    JOIN order_payments p   ON o.order_id = p.order_id
    JOIN customer_value cv  ON c.customer_unique_id = cv.customer_unique_id
    WHERE o.order_status = 'delivered'
    GROUP BY cv.value_tier, segment, p.payment_type
)
SELECT
    segment,
    payment_type,
    usage_count,
    RANK() OVER (
        PARTITION BY segment
        ORDER BY usage_count DESC
    )                                     AS rank_within_segment,
    ROUND(
        usage_count * 100.0
        / SUM(usage_count) OVER (PARTITION BY segment),
    1)                                    AS pct_within_segment
FROM tier_payment
ORDER BY segment, rank_within_segment;

-- Who are our top 20 sellers — ranked by revenue, volume, and average review score?

WITH seller_metrics AS (
    SELECT
        oi.seller_id,
        COUNT(DISTINCT oi.order_id)          AS total_orders,
        ROUND(SUM(oi.price), 2)              AS total_revenue,
        ROUND(AVG(r.review_score), 2)        AS avg_review_score,
        COUNT(DISTINCT oi.product_id)        AS unique_products
    FROM order_items oi
    LEFT JOIN olist_order_reviews_dataset r ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
)
SELECT
    seller_id,
    total_orders,
    total_revenue,
    avg_review_score,
    unique_products,
    RANK()       OVER (ORDER BY total_revenue   DESC) AS revenue_rank,
    RANK()       OVER (ORDER BY total_orders    DESC) AS volume_rank,
    RANK()       OVER (ORDER BY avg_review_score DESC) AS review_rank,
    ROUND(
        (
            RANK() OVER (ORDER BY total_revenue    DESC) * 0.50 +
            RANK() OVER (ORDER BY total_orders     DESC) * 0.30 +
            RANK() OVER (ORDER BY avg_review_score DESC) * 0.20
        ),
    0)                                                AS composite_rank_score
FROM seller_metrics
ORDER BY composite_rank_score ASC
LIMIT 20;