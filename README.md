

# Customer Lifecycle Decay Detection & Proactive Retention Targeting
### SQL-Driven CRM Strategy | Brazilian E-Commerce Dataset (Olist)

> **Tech Stack:** MySQL 8.0 · MySQL Workbench  
> **Dataset:** Brazilian E-Commerce Public Dataset by Olist (2016–2018)  
> **Domain:** E-Commerce · Customer Retention · CRM Analytics

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Problem Statement](#2-business-problem-statement)
3. [Dataset Overview](#3-dataset-overview)
4. [Methodology](#4-methodology)
5. [Key SQL Concepts Explained](#5-key-sql-concepts-explained)
6. [Key Findings & Insights](#6-key-findings--insights)
7. [Business Impact & Recommendations](#7-business-impact--recommendations)
8. [Limitations & Future Scope](#8-limitations--future-scope)
9. [How to Reproduce](#9-how-to-reproduce)
10. [SQL File Index](#10-sql-file-index)

---

## 1. Executive Summary

Most e-commerce businesses treat customer loss as a sudden event — but in reality, churn is a slow drift. This project uses real transactional data from Olist, a Brazilian e-commerce platform (99,441 customers, 2016–2018), to detect that drift before it becomes permanent.

Using pure SQL, the analysis identifies the **2,800 repeat buyers** who matter most, calculates each customer's normal buying rhythm, and flags anyone whose next purchase is overdue. Customers are then sorted into four risk tiers — Active, At Risk, Churning, and Lost — and assigned a **Reactivation Urgency Score** that tells the marketing team exactly *who* to call, *when* to email, and *where* to focus budget for maximum return.

The output is a prioritized, action-ready CRM list — no guesswork, no spray-and-pray campaigns.

---

## 2. Business Problem Statement

### The Reactive Retention Trap

Most CRM teams intervene *after* a customer has already gone quiet for months. By that point, re-engagement costs are high and conversion rates are low. The customer has moved on.

> **The core insight:** Customers don't churn suddenly. They decay gradually — and the decay follows a measurable pattern tied to their own purchase history.

### Why Reactive Retention Fails

| Reactive Approach | Problem |
|---|---|
| Email entire inactive list | Low relevance, high unsubscribe rate |
| Offer discounts to everyone | Margin erosion; rewards customers who would have returned anyway |
| Wait for 90-day inactivity threshold | Too late — customer is already Lost |
| Treat all inactive customers equally | Ignores revenue value and behavioral context |

### The Proactive Alternative

This project shifts the retention strategy from **"Who hasn't ordered in X days?"** to **"Who is deviating from *their own* normal buying cycle, and by how much?"**

This distinction is critical. A customer who buys every 90 days being silent for 100 days is a different signal than a customer who buys every 20 days being silent for the same 100 days. Standard inactivity windows cannot capture this nuance — but per-customer behavioral baselines can.

---

## 3. Dataset Overview

### Source

**Brazilian E-Commerce Public Dataset by Olist**  
Available on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Scale

| Metric | Count |
|---|---|
| Total Customers | 99,441 |
| Delivered Orders | 96,461 |
| Order Items | 112,650 |
| Payment Records | 103,886 |
| Date Range | September 2016 – August 2018 |

### Tables Used

| Table | Key Columns | Description |
|---|---|---|
| `olist_customers` | `customer_id`, `customer_unique_id`, `customer_state` | Maps transactional IDs to true customer identities |
| `olist_orders` | `order_id`, `customer_id`, `order_purchase_timestamp`, `order_status` | Order lifecycle events with timestamps |
| `olist_order_items` | `order_id`, `price`, `freight_value` | Line-item revenue data per order |
| `olist_order_payments` | `order_id`, `payment_value` | Actual payment amounts per order |

### Key Relationships

```
olist_customers ──< olist_orders ──< olist_order_items
                                 └─< olist_order_payments
```

- `olist_customers.customer_id` → `olist_orders.customer_id` (one-to-many)
- `olist_orders.order_id` → `olist_order_items.order_id` (one-to-many)
- `olist_orders.order_id` → `olist_order_payments.order_id` (one-to-many)

### Critical Data Trap: `customer_id` vs `customer_unique_id`

This is the single most important data quality issue in the entire dataset.

**The Problem:**  
`customer_id` in the `olist_orders` table is **not** a stable customer identifier. It is generated fresh for every order. A customer who places 3 orders will have 3 different `customer_id` values — making it appear as if they are 3 separate customers.

**The Impact:**  
If analysis is anchored on `customer_id`, the entire repeat-buyer cohort disappears. Every customer looks like a one-time buyer. Inter-purchase gap analysis becomes impossible.

**The Fix:**  
All analysis in this project is anchored on `customer_unique_id` from the `olist_customers` table — the true, stable customer identifier that persists across orders.

```sql
-- WRONG: customer_id resets per order
SELECT customer_id, COUNT(*) AS orders FROM olist_orders GROUP BY customer_id;

-- CORRECT: customer_unique_id identifies the real person
SELECT c.customer_unique_id, COUNT(*) AS orders
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_unique_id;
```

---

## 4. Methodology

The project is structured as a **5-layer analytical pipeline**, where each layer builds on the output of the previous one.

---

### Layer 1 — Schema Setup & Data Ingestion

**Objective:** Create a clean, correctly-typed relational schema ready for analysis.

- Designed and created the relational schema in **MySQL Workbench**
- Imported 4 source CSVs with explicit column type definitions
- Enforced `DATETIME` typing on all timestamp columns (critical for `DATEDIFF()` operations downstream)
- Validated row counts post-import against source file records to catch truncation or encoding errors

**Why this matters:** Importing timestamps as `VARCHAR` — MySQL's default fallback — would silently break every date arithmetic operation in Layers 3 and 4. Explicit typing at ingestion prevents this class of error entirely.

---

### Layer 2 — Data Understanding & Cohort Isolation

**Objective:** Understand the true shape of the customer base and isolate the analytically relevant cohort.

Key discoveries:
- **97% of customers are one-time buyers** — they have no inter-purchase gap to analyze and are excluded from behavioral modeling
- The `customer_id` trap (described in Section 3) was identified and corrected here
- **2,800 repeat buyers** were isolated as the analysis cohort — these are the customers with a measurable buying cycle and the highest lifetime value potential

```sql
-- Isolating repeat buyers
SELECT customer_unique_id, COUNT(DISTINCT order_id) AS order_count
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE order_status = 'delivered'
GROUP BY customer_unique_id
HAVING order_count > 1;
```

---

### Layer 3 — Inter-Purchase Gap Analysis

**Objective:** Calculate each customer's personal buying rhythm — their average number of days between consecutive purchases.

**Approach:**
1. Used `ROW_NUMBER()` to rank each customer's orders chronologically
2. Used `LAG()` to pull the previous order date alongside the current order date
3. Applied `DATEDIFF()` to calculate the gap in days between consecutive purchases
4. Applied a **noise filter**: gaps ≤ 7 days were excluded (these represent split shipments or bundle orders, not genuine repeat purchase behavior)
5. Averaged the filtered gaps to produce each customer's **behavioral baseline** — their expected purchase cycle length

```sql
-- Simplified structure of gap calculation CTE
WITH ranked_orders AS (
    SELECT
        customer_unique_id,
        order_purchase_timestamp,
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) AS order_rank,
        LAG(order_purchase_timestamp) OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) AS prev_order_date
    FROM ...
),
gaps AS (
    SELECT
        customer_unique_id,
        DATEDIFF(order_purchase_timestamp, prev_order_date) AS gap_days
    FROM ranked_orders
    WHERE prev_order_date IS NOT NULL
      AND DATEDIFF(order_purchase_timestamp, prev_order_date) > 7
)
SELECT customer_unique_id, AVG(gap_days) AS avg_purchase_cycle
FROM gaps
GROUP BY customer_unique_id;
```

---

### Layer 4 — Decay Detection & Risk Classification

**Objective:** Measure how far each customer has drifted past their expected purchase date and classify them into risk tiers.

**Key design decisions:**
- Used `MAX(order_purchase_timestamp)` from the orders table as a **dynamic reference date** — avoiding hardcoded dates that would make the analysis non-reproducible
- Injected this reference date via a `CROSS JOIN` against a single-row subquery
- Calculated **% overdue** = `(days since last purchase ÷ avg purchase cycle) × 100`

**Risk Tier Classification:**

| Tier | % Overdue Threshold | Meaning |
|---|---|---|
|  Active | ≤ 100% | Within or near their normal buying window |
|  At Risk | ≤ 200% | Overdue by up to 1 full cycle — intervention window open |
|  Churning | ≤ 400% | Seriously overdue — urgency is high |
|  Lost | > 400% | Likely permanently lapsed |

```sql
CASE
    WHEN pct_overdue <= 100  THEN ' Active'
    WHEN pct_overdue <= 200  THEN ' At Risk'
    WHEN pct_overdue <= 400  THEN ' Churning'
    ELSE                          ' Lost'
END AS risk_tier
```

---

### Layer 5 — Reactivation Urgency Score

**Objective:** Produce a single, rankable score per customer that combines behavioral risk with revenue value — enabling prioritized CRM action.

**Composite Score Formula:**

```
Urgency Score = (pct_overdue × 0.50) + (total_revenue/10 × 0.30) + (total_orders×10 × 0.20)
```

| Component | Weight | Rationale |
|---|---|---|
| % Overdue | 50% | Primary signal — how urgently intervention is needed |
| Total Revenue (÷10) | 30% | Prioritize high-value customers over low-value ones |
| Total Orders (×10) | 20% | Reward behavioral loyalty as a signal of re-engagement potential |

**CRM Action Mapping:**

| Score Threshold | Recommended Action |
|---|---|
| ≥ 300 |  Immediate High-Touch Call |
| ≥ 150 |  Priority Discount Email Campaign |
| ≥ 50 |  Monitor / Nurture Campaign |
| < 50 |  No Action Needed |

---

## 5. Key SQL Concepts Explained

### `LAG()` — Window Function

**What it does:** Returns the value from the previous row within a defined partition, without collapsing rows like `GROUP BY` would.

**Why used here:** Enables side-by-side comparison of a customer's current order date with their previous order date — the foundation of gap calculation. No self-join required.

---

### `ROW_NUMBER()` — Window Function

**What it does:** Assigns a sequential integer to each row within a partition, ordered by a specified column.

**Why used here:** Establishes chronological purchase order per customer. Ensures `LAG()` pulls the *correct* previous order, not an arbitrary one.

---

### `SUM() OVER()` — Running Window Aggregate

**What it does:** Computes a cumulative or partitioned sum without collapsing rows.

**Why used here:** Used to calculate total revenue per customer across all their orders while preserving row-level order details.

---

### Multi-Layer CTEs (5 Chained)

**What it does:** Common Table Expressions (`WITH` clauses) allow naming intermediate result sets and referencing them in subsequent steps — like named variables in a query.

**Why used here:** Each analytical layer (gap calculation, baseline building, decay scoring, tier classification, urgency scoring) is a separate CTE. This makes the logic auditable, modular, and debuggable — as opposed to nesting five subqueries.

```sql
WITH
  cohort        AS (...),          -- Layer 2: repeat buyers
  gaps          AS (...),          -- Layer 3a: raw purchase gaps
  baseline      AS (...),          -- Layer 3b: avg cycle per customer
  decay         AS (...),          -- Layer 4: pct_overdue + risk tier
  urgency       AS (...)           -- Layer 5: composite score + CRM action
SELECT * FROM urgency ORDER BY urgency_score DESC;
```

---

### `DATEDIFF()` — Date Arithmetic

**What it does:** Returns the integer number of days between two date values.

**Why used here:** Converts timestamp differences into human-interpretable, arithmetically useful day counts. Enables both gap calculation and overdue calculation.

---

### `CROSS JOIN` — Reference Date Injection

**What it does:** Produces a cartesian product — every row in table A combined with every row in table B. When table B is a single-row subquery, it effectively broadcasts that value to every row.

**Why used here:** The dataset's `MAX(order_purchase_timestamp)` is calculated once and joined to the entire customer table without a matching key — making the dynamic reference date available to every row without hardcoding it.

```sql
CROSS JOIN (SELECT MAX(order_purchase_timestamp) AS ref_date FROM olist_orders) AS ref
```

---

### `CASE WHEN` — Conditional Logic

**What it does:** Implements if-then-else branching inside SQL queries.

**Why used here:** Two applications — (1) mapping continuous `pct_overdue` values to discrete risk tier labels, and (2) mapping continuous urgency scores to discrete CRM action labels. Keeps classification logic inside the query rather than in downstream tools.

---

### `HAVING` — Post-Aggregation Filtering

**What it does:** Filters groups *after* aggregation — unlike `WHERE`, which filters rows before aggregation.

**Why used here:** Used to isolate the repeat-buyer cohort by filtering for `COUNT(DISTINCT order_id) > 1` — a condition that can only be evaluated after orders are grouped by customer.

---

## 6. Key Findings & Insights

> **Note:** Replace bracketed placeholders with values from your actual query output.

### Customer Base Composition

| Segment | Count | % of Total |
|---|---|---|
| Total unique customers | 99,441 | 100% |
| One-time buyers | ~96,641 | ~97% |
| Repeat buyers (analysis cohort) | ~2,800 | ~3% |

### Risk Tier Distribution (Repeat Buyers)

| Risk Tier | Customer Count | % of Cohort | Avg Revenue per Customer |
|---|---|---|---|
|  Active | `[X]` | `[X]%` | R$ `[X]` |
|  At Risk | `[X]` | `[X]%` | R$ `[X]` |
|  Churning | `[X]` | `[X]%` | R$ `[X]` |
|  Lost | `[X]` | `[X]%` | R$ `[X]` |

### Urgency Score Distribution

| CRM Action | Customer Count | Est. Revenue at Risk |
|---|---|---|
|  High-Touch Call (Score ≥ 300) | `[X]` | R$ `[X]` |
|  Priority Email (Score ≥ 150) | `[X]` | R$ `[X]` |
|  Nurture Campaign (Score ≥ 50) | `[X]` | R$ `[X]` |
|  No Action (Score < 50) | `[X]` | — |

### Behavioral Insights

- **Average inter-purchase cycle** across repeat buyers: `[X]` days
- **Shortest avg cycle** (most frequent buyer): `[X]` days
- **Longest avg cycle** (slowest repeat buyer): `[X]` days
- **Top customer by urgency score:** `[customer_unique_id]` — Score: `[X]`, Revenue: R$ `[X]`, Tier: `[X]`
- **Largest at-risk revenue concentration:** `[X]%` of at-risk revenue is concentrated in `[X]%` of at-risk customers

---

## 7. Business Impact & Recommendations

### For the Marketing Team

#### Immediate Actions (This Week)

- **Pull the High-Touch Call list** (Score ≥ 300): These are high-revenue customers whose buying cycle has elapsed by 3–4×. Every additional day of inaction reduces re-engagement probability. Assign to outbound sales or senior CRM agents.

- **Deploy Priority Discount Email** (Score ≥ 150): Personalize offers based on their previous purchase categories. A blanket 10% discount is less effective than a category-specific offer tied to their actual purchase history.

#### Structural Recommendations

| Recommendation | Rationale |
|---|---|
| **Shift retention KPI from "inactive days" to "% overdue vs personal baseline"** | Standard 30/60/90 day windows treat all customers the same — this model doesn't |
| **Establish a tiered intervention budget** | High-Touch Calls cost more; reserve them for Score ≥ 300 only — don't waste them on low-value lapsed customers |
| **Run A/B test on intervention timing** | Test intervening at 110% overdue vs 150% overdue to find the optimal trigger point |
| **Do not target Lost customers with discounts** | Re-acquisition cost typically exceeds lifetime value for this segment; redirect that budget to At Risk customers |
| **Track cohort movement monthly** | A customer moving from At Risk → Churning within one reporting cycle signals accelerated decay and warrants immediate escalation |

#### Revenue Opportunity Sizing

The repeat-buyer cohort — while only ~3% of all customers — typically represents a disproportionate share of total revenue. Retaining even `[X]%` of At Risk and Churning customers through targeted intervention is estimated to protect R$ `[X]` in repeat revenue annually. *(Replace with actual figures from your query output.)*

---

## 8. Limitations & Future Scope

### Current Limitations

| Limitation | Impact |
|---|---|
| **Static analysis** — run once against a snapshot | Does not update as new orders arrive; requires re-execution |
| **Simple avg baseline** — sensitive to outliers | A customer with 1 very long gap + 1 short gap gets an unreliable average cycle |
| **No product/category context** | Two customers with the same gap profile but different categories may warrant different interventions |
| **No channel data** | Cannot distinguish customers who are still browsing (low churn risk) from those who have gone fully silent |
| **No external factors** | Seasonality, promotions, and platform-wide events are not controlled for |
| **2018 data** | Dataset is historical; absolute date values are not current, though the methodology is fully transferable |

### Future Scope

#### With Python / Pandas
- Automate monthly pipeline execution and delta reporting
- Plot risk tier migration over time (Sankey diagram)
- Build customer-level trend lines — is the gap growing or shrinking?

#### With Machine Learning
- **Survival Analysis (Kaplan-Meier / Cox PH):** Model the *probability* of a customer making their next purchase at each future time point — more statistically rigorous than threshold-based classification
- **Clustering (K-Means / DBSCAN):** Segment repeat buyers by behavioral profile, not just risk level — enables truly personalized interventions
- **Churn Probability Scoring (Logistic Regression / XGBoost):** Train a model on labeled outcomes (did the customer return or not?) to produce a calibrated churn probability rather than a rule-based tier

#### With a Production Data Stack
- Connect to live order stream (Kafka / Airflow pipeline)
- Real-time risk tier updates in CRM (Salesforce / HubSpot integration)
- Automated trigger: when a customer crosses a tier boundary, fire the appropriate CRM workflow automatically

---

## 9. How to Reproduce

### Prerequisites

- MySQL 8.0+ installed
- MySQL Workbench (or any MySQL-compatible client)
- The Olist dataset CSVs downloaded from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Step 1 — Create the Schema

```sql
CREATE DATABASE olist_ecommerce;
USE olist_ecommerce;
```

Run `01_schema_setup.sql` to create all four tables with correct data types.

### Step 2 — Import the CSV Files

Import the following files using MySQL Workbench's Table Data Import Wizard or `LOAD DATA INFILE`:

| CSV File | Target Table |
|---|---|
| `olist_customers_dataset.csv` | `olist_customers` |
| `olist_orders_dataset.csv` | `olist_orders` |
| `olist_order_items_dataset.csv` | `olist_order_items` |
| `olist_order_payments_dataset.csv` | `olist_order_payments` |

> **Important:** Ensure `order_purchase_timestamp`, `order_approved_at`, `order_delivered_customer_date`, and `order_estimated_delivery_date` are imported as `DATETIME`, not `VARCHAR`.

### Step 3 — Validate Row Counts

```sql
SELECT 'customers'     AS tbl, COUNT(*) FROM olist_customers
UNION ALL
SELECT 'orders',                COUNT(*) FROM olist_orders
UNION ALL
SELECT 'order_items',           COUNT(*) FROM olist_order_items
UNION ALL
SELECT 'order_payments',        COUNT(*) FROM olist_order_payments;
```

Expected output:

| tbl | COUNT(*) |
|---|---|
| customers | 99,441 |
| orders | 99,441 |
| order_items | 112,650 |
| order_payments | 103,886 |

### Step 4 — Run the Analysis

Execute the SQL files **in order**:

```
02_data_understanding.sql
03_gap_analysis.sql
04_decay_classification.sql
05_urgency_scoring.sql
```

### Step 5 — Export the Output

The final `SELECT` in `05_urgency_scoring.sql` produces the full CRM action list. Export to CSV via:

```
MySQL Workbench → Query Results → Export → CSV
```

---

## 10. SQL File Index

| File | Layer | Description |
|---|---|---|
| `01_schema_setup.sql` | Layer 1 | Creates the `olist_ecommerce` database and all four tables with explicit column types. Includes `DROP TABLE IF EXISTS` guards for safe re-runs. |
| `02_data_understanding.sql` | Layer 2 | Exploratory queries: total customer counts, one-time vs repeat buyer breakdown, `customer_id` vs `customer_unique_id` comparison, delivered order filter, and repeat-buyer cohort isolation. |
| `03_gap_analysis.sql` | Layer 3 | CTE-based inter-purchase gap calculation using `LAG()` and `ROW_NUMBER()`. Includes noise filter (≤ 7-day gaps excluded) and per-customer average cycle computation. |
| `04_decay_classification.sql` | Layer 4 | Joins gap baseline to current customer status. Calculates `days_since_last_order` via `CROSS JOIN` reference date injection, computes `pct_overdue`, and applies `CASE WHEN` risk tier classification. |
| `05_urgency_scoring.sql` | Layer 5 | Full 5-CTE pipeline. Computes composite urgency score, maps to CRM action label, and produces the final ranked output table ready for export to CRM or marketing tools. |

---
