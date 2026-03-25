# Dance Studio Analytics Platform (dbt + DuckDB)

## 📌 Project Overview
This project is a comprehensive data engineering and analytics solution for a dance studio network. It transforms raw data concerning classes, payments, and dancers into actionable business insights using **dbt (Data Build Tool)** with **DuckDB** as the high-performance analytical engine.

The platform is designed with modularity, scalability, and data quality as core principles, following modern Analytics Engineering best practices.

---

## 🏗 Data Architecture
The project follows a multi-layered (Medallion) architecture to ensure a structured flow of information:

1.  **Seeds Layer:** Source data provided as CSV files located in the `seeds/` directory.
2.  **Staging Layer:** Initial transformation of raw seeds, including column renaming, type casting, and basic cleaning.
3.  **Mart Layer:** Business-ready models, including Dimensional models (`dim_`) and Fact tables (`fct_`), designed for reporting.

## 📂 Business Context & Data Entities
To fulfill the requirements of the analytical platform, the following business domain and entities were defined:

* **Business Domain**: Dance Studio Management.
* **Core Entities**: Dancers, Coaches, Teams, Classes (types and schedules), and Payments.
* **Business Goals**: Tracking revenue growth, monitoring dancer attendance, and evaluating coach performance.

---

## 💡 Data Insights & Reporting
The final **Mart Layer** provides answers to key business questions through transformed models:

* **Top Customers by Loyalty**: Using the `metrics_dancers` model, we can identify the top students based on their attendance rank and frequency.
* **Revenue Trend Analysis**: The `monthly_revenue_by_class_type` model reveals which dance styles drive the highest Month-over-Month (MoM) growth.
* **Coach Efficiency**: Analytical models allow for tracking the total monthly revenue generated per coach.
## 📊 Lineage Graph
*The project consists of 21 models, maintaining clear dependencies from raw sources to analytical outputs.*
<img width="1244" height="611" alt="Screenshot 2026-03-25 at 12 38 24" src="https://github.com/user-attachments/assets/ad097785-6d3a-4cb6-9cf5-19fa218af3cd" />

---

## 🛠 Technical Implementation

### 1. Incremental Strategy (Performance Optimization)
The project implements **5+ incremental models** to handle data efficiently.
- **Materialization:** `incremental` with a `delete+insert` strategy for the `monthly_revenue_by_class_type` model.
- **Incremental Predicate:** To optimize performance, I implemented a predicate logic that only processes records from the last 2 months:
  ```sql
  where payment_date >= date_trunc('month', current_date) - interval '2 month'

#### 2. Custom Macros
| Macro | Description | Usage Example |
|:--- |:--- |:--- |
| **`calculate_age`** | Automates age calculation based on birth dates. | `{{ calculate_age('d.birth_date') }} as age` |
| **`get_attendance_category`** | Encapsulates business logic for student segmentation. | `{{ get_attendance_category('m.attendance_rate') }} as attendance_category` |

#### 3. Advanced Analytics (Window Functions)
* **Dancer Loyalty Ranking**: Used `rank() over (partition by ...)` in the `metrics_dancers` model to identify top-performing students.
* **Revenue Dynamics**: Used `lag()` in the revenue mart to calculate **Month-over-Month (MoM) Growth**.

#### 4. Data Quality & Advanced Testing
**`accepted_values_from_table`**: A custom generic test implemented to ensure data integrity by validating that column values exist within a filtered subset of a reference table.

```sql
{% test accepted_values_from_table(model, column_name, other_model, other_column, filter_key, filter_value) %}
  select
    {{ column_name }}
  from {{ model }}
  where {{ column_name }} is not null and
      {{ column_name }} not in (select {{ other_column }} from {{ ref(other_model) }}
                                                          where {{ filter_key }} = '{{ filter_value }}')
{% endtest %}
