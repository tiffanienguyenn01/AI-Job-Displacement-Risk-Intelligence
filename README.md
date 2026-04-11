# AI Job Displacement Risk Intelligence Platform

## Executive Summary

This project explores workforce displacement patterns across 7 industries using SQL, Python, and an interactive Tableau dashboard to assess displacement rates and uncover key factors driving job outcomes in the age of AI. The study reveals significant variation in displacement rates and vulnerability scores across age groups, education levels, and industries, identifying high-risk populations and areas for targeted intervention.

A composite **Vulnerability Index** was engineered to predict displacement likelihood based on automation risk, AI adoption level, education, upskilling status, and age. Results reveal stark disparities: workers in high-risk roles with low AI adoption and no upskilling plan score near 10 out of 10, while workers in low-risk roles with active AI adoption score as low as 2 — an 8-point gap. Automation risk and AI adoption level emerged as the strongest predictors of displacement outcomes across all 2,000 survey respondents.

---

## Business Problem

AI-driven job displacement is accelerating across industries, yet most organizations and workers lack a systematic, data-driven way to identify who is most at risk before displacement occurs. Without predictive insights, HR teams cannot efficiently prioritize intervention resources, and workers cannot take proactive steps to protect their careers. This project addresses that gap by building a data engineering pipeline that surfaces displacement risk early and enables targeted operational and individual-level interventions.

---

## Methodology

Built an automated ETL pipeline using Python to extract and transform 2,000 survey records into a structured SQLite database, engineering 12 new features including the **Vulnerability Index**, salary delta metrics, ordinal risk scores, binary outcome flags, and age group buckets that expanding the raw dataset from 17 to 29 columns.

Designed and executed 7 SQL analytical queries to evaluate displacement performance by industry, age group, education level, AI adoption level, and salary bracket, surfacing key workforce KPIs including replacement rate, average vulnerability, and upskilling effectiveness.

Visualized key insights through a **5-sheet Tableau-style interactive JavaScript dashboard** tracking displacement rates, vulnerability tiers, demographic breakdowns, AI adoption impact, and scatter-based exploration — enabling stakeholder filtering by industry, risk level, job status, and age group in real time.

---

## Skills

- **SQL:** Data cleaning, aggregation, `CASE` statements, window functions, `HAVING`, parameterised queries, KPI development
- **Python:** `pandas`, `sqlite3`, ETL pipeline design, feature engineering, integrity assertions, self-healing patch system
- **Feature Engineering:** Ordinal encoding, salary deltas, age binning, binary flags, composite Vulnerability Index design
- **Data Modeling:** Star schema design — fact table + dimension tables, pre-aggregation strategy
- **Tableau:** Dashboard, `Set`-based filter state, chart instance registry, 5-sheet renderer architecture
- **Excel:** `openpyxl`, 4-sheet formatted workbook, conditional formatting, heat bars, color-coded headers

---

## Analysis Architecture

```
Online Survey Data (n = 2,000)
        │
        ▼
Python ETL Pipeline  →  SQLite Database  →  7 SQL Queries
                                                  │
                              ┌───────────────────┼──────────────────┐
                              ▼                   ▼                  ▼
                    Tableau Dashboard     Excel Workbook    CSV Exports
                    (Live interactive)           
```

---

## Data Sources

| Source | Data | Volume |
|---|---|---|
| Online Survey | Self-reported workforce and AI adoption data | 2,000 respondents |
| Survey Fields | Job role, industry, education, AI adoption, automation risk, salary, job status | 17 raw columns |
| Engineered Features | Vulnerability Index, salary deltas, age groups, risk scores, binary flags | 12 new columns |

---

## Tools & Technologies

| Layer | Tool | Purpose |
|---|---|---|
| Data Engineering | Python, pandas, sqlite3 | ETL pipeline, feature engineering, SQLite loading |
| Analytics | SQL (7 queries) | Aggregations, window functions, KPI development |
| Dashboard | Tableau | 5-sheet interactive dashboard, scatter explorer |
| Spreadsheet | Python, openpyxl | 4-sheet formatted Excel workbook |
| SQL Editor | VS Code + SQLite extension | 20 analytical queries, sortable results |
| Version Control | Git / GitHub | Commit-per-stage workflow, documentation |

---

## Results and Business Recommendations

**Results:** Displacement outcomes vary dramatically across worker groups. Workers in high-risk roles with low AI adoption and no upskilling plan score near **10 out of 10** on the Vulnerability Index, while workers in low-risk roles with active AI adoption score as low as **2** — a gap of 8 points driven almost entirely by automation risk and AI adoption behavior. Displacement rate analysis identified the **41–50 age cohort** as the most vulnerable group at a 6.0% displacement rate — 62% higher than workers aged 22–30. The **Master's Degree Paradox** emerged as a counterintuitive finding: holders of master's degrees face the highest displacement rate at 6.5%, exceeding high school graduates at 4.4%, confirming that AI is disrupting knowledge work faster than manual labor. **Marketing** is the most exposed industry at 7.0%, while **Manufacturing** remains the most protected at 3.7%. High AI adopters earn a **4× salary advantage** over low adopters (+10.4% vs +2.5%), confirming that proactive AI adoption is the single strongest individual-level protective factor available to workers.

**Business Recommendations:** Shelters — and by analogy HR teams — should flag high-risk workers at the point of intake (hiring or role review) for immediate upskilling assessment and targeted outreach rather than waiting for displacement to occur. Workers with Vulnerability Index scores of 8.0 or above should receive proactive intervention through AI literacy programs, role redesign pathways, and internal mobility support to reduce displacement risk and preserve organisational capacity. Future iterations should incorporate additional features such as industry-level automation penetration rates, regional labour market conditions, and individual tenure to improve predictive accuracy and enable real-time risk scoring at the point of workforce planning.

---

## Impact

- Created a data-driven system to identify high-risk worker groups with elevated displacement probability, enabling focused upskilling, role redesign, and AI adoption interventions before displacement occurs.
- Improved operational visibility by quantifying displacement inequalities and vulnerability bottlenecks across 7 industries, supporting more effective resource allocation and workforce capacity planning.
- Delivered an executive-level interactive dashboard converting Vulnerability Index scores into actionable KPIs — filtering by industry, risk level, age group, and job status — supporting strategic decision-making and long-term workforce resilience monitoring.
