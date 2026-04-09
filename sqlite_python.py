import pandas as pd
import sqlite3
import os
from datetime import datetime

DB_PATH  = "ai_jobs.db"
CSV_PATH = "ai_job_impact.csv"

# ── Colour helpers ───────────────────────────────────────────
G  = "\033[92m"
Y  = "\033[93m"
R  = "\033[91m"
B  = "\033[94m"
W  = "\033[97m"
BD = "\033[1m"
RS = "\033[0m"

def section(title):
    print(f"\n{BD}{B}{'═'*60}{RS}")
    print(f"{BD}{W}  {title}{RS}")
    print(f"{BD}{B}{'═'*60}{RS}")

def show(df, label=""):
    if label:
        print(f"\n{Y}{BD}{label}{RS}")
    print(df.to_string(index=False))
    print(f"{G}  → {len(df)} rows returned{RS}")


# ════════════════════════════════════════════════════════════
#  STEP 0 — Build DB if missing
# ════════════════════════════════════════════════════════════
if not os.path.exists(DB_PATH):
    print(f"{Y}DB not found — running inline ETL...{RS}")
    df = pd.read_csv(CSV_PATH)

    df["Salary_Change"]       = df["Salary_After_AI"] - df["Salary_Before_AI"]
    df["Salary_Change_Pct"]   = (df["Salary_Change"] / df["Salary_Before_AI"] * 100).round(2)
    df["Risk_Score"]          = df["Automation_Risk"].map({"Low":1,"Medium":2,"High":3})
    df["AI_Score"]            = df["AI_Adoption_Level"].map({"Low":1,"Medium":2,"High":3})
    df["Edu_Score"]           = df["Education_Level"].map(
                                    {"High School":1,"Bachelor":2,"Master":3,"PhD":4})
    df["Age_Group"]           = pd.cut(df["Age"], bins=[21,30,40,50,60],
                                       labels=["22-30","31-40","41-50","51-60"]).astype(str)
    df["Salary_Bracket"]      = pd.cut(df["Salary_Before_AI"],
                                       bins=[0,50000,75000,100000,999999],
                                       labels=["<50k","50-75k","75-100k","100k+"]).astype(str)
    df["Is_Replaced"]         = (df["Job_Status"] == "Replaced").astype(int)
    df["Is_Modified"]         = (df["Job_Status"] == "Modified").astype(int)
    df["Is_Upskilled"]        = (df["Upskilling_Required"] == "Yes").astype(int)
    df["Is_Remote"]           = (df["Remote_Work"] == "Yes").astype(int)
    df["Vulnerability_Index"] = (
        df["Risk_Score"] * 2.0 +
        (4 - df["Edu_Score"]) * 0.5 +
        (3 - df["AI_Score"]) * 0.5 +
        df["Is_Upskilled"].apply(lambda x: 0 if x else 1) +
        df["Age"].apply(lambda a: 0.5 if a > 45 else 0)
    ).round(2)
    df = df.rename(columns={"Productivity_Change_%": "Productivity_Change_Pct"})

    _conn = sqlite3.connect(DB_PATH)
    df.to_sql("employees", _conn, if_exists="replace", index=False)
    _conn.commit()
    _conn.close()
    print(f"{G}✅  DB built from scratch → {DB_PATH}{RS}")


# ════════════════════════════════════════════════════════════
#  CONNECT — always runs whether DB was just built or existed
# ════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row


# ════════════════════════════════════════════════════════════
#  PATCH — add any columns missing from older ETL versions
# ════════════════════════════════════════════════════════════
existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(employees)")]

patches = {
    "Salary_Bracket": [
        "ALTER TABLE employees ADD COLUMN Salary_Bracket TEXT",
        """UPDATE employees SET Salary_Bracket = CASE
               WHEN Salary_Before_AI < 50000  THEN '<50k'
               WHEN Salary_Before_AI < 75000  THEN '50-75k'
               WHEN Salary_Before_AI < 100000 THEN '75-100k'
               ELSE '100k+' END""",
    ],
    "Age_Group": [
        "ALTER TABLE employees ADD COLUMN Age_Group TEXT",
        """UPDATE employees SET Age_Group = CASE
               WHEN Age <= 30 THEN '22-30'
               WHEN Age <= 40 THEN '31-40'
               WHEN Age <= 50 THEN '41-50'
               ELSE '51-60' END""",
    ],
    "Salary_Change": [
        "ALTER TABLE employees ADD COLUMN Salary_Change INTEGER",
        "UPDATE employees SET Salary_Change = Salary_After_AI - Salary_Before_AI",
    ],
    "Salary_Change_Pct": [
        "ALTER TABLE employees ADD COLUMN Salary_Change_Pct REAL",
        """UPDATE employees SET Salary_Change_Pct =
               ROUND((Salary_After_AI - Salary_Before_AI) * 1.0
                     / Salary_Before_AI * 100, 2)""",
    ],
    "Risk_Score": [
        "ALTER TABLE employees ADD COLUMN Risk_Score INTEGER",
        """UPDATE employees SET Risk_Score = CASE Automation_Risk
               WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END""",
    ],
    "AI_Score": [
        "ALTER TABLE employees ADD COLUMN AI_Score INTEGER",
        """UPDATE employees SET AI_Score = CASE AI_Adoption_Level
               WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END""",
    ],
    "Edu_Score": [
        "ALTER TABLE employees ADD COLUMN Edu_Score INTEGER",
        """UPDATE employees SET Edu_Score = CASE Education_Level
               WHEN 'High School' THEN 1 WHEN 'Bachelor' THEN 2
               WHEN 'Master' THEN 3 ELSE 4 END""",
    ],
    "Is_Replaced": [
        "ALTER TABLE employees ADD COLUMN Is_Replaced INTEGER",
        "UPDATE employees SET Is_Replaced = CASE WHEN Job_Status='Replaced' THEN 1 ELSE 0 END",
    ],
    "Is_Modified": [
        "ALTER TABLE employees ADD COLUMN Is_Modified INTEGER",
        "UPDATE employees SET Is_Modified = CASE WHEN Job_Status='Modified' THEN 1 ELSE 0 END",
    ],
    "Is_Upskilled": [
        "ALTER TABLE employees ADD COLUMN Is_Upskilled INTEGER",
        "UPDATE employees SET Is_Upskilled = CASE WHEN Upskilling_Required='Yes' THEN 1 ELSE 0 END",
    ],
    "Is_Remote": [
        "ALTER TABLE employees ADD COLUMN Is_Remote INTEGER",
        "UPDATE employees SET Is_Remote = CASE WHEN Remote_Work='Yes' THEN 1 ELSE 0 END",
    ],
    "Vulnerability_Index": [
        "ALTER TABLE employees ADD COLUMN Vulnerability_Index REAL",
        """UPDATE employees SET Vulnerability_Index = ROUND(
               CASE Automation_Risk WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END * 2.0
               + (4 - CASE Education_Level WHEN 'High School' THEN 1
                       WHEN 'Bachelor' THEN 2 WHEN 'Master' THEN 3 ELSE 4 END) * 0.5
               + (3 - CASE AI_Adoption_Level WHEN 'Low' THEN 1
                       WHEN 'Medium' THEN 2 ELSE 3 END) * 0.5
               + CASE WHEN Upskilling_Required='Yes' THEN 0 ELSE 1 END
               + CASE WHEN Age > 45 THEN 0.5 ELSE 0 END
           , 2)""",
    ],
}

patched_any = False
for col, stmts in patches.items():
    if col not in existing_cols:
        for stmt in stmts:
            conn.execute(stmt)
        conn.commit()
        print(f"{G}✅  Patched missing column: {col}{RS}")
        patched_any = True


# ════════════════════════════════════════════════════════════
#  1. INSPECT THE TRANSFORMED TABLE
# ════════════════════════════════════════════════════════════
section("1. INSPECT THE TRANSFORMED TABLE")

cursor = conn.execute("PRAGMA table_info(employees)")
schema = cursor.fetchall()
print(f"\n{Y}{BD}Schema — employees table ({len(schema)} columns){RS}")
print(f"  {'Col #':<6} {'Column Name':<28} {'Type':<10}")
print(f"  {'-'*6} {'-'*28} {'-'*10}")
for col in schema:
    print(f"  {col['cid']:<6} {col['name']:<28} {col['type']:<10}")

total = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
print(f"\n{G}  Total rows: {total:,}{RS}")

df_preview = pd.read_sql("""
    SELECT Employee_ID, Age, Age_Group, Gender, Industry, Job_Role,
           Job_Status, Risk_Score, AI_Score, Edu_Score,
           Salary_Change_Pct, Vulnerability_Index
    FROM employees LIMIT 5
""", conn)
show(df_preview, "First 5 rows (key columns):")

df_stats = pd.read_sql("""
    SELECT
        ROUND(MIN(Vulnerability_Index),2)  AS vi_min,
        ROUND(MAX(Vulnerability_Index),2)  AS vi_max,
        ROUND(AVG(Vulnerability_Index),2)  AS vi_avg,
        ROUND(MIN(Salary_Change_Pct),2)    AS salary_pct_min,
        ROUND(MAX(Salary_Change_Pct),2)    AS salary_pct_max,
        ROUND(AVG(Salary_Change_Pct),2)    AS salary_pct_avg,
        ROUND(MIN(Risk_Score),0)           AS risk_min,
        ROUND(MAX(Risk_Score),0)           AS risk_max,
        ROUND(AVG(Risk_Score),2)           AS risk_avg
    FROM employees
""", conn)
show(df_stats, "Engineered column statistics:")


# ════════════════════════════════════════════════════════════
#  2. WORKFORCE OVERVIEW
# ════════════════════════════════════════════════════════════
section("2. WORKFORCE OVERVIEW")

df_status = pd.read_sql("""
    SELECT
        Job_Status,
        COUNT(*)                                     AS workers,
        ROUND(100.0 * COUNT(*) / 2000, 1)            AS pct_workforce,
        ROUND(AVG(Salary_Change_Pct), 2)             AS avg_salary_change_pct,
        ROUND(AVG(Vulnerability_Index), 2)           AS avg_vulnerability,
        ROUND(AVG(Job_Satisfaction), 2)              AS avg_satisfaction
    FROM employees
    GROUP BY Job_Status
    ORDER BY workers DESC
""", conn)
show(df_status, "Job status breakdown:")

df_risk = pd.read_sql("""
    SELECT
        Automation_Risk,
        Risk_Score,
        COUNT(*)                                     AS workers,
        ROUND(100.0 * COUNT(*) / 2000, 1)            AS pct_workforce,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(AVG(Vulnerability_Index), 2)           AS avg_vulnerability
    FROM employees
    GROUP BY Automation_Risk, Risk_Score
    ORDER BY Risk_Score DESC
""", conn)
show(df_risk, "Automation risk distribution:")

df_danger = pd.read_sql("""
    SELECT
        COUNT(*)                                     AS danger_zone_workers,
        SUM(Is_Replaced)                             AS already_replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Vulnerability_Index), 2)           AS avg_vulnerability,
        ROUND(AVG(Salary_Change_Pct), 2)             AS avg_salary_change
    FROM employees
    WHERE Automation_Risk = 'High'
      AND Upskilling_Required = 'No'
""", conn)
show(df_danger, "Danger zone (High risk + No upskilling):")


# ════════════════════════════════════════════════════════════
#  3. INDUSTRY ANALYSIS
# ════════════════════════════════════════════════════════════
section("3. INDUSTRY ANALYSIS")

df_industry = pd.read_sql("""
    SELECT
        Industry,
        COUNT(*)                                     AS total_workers,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Risk_Score),2)                     AS avg_risk_score,
        ROUND(AVG(Salary_Before_AI),0)               AS avg_salary_before,
        ROUND(AVG(Salary_After_AI),0)                AS avg_salary_after,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change_pct,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY Industry
    ORDER BY replaced DESC
""", conn)
show(df_industry, "Industry summary (sorted by displacement):")

df_salary_outcome = pd.read_sql("""
    SELECT
        Industry,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change_pct,
        CASE
            WHEN AVG(Salary_Change_Pct) >= 7  THEN 'Strong winner'
            WHEN AVG(Salary_Change_Pct) >= 5  THEN 'Moderate winner'
            ELSE                                   'Below average'
        END                                          AS salary_outcome
    FROM employees
    GROUP BY Industry
    ORDER BY avg_salary_change_pct DESC
""", conn)
show(df_salary_outcome, "Salary outcome by industry (CASE WHEN label):")


# ════════════════════════════════════════════════════════════
#  4. DEMOGRAPHIC VULNERABILITY
# ════════════════════════════════════════════════════════════
section("4. DEMOGRAPHIC VULNERABILITY")

df_age = pd.read_sql("""
    SELECT
        Age_Group,
        COUNT(*)                                     AS total,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change,
        ROUND(AVG(Years_Experience),1)               AS avg_experience
    FROM employees
    GROUP BY Age_Group
    ORDER BY Age_Group
""", conn)
show(df_age, "Age group vulnerability:")

df_edu = pd.read_sql("""
    SELECT
        Education_Level,
        Edu_Score,
        COUNT(*)                                     AS total,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Risk_Score),2)                     AS avg_risk_score,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change
    FROM employees
    GROUP BY Education_Level, Edu_Score
    ORDER BY Edu_Score DESC
""", conn)
show(df_edu, "Education level vs displacement:")

df_gender = pd.read_sql("""
    SELECT
        Gender,
        COUNT(*)                                     AS total,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY Gender
    ORDER BY replacement_rate_pct DESC
""", conn)
show(df_gender, "Gender breakdown:")


# ════════════════════════════════════════════════════════════
#  5. AI ADOPTION & UPSKILLING IMPACT
# ════════════════════════════════════════════════════════════
section("5. AI ADOPTION & UPSKILLING IMPACT")

df_ai = pd.read_sql("""
    SELECT
        AI_Adoption_Level,
        AI_Score,
        COUNT(*)                                     AS workers,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(AVG(Productivity_Change_Pct),2)        AS avg_productivity,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change,
        ROUND(AVG(Job_Satisfaction),2)               AS avg_satisfaction,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY AI_Adoption_Level, AI_Score
    ORDER BY AI_Score DESC
""", conn)
show(df_ai, "AI adoption impact (AI_Score column):")

df_upskill = pd.read_sql("""
    SELECT
        Upskilling_Required,
        Is_Upskilled,
        Job_Status,
        COUNT(*)                                     AS count,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY Upskilling_Required, Is_Upskilled, Job_Status
    ORDER BY Upskilling_Required, Job_Status
""", conn)
show(df_upskill, "Upskilling effectiveness (Is_Upskilled flag):")

df_best = pd.read_sql("""
    SELECT
        AI_Adoption_Level,
        Upskilling_Required,
        COUNT(*)                                     AS workers,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY AI_Adoption_Level, Upskilling_Required
    ORDER BY avg_salary_change DESC
""", conn)
show(df_best, "Best/worst worker profile combinations:")


# ════════════════════════════════════════════════════════════
#  6. VULNERABILITY INDEX DEEP DIVE
# ════════════════════════════════════════════════════════════
section("6. VULNERABILITY INDEX DEEP DIVE")

df_tiers = pd.read_sql("""
    SELECT
        CASE
            WHEN Vulnerability_Index >= 8 THEN 'Critical (8-10)'
            WHEN Vulnerability_Index >= 6 THEN 'High     (6-8)'
            WHEN Vulnerability_Index >= 4 THEN 'Medium   (4-6)'
            ELSE                               'Low      (0-4)'
        END                                          AS vulnerability_tier,
        COUNT(*)                                     AS workers,
        ROUND(100.0*COUNT(*)/2000,1)                 AS pct_workforce,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change
    FROM employees
    GROUP BY vulnerability_tier
    ORDER BY MIN(Vulnerability_Index) DESC
""", conn)
show(df_tiers, "Vulnerability tier segmentation:")

df_top10 = pd.read_sql("""
    SELECT
        Job_Role,
        Industry,
        COUNT(*)                                     AS workers,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct
    FROM employees
    WHERE Automation_Risk = 'High'
    GROUP BY Job_Role, Industry
    HAVING workers >= 3
    ORDER BY avg_vulnerability DESC
    LIMIT 10
""", conn)
show(df_top10, "Top 10 most vulnerable high-risk roles:")


# ════════════════════════════════════════════════════════════
#  7. SALARY BRACKET ANALYSIS
# ════════════════════════════════════════════════════════════
section("7. SALARY BRACKET ANALYSIS")

df_bracket = pd.read_sql("""
    SELECT
        Salary_Bracket,
        COUNT(*)                                     AS workers,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Salary_Change),0)                  AS avg_salary_change_usd,
        ROUND(AVG(Salary_Change_Pct),2)              AS avg_salary_change_pct,
        ROUND(AVG(Vulnerability_Index),2)            AS avg_vulnerability
    FROM employees
    GROUP BY Salary_Bracket
    ORDER BY MIN(Salary_Before_AI)
""", conn)
show(df_bracket, "Does earning more protect you?")

df_hours = pd.read_sql("""
    SELECT
        CASE
            WHEN Work_Hours_Per_Week < 35  THEN 'Part-time  (<35h)'
            WHEN Work_Hours_Per_Week <= 45 THEN 'Standard   (35-45h)'
            ELSE                                'Overworked (>45h)'
        END                                          AS work_schedule,
        COUNT(*)                                     AS workers,
        SUM(Is_Replaced)                             AS replaced,
        ROUND(100.0*SUM(Is_Replaced)/COUNT(*),1)     AS replacement_rate_pct,
        ROUND(AVG(Job_Satisfaction),2)               AS avg_satisfaction,
        ROUND(AVG(Productivity_Change_Pct),2)        AS avg_productivity
    FROM employees
    GROUP BY work_schedule
    ORDER BY replacement_rate_pct DESC
""", conn)
show(df_hours, "Work schedule vs replacement rate:")


# ════════════════════════════════════════════════════════════
#  8. PYTHON + SQLITE COMBINED
# ════════════════════════════════════════════════════════════
section("8. PYTHON + SQLITE COMBINED")

df_high_risk = pd.read_sql("""
    SELECT * FROM employees
    WHERE Risk_Score   = 3
      AND AI_Score     = 1
      AND Is_Upskilled = 0
    ORDER BY Vulnerability_Index DESC
""", conn)

df_high_risk["Risk_Label"] = df_high_risk["Vulnerability_Index"].apply(
    lambda v: "CRITICAL" if v >= 8 else "HIGH"
)
df_high_risk["Salary_Impact"] = df_high_risk["Salary_Change"].apply(
    lambda s: f"+${abs(int(s)):,}" if s >= 0 else f"-${abs(int(s)):,}"
)

print(f"\n{Y}{BD}Most at-risk workers (High risk, Low AI, No upskilling):{RS}")
print(df_high_risk[[
    "Employee_ID", "Age_Group", "Industry", "Job_Role",
    "Vulnerability_Index", "Risk_Label", "Salary_Impact"
]].head(10).to_string(index=False))

df_high_risk.to_sql("high_risk_workers", conn, if_exists="replace", index=False)
count = conn.execute("SELECT COUNT(*) FROM high_risk_workers").fetchone()[0]
print(f"{G}✅  Written 'high_risk_workers' table → {count} rows in DB{RS}")

industry_filter = "Marketing"
min_vuln        = 7.0
df_param = pd.read_sql("""
    SELECT Employee_ID, Job_Role, Vulnerability_Index, Salary_Change_Pct
    FROM employees
    WHERE Industry            = ?
      AND Vulnerability_Index >= ?
    ORDER BY Vulnerability_Index DESC
""", conn, params=(industry_filter, min_vuln))
show(df_param, f"Parameterised query — {industry_filter}, vulnerability >= {min_vuln}:")

df_plot_ready = pd.read_sql("""
    SELECT
        Industry,
        ROUND(AVG(Vulnerability_Index),2) AS avg_vulnerability,
        ROUND(AVG(Salary_Change_Pct),2)   AS avg_salary_change,
        SUM(Is_Replaced)                  AS total_replaced
    FROM employees
    GROUP BY Industry
    ORDER BY avg_vulnerability DESC
""", conn)
print(f"\n{Y}{BD}Plot-ready dataframe (aggregate SQL -> pandas):{RS}")
print(df_plot_ready.to_string(index=False))


# ════════════════════════════════════════════════════════════
#  9. EXPORT QUERY RESULTS TO CSV
# ════════════════════════════════════════════════════════════
section("9. EXPORT QUERY RESULTS TO CSV")

exports = {
    "ai_jobs_transformed_full.csv": "SELECT * FROM employees",
    "ai_jobs_high_risk_segment.csv": "SELECT * FROM high_risk_workers",
    "ai_jobs_vulnerability_tiers.csv": """
        SELECT *,
            CASE
                WHEN Vulnerability_Index >= 8 THEN 'Critical'
                WHEN Vulnerability_Index >= 6 THEN 'High'
                WHEN Vulnerability_Index >= 4 THEN 'Medium'
                ELSE 'Low'
            END AS Vulnerability_Tier
        FROM employees
        ORDER BY Vulnerability_Index DESC
    """,
    "ai_jobs_replaced_workers.csv": """
        SELECT Employee_ID, Age, Age_Group, Gender, Industry, Job_Role,
               Education_Level, Edu_Score, AI_Adoption_Level, AI_Score,
               Automation_Risk, Risk_Score, Upskilling_Required,
               Salary_Before_AI, Salary_After_AI, Salary_Change,
               Salary_Change_Pct, Vulnerability_Index
        FROM employees
        WHERE Is_Replaced = 1
        ORDER BY Vulnerability_Index DESC
    """,
}

for filename, sql in exports.items():
    df_exp = pd.read_sql(sql, conn)
    df_exp.to_csv(filename, index=False)
    print(f"{G}✅  {filename:<45}{RS} {len(df_exp):>5} rows x {len(df_exp.columns)} cols")

tables = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()
conn.close()

print(f"\n{G}{BD}Tables in ai_jobs.db:{RS}")
for t in tables:
    print(f"{G}  • {t[0]}{RS}")
