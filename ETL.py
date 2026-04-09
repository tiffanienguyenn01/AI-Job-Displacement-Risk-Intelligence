import pandas as pd

# Peek at the structure
df = pd.read_csv('ai_job_impact.csv', nrows=5)
print(df)
print(df.dtypes)

# Full analysis
df = pd.read_csv('ai_job_impact.csv')
print('Total rows:', len(df))

# Understand every categorical column
for col in ['Industry', 'Job_Status', 'AI_Adoption_Level', 'Automation_Risk', 'Education_Level']:
    print(f'{col}: {df[col].unique()}')

# Numeric summary
print(df[['Age', 'Salary_Before_AI', 'Salary_After_AI', 'Productivity_Change_%']].describe())

# Distribution of the target variable
print(df['Job_Status'].value_counts())
print(df['Industry'].value_counts())

# Extract
import logging
def extract(path: str) -> pd.DataFrame:
    log.info(f"EXTRACT -> Reading '{path}'")
    df = pd.read_csv(path)
    log.info(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")

    # Integrity gates — pipeline stops here if data is bad
    assert df["Employee_ID"].nunique() == len(df), "Duplicate Employee IDs!"
    assert df.isnull().sum().sum() == 0,           "Unexpected nulls found!"

    log.info("  ✓ Integrity checks passed")
    return df

# Transform
def transform(df: pd.DataFrame) -> pd.DataFrame:

    # 1. Ordinal encodings (turn text categories into numbers)
    df["Risk_Score"] = df["Automation_Risk"].map({"Low": 1, "Medium": 2, "High": 3})
    df["AI_Score"]   = df["AI_Adoption_Level"].map({"Low": 1, "Medium": 2, "High": 3})
    df["Edu_Score"]  = df["Education_Level"].map(
        {"High School": 1, "Bachelor": 2, "Master": 3, "PhD": 4}
    )

    # 2. Salary delta features
    df["Salary_Change"]     = df["Salary_After_AI"] - df["Salary_Before_AI"]
    df["Salary_Change_Pct"] = (df["Salary_Change"] / df["Salary_Before_AI"] * 100).round(2)
    df["Salary_Bracket"]    = pd.cut(
        df["Salary_Before_AI"],
        bins=[0, 50_000, 75_000, 100_000, 200_000],
        labels=["<50k", "50–75k", "75–100k", "100k+"]
    ).astype(str)

    # 3. Age bucketing
    df["Age_Group"] = pd.cut(
        df["Age"],
        bins=[21, 30, 40, 50, 60],
        labels=["22–30", "31–40", "41–50", "51–60"]
    ).astype(str)

    # 4. Binary outcome flags
    df["Is_Replaced"]  = (df["Job_Status"] == "Replaced").astype(int)
    df["Is_Modified"]  = (df["Job_Status"] == "Modified").astype(int)
    df["Is_Upskilled"] = (df["Upskilling_Required"] == "Yes").astype(int)
    df["Is_Remote"]    = (df["Remote_Work"] == "Yes").astype(int)

    # 5. Composite Vulnerability Index (0–10 scale)
    df["Vulnerability_Index"] = (
        df["Risk_Score"] * 2.0                                          +  # max 6.0
        (4 - df["Edu_Score"]) * 0.5                                     +  # max 1.5
        (3 - df["AI_Score"]) * 0.5                                      +  # max 1.0
        df["Is_Upskilled"].apply(lambda x: 0 if x else 1) * 1.0        +  # max 1.0
        df["Age"].apply(lambda a: 0.5 if a > 45 else 0)                    # max 0.5
    ).round(2)

    # 6. Fix column name that breaks SQL (% sign)
    df = df.rename(columns={"Productivity_Change_%": "Productivity_Change_Pct"})

    return df

# Load
def load(df: pd.DataFrame, db_path: str):
    conn = sqlite3.connect(db_path)

    # Fact table — all 2,000 rows with engineered features
    df.to_sql("employees", conn, if_exists="replace", index=False)

    # Dimension table 1: pre-aggregated by industry
    industry_dim = df.groupby("Industry").agg(
        total_workers      = ("Employee_ID", "count"),
        replaced           = ("Is_Replaced", "sum"),
        avg_risk           = ("Risk_Score", "mean"),
        avg_salary_before  = ("Salary_Before_AI", "mean"),
        avg_salary_after   = ("Salary_After_AI", "mean"),
        avg_salary_change_pct = ("Salary_Change_Pct", "mean"),
        avg_productivity   = ("Productivity_Change_Pct", "mean"),
        avg_vulnerability  = ("Vulnerability_Index", "mean")
    ).round(2).reset_index()
    industry_dim.to_sql("dim_industry", conn, if_exists="replace", index=False)

    # Dimension table 2: pre-aggregated by education
    edu_dim = df.groupby("Education_Level").agg(
        total              = ("Employee_ID", "count"),
        replaced           = ("Is_Replaced", "sum"),
        avg_risk           = ("Risk_Score", "mean"),
        avg_salary_change_pct = ("Salary_Change_Pct", "mean")
    ).round(2).reset_index()
    edu_dim["replacement_rate"] = (edu_dim["replaced"] / edu_dim["total"] * 100).round(1)
    edu_dim.to_sql("dim_education", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()

df.to_csv("ai_jobs_transformed.csv", index=False)