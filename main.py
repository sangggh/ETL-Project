import pandas as pd

def extract(filepath):
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    print(df.info())
    print(df.head())
    return df

df = extract("data/employees.csv")

def transform(df):
    df.columns = df.columns.str.lower().str.strip()

    df = df.dropna(subset=["name"])
    df = df.copy()
    df["email"] = df["email"].fillna("unknown@example.com")

    df = df[df["salary"].notna() & (df["salary"] >= 0)]

    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")

    df["seniority"] = pd.cut(df["join_date"].dt.year, bins=[0, 2020, 2022, 9999], labels=["senior", "mid", "junior"])

    print(f"After cleaning: {len(df)} rows remain")
    return df

df = transform(df)
