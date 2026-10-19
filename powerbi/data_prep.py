"""
Power BI data prep — produces star-schema CSV extracts from the raw Kaggle
source files, ready for import into Power BI Desktop.

This is a local, pandas-based re-implementation of the transformation logic
already defined in ../sql/01-views, ../sql/02-dims, and ../sql/03-facts
(which run against Amazon Athena / Presto SQL on Parquet data in S3). The
production pipeline lives in AWS; this script exists so the same star
schema can be reproduced from the sample CSVs on a laptop, with no AWS
account required, purely for building the Power BI semantic model.

Inputs  (../data/sample-data/):
    Train_Beneficiarydata.csv
    Train_Inpatientdata.csv
    Train_Outpatientdata.csv
    Train_Provider.csv

Outputs (./extracts/), one CSV per star-schema table:
    dim_patient.csv, dim_provider.csv, dim_date.csv,
    dim_diagnosis.csv, dim_procedure.csv,
    fact_claims.csv, fact_provider_summary.csv, fact_patient_claims_summary.csv

Usage:
    python powerbi/data_prep.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "sample-data"
OUT_DIR = Path(__file__).resolve().parent / "extracts"


# ──────────────────────────────────────────────────────────────────────────
#  Views (sql/01-views/*.sql equivalents)
# ──────────────────────────────────────────────────────────────────────────

def load_inpatient() -> pd.DataFrame:
    """Mirrors sql/01-views/v_inpatient_claims_etl.sql"""
    df = pd.read_csv(DATA_DIR / "Train_Inpatientdata.csv", dtype=str, keep_default_na=False, na_values=["NA", ""])

    out = pd.DataFrame({
        "patient_id": df["BeneID"],
        "claim_id": df["ClaimID"],
        "provider_id": df["Provider"],
        "claim_start_date": pd.to_datetime(df["ClaimStartDt"], errors="coerce"),
        "claim_end_date": pd.to_datetime(df["ClaimEndDt"], errors="coerce"),
        "admission_date": pd.to_datetime(df["AdmissionDt"], errors="coerce"),
        "discharge_date": pd.to_datetime(df["DischargeDt"], errors="coerce"),
        "claim_amount": pd.to_numeric(df["InscClaimAmtReimbursed"], errors="coerce"),
        "deductible_amount": pd.to_numeric(df["DeductibleAmtPaid"], errors="coerce"),
        "attending_physician_id": df["AttendingPhysician"],
        "operating_physician_id": df["OperatingPhysician"],
        "other_physician_id": df["OtherPhysician"],
        "admit_diagnosis_code": df["ClmAdmitDiagnosisCode"],
        "diagnosis_group_code": df["DiagnosisGroupCode"],
        "procedure_code_1": df["ClmProcedureCode_1"],
        "claim_type": "Inpatient",
    })
    out["length_of_stay"] = (out["discharge_date"] - out["admission_date"]).dt.days

    for i in range(1, 11):
        out[f"diagnosis_code_{i}"] = df[f"ClmDiagnosisCode_{i}"]
    for i in range(2, 7):
        out[f"procedure_code_{i}"] = df[f"ClmProcedureCode_{i}"]

    out["diagnosis_code_1"] = df["ClmDiagnosisCode_1"]
    return out[out["claim_id"].notna()]


def load_outpatient() -> pd.DataFrame:
    """Mirrors sql/01-views/v_outpatient_claims_etl.sql"""
    df = pd.read_csv(DATA_DIR / "Train_Outpatientdata.csv", dtype=str, keep_default_na=False, na_values=["NA", ""])

    out = pd.DataFrame({
        "patient_id": df["BeneID"],
        "claim_id": df["ClaimID"],
        "provider_id": df["Provider"],
        "claim_start_date": pd.to_datetime(df["ClaimStartDt"], errors="coerce"),
        "claim_end_date": pd.to_datetime(df["ClaimEndDt"], errors="coerce"),
        "claim_amount": pd.to_numeric(df["InscClaimAmtReimbursed"], errors="coerce"),
        "deductible_amount": pd.to_numeric(df["DeductibleAmtPaid"], errors="coerce"),
        "attending_physician_id": df["AttendingPhysician"],
        "operating_physician_id": df["OperatingPhysician"],
        "other_physician_id": df["OtherPhysician"],
        "admit_diagnosis_code": df["ClmAdmitDiagnosisCode"],
        "diagnosis_group_code": np.nan,
        "procedure_code_1": df["ClmProcedureCode_1"],
        "claim_type": "Outpatient",
    })
    out["admission_date"] = pd.NaT
    out["discharge_date"] = pd.NaT
    out["length_of_stay"] = 0

    for i in range(1, 11):
        out[f"diagnosis_code_{i}"] = df.get(f"ClmDiagnosisCode_{i}")
    for i in range(2, 7):
        out[f"procedure_code_{i}"] = df.get(f"ClmProcedureCode_{i}") if i <= 3 else np.nan

    return out[out["claim_id"].notna()]


def load_all_claims() -> pd.DataFrame:
    """
    Mirrors sql/01-views/v_all_claims_etl.sql (UNION ALL of inpatient + outpatient)
    for the core claim grain, but — unlike the SQL view — also carries along
    diagnosis_code_2..10 and procedure_code_2..6 so dim_diagnosis/dim_procedure
    can be built from a single frame (the SQL versions of those dims read
    directly from v_inpatient_claims_etl / v_outpatient_claims_etl instead).
    """
    inpat = load_inpatient()
    outpat = load_outpatient()
    cols = [
        "patient_id", "claim_id", "provider_id", "claim_start_date", "claim_end_date",
        "admission_date", "discharge_date", "length_of_stay", "claim_amount",
        "deductible_amount", "attending_physician_id", "operating_physician_id",
        "other_physician_id", "admit_diagnosis_code", "diagnosis_group_code",
        "diagnosis_code_1", "procedure_code_1", "claim_type",
    ]
    extra_diag = [f"diagnosis_code_{i}" for i in range(2, 11)]
    extra_proc = [f"procedure_code_{i}" for i in range(2, 7)]
    all_cols = cols + extra_diag + extra_proc

    for df in (inpat, outpat):
        for c in extra_diag + extra_proc:
            if c not in df.columns:
                df[c] = np.nan

    return pd.concat([inpat[all_cols], outpat[all_cols]], ignore_index=True)


def load_patients() -> pd.DataFrame:
    """Mirrors sql/01-views/v_patients_etl.sql"""
    df = pd.read_csv(DATA_DIR / "Train_Beneficiarydata.csv", dtype=str, keep_default_na=False, na_values=["NA", ""])
    df = df[df["BeneID"].notna()]

    chronic_cols = {
        "has_alzheimer": "ChronicCond_Alzheimer",
        "has_heart_failure": "ChronicCond_Heartfailure",
        "has_kidney_disease": "ChronicCond_KidneyDisease",
        "has_cancer": "ChronicCond_Cancer",
        "has_copd": "ChronicCond_ObstrPulmonary",
        "has_depression": "ChronicCond_Depression",
        "has_diabetes": "ChronicCond_Diabetes",
        "has_ischemic_heart": "ChronicCond_IschemicHeart",
        "has_osteoporosis": "ChronicCond_Osteoporasis",
        "has_arthritis": "ChronicCond_rheumatoidarthritis",
        "has_stroke": "ChronicCond_stroke",
    }

    out = pd.DataFrame({
        "patient_id": df["BeneID"],
        "date_of_birth": pd.to_datetime(df["DOB"], errors="coerce"),
        "date_of_death": pd.to_datetime(df["DOD"], errors="coerce"),
        "gender": df["Gender"].map({"1": "Male", "2": "Female"}),
        "race": df["Race"].map({"1": "White", "2": "Black", "3": "Asian", "4": "Hispanic", "5": "Other"}),
        "has_renal_disease": df["RenalDiseaseIndicator"].isin(["Y", "1"]),
        "state_code": pd.to_numeric(df["State"], errors="coerce").astype("Int64"),
        "county_code": pd.to_numeric(df["County"], errors="coerce").astype("Int64"),
    })

    for col, src in chronic_cols.items():
        out[col] = df[src] == "1"

    out["chronic_condition_count"] = out[list(chronic_cols.keys())].sum(axis=1)
    out["is_deceased"] = df["DOD"].notna()

    return out


def load_providers(all_claims: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/01-views/v_providers_etl.sql"""
    df = pd.read_csv(DATA_DIR / "Train_Provider.csv", dtype=str)
    df = df[df["Provider"].notna()].drop_duplicates(subset="Provider")

    out = pd.DataFrame({
        "provider_id": df["Provider"],
        "is_fraudulent": df["PotentialFraud"].str.contains("es", case=False, na=False)
        | df["PotentialFraud"].isin(["Y", "1"]),
        "raw_fraud_value": df["PotentialFraud"],
    })
    return out


# ──────────────────────────────────────────────────────────────────────────
#  Dimensions (sql/02-dims/*.sql equivalents)
# ──────────────────────────────────────────────────────────────────────────

def build_dim_date(all_claims: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/02-dims/dim_date_etl.sql, with one deliberate difference:
    the SQL version only keeps dates that appear in claims (398 rows, with
    gaps), while Power BI's "Mark as date table" and time-intelligence
    functions need a contiguous calendar. So this builds every day from
    Jan 1 of the first claim year to Dec 31 of the last claim year."""
    seen = pd.concat([
        all_claims["claim_start_date"], all_claims["claim_end_date"],
        all_claims["admission_date"], all_claims["discharge_date"],
    ]).dropna()
    dates = pd.date_range(f"{seen.min().year}-01-01", f"{seen.max().year}-12-31", freq="D")

    dim = pd.DataFrame({"full_date": dates})
    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.strftime("%B")
    dim["day_of_month"] = dim["full_date"].dt.day
    # SQL DAY_OF_WEEK: 1=Sunday..7=Saturday; pandas dayofweek: 0=Monday..6=Sunday
    dim["day_of_week"] = ((dim["full_date"].dt.dayofweek + 1) % 7) + 1
    dim["day_name"] = dim["full_date"].dt.strftime("%A")
    dim["is_weekend"] = dim["day_of_week"].isin([6, 7])
    dim["year_month"] = dim["full_date"].dt.strftime("%Y-%m")

    return dim.sort_values("date_key").reset_index(drop=True)[
        ["date_key", "full_date", "year", "quarter", "month", "month_name",
         "day_of_month", "day_of_week", "day_name", "is_weekend", "year_month"]
    ]


def build_dim_patient(patients: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/02-dims/dim_patient_etl.sql"""
    dim = patients.sort_values("patient_id").reset_index(drop=True).copy()
    dim.insert(0, "patient_sk", dim.index + 1)

    def risk_category(n: int) -> str:
        if n >= 5:
            return "Very High"
        if n >= 3:
            return "High"
        if n >= 1:
            return "Medium"
        return "Low"

    dim["risk_category"] = dim["chronic_condition_count"].apply(risk_category)
    return dim


_ICD9_CHAPTERS_NUMERIC = [
    (0, 139, "Infectious and Parasitic Diseases"),
    (140, 239, "Neoplasms"),
    (240, 279, "Endocrine, Nutritional and Metabolic"),
    (280, 289, "Blood and Blood-Forming Organs"),
    (290, 319, "Mental Disorders"),
    (320, 389, "Nervous System and Sense Organs"),
    (390, 459, "Circulatory System"),
    (460, 519, "Respiratory System"),
    (520, 579, "Digestive System"),
    (580, 629, "Genitourinary System"),
    (630, 679, "Pregnancy, Childbirth and Puerperium"),
    (680, 709, "Skin and Subcutaneous Tissue"),
    (710, 739, "Musculoskeletal and Connective Tissue"),
    (740, 759, "Congenital Anomalies"),
    (760, 779, "Perinatal Period Conditions"),
    (780, 799, "Symptoms, Signs and Ill-Defined Conditions"),
    (800, 999, "Injury and Poisoning"),
]


def _icd9_chapter(code: str) -> str:
    """Mirrors the icd9_chapter CASE expression in dim_diagnosis_etl.sql."""
    if not code:
        return "Other/Unknown"
    first = code[0]
    if first == "V":
        return "Supplementary Classification (V Codes)"
    if first == "E":
        return "External Causes of Injury (E Codes)"
    cat3 = code[:3]
    if not cat3.isdigit():
        return "Other/Unknown"
    n = int(cat3)
    for lo, hi, label in _ICD9_CHAPTERS_NUMERIC:
        if lo <= n <= hi:
            return label
    return "Other/Unknown"


def _diagnosis_subcategory(code3: str, code_full: str) -> str | None:
    if code3 == "250":
        return "Diabetes Mellitus"
    if code3 in {"401", "402", "403", "404", "405"}:
        return "Hypertensive Disease"
    if code3 == "428":
        return "Heart Failure"
    if code_full == "4019":
        return "Essential Hypertension"
    if code_full in {"2720", "2724"}:
        return "Hyperlipidemia"
    return None


_CHRONIC_CODES = {"4019", "25000", "2724", "496", "4280", "42731", "2859"}
_CHRONIC_PREFIXES = {"250", "401", "402", "403", "404", "405", "428", "496"}
_COMMON_CODES = {"4019", "2724", "25000", "4280", "2859", "496"}


def _diagnosis_risk_level(code3: str, first: str) -> str:
    if first in {"1", "2"} and "140" <= code3 <= "239":
        return "High"
    if "390" <= code3 <= "459":
        return "High"
    if "580" <= code3 <= "629":
        return "High"
    if first == "V":
        return "Low"
    return "Standard"


def _diagnosis_cost_category(code3: str, first: str) -> str:
    if first in {"1", "2"} and "140" <= code3 <= "239":
        return "Very High"
    if first in {"8", "9"}:
        return "High"
    return "Standard"


def build_dim_diagnosis(all_claims: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/02-dims/dim_diagnosis_etl.sql"""
    diag_cols = [f"diagnosis_code_{i}" for i in range(1, 11)] + ["admit_diagnosis_code"]
    codes = pd.unique(all_claims[diag_cols].values.ravel())
    codes = pd.Series(codes).dropna()
    codes = codes.str.replace('"', "", regex=False).str.replace("'", "", regex=False)
    codes = codes[(codes != "") & (codes != "NA")].drop_duplicates().sort_values()

    dim = pd.DataFrame({"diagnosis_code": codes}).reset_index(drop=True)
    dim.insert(0, "diagnosis_sk", dim.index + 1)
    dim["code_category"] = dim["diagnosis_code"].str[:3]
    dim["icd9_chapter"] = dim["diagnosis_code"].apply(_icd9_chapter)
    dim["subcategory"] = dim.apply(
        lambda r: _diagnosis_subcategory(r["code_category"], r["diagnosis_code"]), axis=1
    )
    dim["is_chronic"] = dim["diagnosis_code"].isin(_CHRONIC_CODES) | dim["code_category"].isin(_CHRONIC_PREFIXES)
    dim["risk_level"] = dim.apply(
        lambda r: _diagnosis_risk_level(r["code_category"], r["diagnosis_code"][:1]), axis=1
    )
    dim["is_common_code"] = dim["diagnosis_code"].isin(_COMMON_CODES)
    dim["cost_category"] = dim.apply(
        lambda r: _diagnosis_cost_category(r["code_category"], r["diagnosis_code"][:1]), axis=1
    )
    return dim


_PROCEDURE_CATEGORIES = [
    (1, 5, "Operations on Nervous System"),
    (6, 7, "Operations on Endocrine System"),
    (8, 16, "Operations on Eye"),
    (17, 20, "Operations on Ear"),
    (21, 29, "Operations on Nose, Mouth, and Pharynx"),
    (30, 34, "Operations on Respiratory System"),
    (35, 39, "Operations on Cardiovascular System"),
    (40, 41, "Operations on Hemic and Lymphatic System"),
    (42, 54, "Operations on Digestive System"),
    (55, 59, "Operations on Urinary System"),
    (60, 64, "Operations on Male Genital Organs"),
    (65, 71, "Operations on Female Genital Organs"),
    (72, 75, "Obstetrical Procedures"),
    (76, 84, "Operations on Musculoskeletal System"),
    (85, 86, "Operations on Integumentary System"),
    (87, 99, "Miscellaneous Diagnostic and Therapeutic Procedures"),
]


def _procedure_category(n: int) -> str:
    for lo, hi, label in _PROCEDURE_CATEGORIES:
        if lo <= n <= hi:
            return label
    return "Other/Unknown"


def _procedure_risk_level(n: int) -> str:
    if 1 <= n <= 5 or 35 <= n <= 39:
        return "High"
    if n >= 87:
        return "Low"
    return "Medium"


def _procedure_cost_category(n: int) -> str:
    if 35 <= n <= 39 or 1 <= n <= 5:
        return "Very High"
    if 76 <= n <= 84:
        return "High"
    if n >= 87:
        return "Low"
    return "Medium"


def build_dim_procedure(all_claims: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/02-dims/dim_procedure_etl.sql"""
    proc_cols = [f"procedure_code_{i}" for i in range(1, 7) if f"procedure_code_{i}" in all_claims.columns]
    codes = pd.unique(all_claims[proc_cols].values.ravel())
    codes = pd.Series(codes).dropna()
    codes = codes.str.replace('"', "", regex=False).str.replace("'", "", regex=False).str.replace(" ", "", regex=False)
    codes = codes[(codes != "") & (codes != "NA")]

    # validated_codes: not V/E-prefixed, len<=4, numeric, 1..9999
    def is_valid(c: str) -> bool:
        if c.startswith("V") or c.startswith("E") or len(c) > 4:
            return False
        try:
            v = int(c)
        except ValueError:
            return False
        return 0 < v <= 9999

    codes = codes[codes.apply(is_valid)].drop_duplicates().sort_values()

    dim = pd.DataFrame({"procedure_code": codes}).reset_index(drop=True)
    dim.insert(0, "procedure_sk", dim.index + 1)
    dim["code_category"] = dim["procedure_code"].str[:2]
    dim["code_type"] = "ICD-9 Procedure"
    nums = dim["code_category"].astype(int)
    dim["procedure_category"] = nums.apply(_procedure_category)
    dim["is_major_procedure"] = nums.apply(lambda n: 35 <= n <= 39 or 1 <= n <= 5 or 60 <= n <= 71)
    dim["risk_level"] = nums.apply(_procedure_risk_level)
    dim["cost_category"] = nums.apply(_procedure_cost_category)
    return dim


def build_dim_provider(providers: pd.DataFrame, all_claims: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/02-dims/dim_provider_etl.sql"""
    stats = all_claims.groupby("provider_id").agg(
        total_patients=("patient_id", "nunique"),
        total_claims=("claim_id", "count"),
        total_amount=("claim_amount", "sum"),
        avg_claim_amount=("claim_amount", "mean"),
        max_claim_amount=("claim_amount", "max"),
        min_claim_amount=("claim_amount", "min"),
        inpatient_claims=("claim_type", lambda s: (s == "Inpatient").sum()),
        outpatient_claims=("claim_type", lambda s: (s == "Outpatient").sum()),
    ).reset_index()
    stats["avg_length_of_stay"] = all_claims[all_claims["claim_type"] == "Inpatient"].groupby(
        "provider_id"
    )["length_of_stay"].mean().reindex(stats["provider_id"]).fillna(0).values

    dim = providers.merge(stats, on="provider_id", how="left").fillna({
        "total_patients": 0, "total_claims": 0, "total_amount": 0, "avg_claim_amount": 0,
        "max_claim_amount": 0, "min_claim_amount": 0, "inpatient_claims": 0,
        "outpatient_claims": 0, "avg_length_of_stay": 0,
    })
    dim = dim.sort_values("provider_id").reset_index(drop=True)
    dim.insert(0, "provider_sk", dim.index + 1)

    def provider_type(r) -> str:
        if r["inpatient_claims"] > 0 and r["outpatient_claims"] == 0:
            return "Hospital Only"
        if r["inpatient_claims"] == 0 and r["outpatient_claims"] > 0:
            return "Clinic Only"
        if r["inpatient_claims"] > r["outpatient_claims"]:
            return "Primarily Hospital"
        if r["outpatient_claims"] > r["inpatient_claims"]:
            return "Primarily Clinic"
        return "Mixed"

    dim["provider_type"] = dim.apply(provider_type, axis=1)

    mean_amt = all_claims["claim_amount"].mean()
    std_amt = all_claims["claim_amount"].std()

    def risk_level(r) -> str:
        if r["is_fraudulent"]:
            return "Confirmed Fraud"
        if r["avg_claim_amount"] > mean_amt + 2 * std_amt:
            return "High Risk"
        if r["avg_claim_amount"] > mean_amt + std_amt:
            return "Medium Risk"
        return "Low Risk"

    dim["risk_level"] = dim.apply(risk_level, axis=1)

    def activity_level(n: float) -> str:
        if n > 1000:
            return "Very High Volume"
        if n > 500:
            return "High Volume"
        if n > 100:
            return "Medium Volume"
        if n > 0:
            return "Low Volume"
        return "Inactive"

    dim["activity_level"] = dim["total_claims"].apply(activity_level)
    return dim


# ──────────────────────────────────────────────────────────────────────────
#  Facts (sql/03-facts/*.sql equivalents)
# ──────────────────────────────────────────────────────────────────────────

def build_fact_claims(all_claims: pd.DataFrame, dim_patient: pd.DataFrame, dim_provider: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/03-facts/fact_claims_etl.sql"""
    fact = all_claims.merge(
        dim_patient[["patient_id", "patient_sk"]], on="patient_id", how="inner"
    ).merge(
        dim_provider[["provider_id", "provider_sk", "is_fraudulent"]], on="provider_id", how="inner"
    ).sort_values("claim_id").reset_index(drop=True)

    fact.insert(0, "claim_sk", fact.index + 1)

    def date_key(s: pd.Series) -> pd.Series:
        return s.dt.strftime("%Y%m%d").fillna("0").astype(float).fillna(0).astype(int)

    fact["claim_start_date_key"] = date_key(fact["claim_start_date"])
    fact["claim_end_date_key"] = date_key(fact["claim_end_date"])
    fact["admission_date_key"] = date_key(fact["admission_date"])
    fact["discharge_date_key"] = date_key(fact["discharge_date"])

    fact["claim_amount"] = fact["claim_amount"].fillna(0)
    fact["deductible_amount"] = fact["deductible_amount"].fillna(0)
    fact["length_of_stay"] = fact["length_of_stay"].fillna(0)
    fact["total_amount"] = fact["claim_amount"] + fact["deductible_amount"]

    return fact[[
        "claim_sk", "patient_sk", "provider_sk",
        "claim_start_date_key", "claim_end_date_key", "admission_date_key", "discharge_date_key",
        "claim_id", "claim_type", "claim_amount", "deductible_amount", "length_of_stay", "total_amount",
        "is_fraudulent", "admit_diagnosis_code", "diagnosis_group_code", "diagnosis_code_1",
        "procedure_code_1", "attending_physician_id", "operating_physician_id", "other_physician_id",
    ]]


def build_fact_provider_summary(all_claims: pd.DataFrame, dim_provider: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/03-facts/fact_provider_summary_etl.sql"""
    df = all_claims.merge(dim_provider[["provider_id", "provider_sk", "is_fraudulent"]], on="provider_id")
    df["month_key"] = df["claim_start_date"].dt.strftime("%Y%m")
    df["fraud_amount"] = np.where(df["is_fraudulent"], df["claim_amount"], 0)

    grp = df.groupby(["provider_sk", "month_key"]).agg(
        total_claims=("claim_id", "count"),
        inpatient_claims=("claim_type", lambda s: (s == "Inpatient").sum()),
        outpatient_claims=("claim_type", lambda s: (s == "Outpatient").sum()),
        unique_patients=("patient_id", "nunique"),
        total_claimed=("claim_amount", "sum"),
        avg_claim_amount=("claim_amount", "mean"),
        provider_is_fraudulent=("is_fraudulent", "first"),
        fraud_exposure_amount=("fraud_amount", "sum"),
    ).reset_index()
    return grp


def build_fact_patient_summary(all_claims: pd.DataFrame, dim_patient: pd.DataFrame, dim_provider: pd.DataFrame) -> pd.DataFrame:
    """Mirrors sql/03-facts/fact_patient_claims_summary.sql"""
    df = all_claims.merge(dim_patient[["patient_id", "patient_sk"]], on="patient_id")
    df = df.merge(dim_provider[["provider_id", "is_fraudulent"]], on="provider_id")
    df["month_key"] = df["claim_start_date"].dt.strftime("%Y%m")
    df["fraud_visit"] = df["is_fraudulent"].astype(int)
    df["fraud_amount"] = np.where(df["is_fraudulent"], df["claim_amount"], 0)

    grp = df.groupby(["patient_sk", "month_key"]).agg(
        total_claims=("claim_id", "count"),
        inpatient_visits=("claim_type", lambda s: (s == "Inpatient").sum()),
        outpatient_visits=("claim_type", lambda s: (s == "Outpatient").sum()),
        total_claimed=("claim_amount", "sum"),
        total_deductible=("deductible_amount", "sum"),
        fraudulent_provider_visits=("fraud_visit", "sum"),
        fraud_exposure_amount=("fraud_amount", "sum"),
    ).reset_index()
    return grp


# ──────────────────────────────────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────────────────────────────────

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading raw sources...")
    all_claims = load_all_claims()
    patients = load_patients()

    print("Building dimensions...")
    dim_patient = build_dim_patient(patients)
    dim_provider = build_dim_provider(load_providers(all_claims), all_claims)
    dim_date = build_dim_date(all_claims)
    dim_diagnosis = build_dim_diagnosis(all_claims)
    dim_procedure = build_dim_procedure(all_claims)

    print("Building facts...")
    fact_claims = build_fact_claims(all_claims, dim_patient, dim_provider)
    fact_provider_summary = build_fact_provider_summary(all_claims, dim_provider)
    fact_patient_summary = build_fact_patient_summary(all_claims, dim_patient, dim_provider)

    tables = {
        "dim_patient": dim_patient,
        "dim_provider": dim_provider,
        "dim_date": dim_date,
        "dim_diagnosis": dim_diagnosis,
        "dim_procedure": dim_procedure,
        "fact_claims": fact_claims,
        "fact_provider_summary": fact_provider_summary,
        "fact_patient_claims_summary": fact_patient_summary,
    }

    for name, df in tables.items():
        out_path = OUT_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(f"  wrote {out_path.relative_to(BASE_DIR)}  ({len(df):,} rows)")

    print("Done. Import the CSVs in powerbi/extracts/ into Power BI Desktop (Get Data > Text/CSV).")


if __name__ == "__main__":
    main()
