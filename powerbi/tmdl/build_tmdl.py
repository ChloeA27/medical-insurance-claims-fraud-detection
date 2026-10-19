"""
Generates FraudModel.tmdl, a TMDL "createOrReplace" script for the Power BI
semantic model described in ../SEMANTIC_MODEL.md and ../RLS_DESIGN.md.

Paste the generated script into Power BI Desktop's TMDL view and click Apply
to create every table (with its Power Query M import), relationship, DAX
measure, and RLS role in one step. See ../WINDOWS_BUILD_GUIDE.md.

The model is defined once here as Python data so the column list, M type
conversions, and TMDL column metadata cannot drift apart.

Usage:
    python powerbi/tmdl/build_tmdl.py
"""

from __future__ import annotations

from pathlib import Path

OUT_FILE = Path(__file__).resolve().parent / "FraudModel.tmdl"
DEFAULT_FOLDER = r"C:\fraud-extracts"

# TMDL dataType -> Power Query type
M_TYPES = {
    "int64": "Int64.Type",
    "double": "type number",
    "string": "type text",
    "boolean": "type logical",
    "dateTime": "type date",
}


def col(name, dtype, *, hidden=False, fmt=None, summarize="none", sort_by=None, key=False):
    return dict(name=name, dtype=dtype, hidden=hidden, fmt=fmt,
                summarize=summarize, sort_by=sort_by, key=key)


TABLES = {
    "dim_date": dict(
        data_category="Time",
        columns=[
            col("date_key", "int64", hidden=True),
            col("full_date", "dateTime", fmt="yyyy-mm-dd", key=True),
            col("year", "int64"),
            col("quarter", "int64"),
            col("month", "int64", hidden=True),
            col("month_name", "string", sort_by="month"),
            col("day_of_month", "int64"),
            col("day_of_week", "int64", hidden=True),
            col("day_name", "string", sort_by="day_of_week"),
            col("is_weekend", "boolean"),
            col("year_month", "string"),
        ],
    ),
    "dim_patient": dict(
        columns=[
            col("patient_sk", "int64", hidden=True),
            col("patient_id", "string"),
            col("gender", "string"),
            col("race", "string"),
            col("state_code", "int64"),
            col("county_code", "int64"),
            col("has_renal_disease", "boolean"),
            col("has_alzheimer", "boolean"),
            col("has_heart_failure", "boolean"),
            col("has_kidney_disease", "boolean"),
            col("has_cancer", "boolean"),
            col("has_copd", "boolean"),
            col("has_depression", "boolean"),
            col("has_diabetes", "boolean"),
            col("has_ischemic_heart", "boolean"),
            col("has_osteoporosis", "boolean"),
            col("has_arthritis", "boolean"),
            col("has_stroke", "boolean"),
            col("chronic_condition_count", "int64"),
            col("is_deceased", "boolean"),
            col("risk_category", "string"),
        ],
    ),
    "dim_provider": dict(
        columns=[
            col("provider_sk", "int64", hidden=True),
            col("provider_id", "string"),
            col("is_fraudulent", "boolean"),
            col("provider_type", "string"),
            col("risk_level", "string"),
            col("activity_level", "string"),
            col("avg_length_of_stay", "double", fmt="0.0"),
        ],
    ),
    "dim_diagnosis": dict(
        columns=[
            col("diagnosis_code", "string"),
            col("icd9_chapter", "string"),
            col("subcategory", "string"),
            col("is_chronic", "boolean"),
            col("risk_level", "string"),
            col("cost_category", "string"),
        ],
    ),
    "dim_procedure": dict(
        columns=[
            col("procedure_code", "string"),
            col("code_type", "string"),
            col("procedure_category", "string"),
            col("is_major_procedure", "boolean"),
            col("risk_level", "string"),
            col("cost_category", "string"),
        ],
    ),
    "fact_claims": dict(
        columns=[
            col("claim_sk", "int64", hidden=True),
            col("patient_sk", "int64", hidden=True),
            col("provider_sk", "int64", hidden=True),
            col("claim_start_date_key", "int64", hidden=True),
            col("claim_end_date_key", "int64", hidden=True),
            col("admission_date_key", "int64", hidden=True),
            col("discharge_date_key", "int64", hidden=True),
            col("claim_id", "string"),
            col("claim_type", "string"),
            col("claim_amount", "int64", fmt="$#,##0", summarize="sum"),
            col("deductible_amount", "double", fmt="$#,##0", summarize="sum"),
            col("length_of_stay", "int64", summarize="average"),
            col("is_fraudulent", "boolean"),
            col("diagnosis_code_1", "string", hidden=True),
            col("procedure_code_1", "string", hidden=True),
        ],
        measures="FACT_CLAIMS_MEASURES",
    ),
    "fact_patient_claims_summary": dict(
        columns=[
            col("patient_sk", "int64", hidden=True),
            col("month_key", "int64"),
            col("total_claims", "int64", summarize="sum"),
            col("inpatient_visits", "int64", summarize="sum"),
            col("outpatient_visits", "int64", summarize="sum"),
            col("total_claimed", "int64", fmt="$#,##0", summarize="sum"),
            col("total_deductible", "double", fmt="$#,##0", summarize="sum"),
            col("fraudulent_provider_visits", "int64", summarize="sum"),
            col("fraud_exposure_amount", "int64", fmt="$#,##0", summarize="sum"),
        ],
    ),
}

# (name, display folder, format string, DAX)
FACT_CLAIMS_MEASURES = [
    ("Total Claims", "Core", "#,##0",
     "COUNTROWS ( fact_claims )"),
    ("Total Claim Amount $", "Core", "$#,##0",
     "SUM ( fact_claims[claim_amount] )"),
    ("Avg Claim Amount", "Core", "$#,##0",
     "AVERAGE ( fact_claims[claim_amount] )"),
    ("Inpatient Share %", "Core", "0.0%",
     """DIVIDE (
    CALCULATE ( [Total Claims], fact_claims[claim_type] = "Inpatient" ),
    [Total Claims]
)"""),
    ("Fraud Claim Count", "Fraud Exposure", "#,##0",
     "CALCULATE ( [Total Claims], fact_claims[is_fraudulent] = TRUE () )"),
    ("Fraud Rate %", "Fraud Exposure", "0.0%",
     "DIVIDE ( [Fraud Claim Count], [Total Claims] )"),
    ("Total Fraud Exposure $", "Fraud Exposure", "$#,##0",
     "CALCULATE ( [Total Claim Amount $], fact_claims[is_fraudulent] = TRUE () )"),
    ("Fraud Exposure % of Total $", "Fraud Exposure", "0.0%",
     "DIVIDE ( [Total Fraud Exposure $], [Total Claim Amount $] )"),
    ("Fraud Rate Lift (x)", "Fraud Exposure", "0.00",
     """VAR Baseline =
    CALCULATE ( [Fraud Rate %], REMOVEFILTERS ( dim_diagnosis ), REMOVEFILTERS ( dim_procedure ) )
RETURN
    DIVIDE ( [Fraud Rate %], Baseline )"""),
    ("Distinct Fraudulent Providers", "Provider Risk", "#,##0",
     """CALCULATE (
    DISTINCTCOUNT ( fact_claims[provider_sk] ),
    fact_claims[is_fraudulent] = TRUE ()
)"""),
    ("Fraud Exposure per Fraudulent Provider $", "Provider Risk", "$#,##0",
     "DIVIDE ( [Total Fraud Exposure $], [Distinct Fraudulent Providers] )"),
    ("Top 50 Provider Fraud Share %", "Provider Risk", "0.0%",
     """VAR ProviderFraud =
    ADDCOLUMNS (
        CALCULATETABLE ( VALUES ( fact_claims[provider_sk] ), fact_claims[is_fraudulent] = TRUE () ),
        "@FraudAmt", [Total Fraud Exposure $]
    )
VAR Top50Total =
    SUMX ( TOPN ( 50, ProviderFraud, [@FraudAmt], DESC ), [@FraudAmt] )
RETURN
    DIVIDE ( Top50Total, SUMX ( ProviderFraud, [@FraudAmt] ) )"""),
    ("Claim Amount Prior Month $", "Trend", "$#,##0",
     "CALCULATE ( [Total Claim Amount $], DATEADD ( dim_date[full_date], -1, MONTH ) )"),
    ("Claim Amount MoM %", "Trend", "0.0%",
     """DIVIDE (
    [Total Claim Amount $] - [Claim Amount Prior Month $],
    [Claim Amount Prior Month $]
)"""),
    ("Claim Count QoQ %", "Trend", "0.0%",
     """VAR PriorQ =
    CALCULATE ( [Total Claims], DATEADD ( dim_date[full_date], -1, QUARTER ) )
RETURN
    DIVIDE ( [Total Claims] - PriorQ, PriorQ )"""),
]

# (from table.column, to table.column, active)
RELATIONSHIPS = [
    ("fact_claims.patient_sk", "dim_patient.patient_sk", True),
    ("fact_claims.provider_sk", "dim_provider.provider_sk", True),
    ("fact_claims.claim_start_date_key", "dim_date.date_key", True),
    ("fact_claims.claim_end_date_key", "dim_date.date_key", False),
    ("fact_claims.admission_date_key", "dim_date.date_key", False),
    ("fact_claims.discharge_date_key", "dim_date.date_key", False),
    ("fact_claims.diagnosis_code_1", "dim_diagnosis.diagnosis_code", True),
    ("fact_claims.procedure_code_1", "dim_procedure.procedure_code", True),
    ("fact_patient_claims_summary.patient_sk", "dim_patient.patient_sk", True),
]

# role name -> filter on dim_patient (None = full access)
ROLES = {
    "Analyst_State05": "[state_code] = 5",
    "Analyst_State10": "[state_code] = 10",
    "Analyst_MultiState": "[state_code] IN { 5, 10, 45 }",
    "National_Fraud_Team": None,
}


def q(name: str) -> str:
    """Quote a TMDL object name when it contains anything but word characters."""
    return name if name.replace("_", "").isalnum() else "'" + name.replace("'", "''") + "'"


def indent(text: str, tabs: int) -> list[str]:
    return ["\t" * tabs + line for line in text.splitlines()]


def one_line(expr: str) -> str:
    """Collapse a DAX or M expression onto one line. Power BI Desktop's TMDL
    view failed to find where indented multi-line expressions ended (the
    following formatString line was parsed as part of the DAX), and both
    languages ignore whitespace, so every expression is emitted single-line."""
    return " ".join(part.strip() for part in expr.splitlines() if part.strip())


def m_query(table: str, columns: list[dict]) -> str:
    names = ", ".join(f'"{c["name"]}"' for c in columns)
    types = ", ".join(f'{{"{c["name"]}", {M_TYPES[c["dtype"]]}}}' for c in columns)
    return "\n".join([
        "let",
        f'    Source = Csv.Document(File.Contents(ExtractsFolder & "\\{table}.csv"), [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),',
        "    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),",
        f"    Selected = Table.SelectColumns(Promoted, {{{names}}}),",
        f'    Typed = Table.TransformColumnTypes(Selected, {{{types}}}, "en-US")',
        "in",
        "    Typed",
    ])


def build() -> str:
    lines = ["createOrReplace", ""]
    lines += [
        f'\texpression ExtractsFolder = "{DEFAULT_FOLDER}" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]',
        "",
    ]

    for table, spec in TABLES.items():
        lines.append(f"\ttable {table}")
        if spec.get("data_category"):
            lines.append(f"\t\tdataCategory: {spec['data_category']}")
        lines.append("")

        if spec.get("measures"):
            for name, folder, fmt, dax in globals()[spec["measures"]]:
                lines.append(f"\t\tmeasure {q(name)} = {one_line(dax)}")
                lines.append(f"\t\t\tformatString: {fmt}")
                lines.append(f"\t\t\tdisplayFolder: {folder}")
                lines.append("")

        for c in spec["columns"]:
            lines.append(f"\t\tcolumn {q(c['name'])}")
            lines.append(f"\t\t\tdataType: {c['dtype']}")
            if c["key"]:
                lines.append("\t\t\tisKey")
            if c["hidden"]:
                lines.append("\t\t\tisHidden")
            if c["fmt"]:
                lines.append(f"\t\t\tformatString: {c['fmt']}")
            lines.append(f"\t\t\tsummarizeBy: {c['summarize']}")
            lines.append(f"\t\t\tsourceColumn: {c['name']}")
            if c["sort_by"]:
                lines.append(f"\t\t\tsortByColumn: {c['sort_by']}")
            lines.append("")

        lines.append(f"\t\tpartition {table} = m")
        lines.append("\t\t\tmode: import")
        lines.append(f"\t\t\tsource = {one_line(m_query(table, spec['columns']))}")
        lines.append("")

    for i, (src, dst, active) in enumerate(RELATIONSHIPS, start=1):
        lines.append(f"\trelationship rel_{i:02d}_{src.split('.')[0]}_{src.split('.')[1]}")
        if not active:
            lines.append("\t\tisActive: false")
        lines.append(f"\t\tfromColumn: {src}")
        lines.append(f"\t\ttoColumn: {dst}")
        lines.append("")

    for role, flt in ROLES.items():
        lines.append(f"\trole {role}")
        lines.append("\t\tmodelPermission: read")
        if flt:
            lines.append("")
            lines.append(f"\t\ttablePermission dim_patient = {flt}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    OUT_FILE.write_text(build(), encoding="utf-8")
    print(f"wrote {OUT_FILE}")
