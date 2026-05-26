from typing import Dict, List, Tuple, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, FloatType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], str, List[Tuple[str, str]], bool]]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = [(k, (expected_schema[k], actual_schema[k])) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any("float" in col for col in new_columns.values()):
            drift_severity = 'HIGH'
        elif any("int" in col for col in new_columns.values()):
            drift_severity = 'LOW'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], List[Tuple[str, str]], str, List[Tuple[str, str]]]]) -> Dict[str, Dict[str, Union[str, str, str]]]:
    decisions = {}
    for col_name, col_type in drift_report["new_columns"].items():
        if col_type == "string":
            decisions[col_name] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable column", "risk_level": "LOW"}
        elif col_type in ["float", "double"]:
            decisions[col_name] = {"action": "FLAG_ANOMALY", "reason": "New numeric column", "risk_level": "HIGH"}
    for col_name, _ in drift_report["removed_columns"]:
        decisions[col_name] = {"action": "HALT", "reason": "Removed column", "risk_level": "BREAKING"}
    for col_name, (old_type, new_type) in drift_report["type_changes"]:
        if new_type == "float" and old_type == "int":
            decisions[col_name] = {"action": "ADD_TO_SCHEMA", "reason": "Type widening", "risk_level": "LOW"}
        elif new_type == "int" and old_type == "float":
            decisions[col_name] = {"action": "FLAG_ANOMALY", "reason": "Type narrowing", "risk_level": "HIGH"}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, str, str]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for col_name, action_info in decisions.items():
        action = action_info["action"]
        if action == "DROP_SILENTLY":
            spark_df = spark_df.drop(col_name)
        elif action == "ADD_TO_SCHEMA":
            migration_notes.append(f"Added new column: {col_name} with type {updated_schema[col_name]}")
        elif action == "FLAG_ANOMALY":
            from pyspark.sql.functions import col
            spark_df = spark_df.withColumn(f"{col_name}_anomaly", col(col_name).isNull())
            migration_notes.append(f"Flagged anomaly in column: {col_name}")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict[str, Union[Dict[str, Union[str, str, str]], List[str]]], Dict[str, Union[Dict[str, str], List[Tuple[str, str]], str, List[Tuple[str, str]]]]]]]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    print(f"Drift Report: {drift_report}")
    print(f"Action Decisions: {decisions}")
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {"evolution_report": {"drift_report": drift_report, "decisions": decisions, "migration_notes": migration_notes}, "evolved_df": evolved_df}
    return {"evolution_report": {"drift_report": drift_report, "decisions": decisions}}
