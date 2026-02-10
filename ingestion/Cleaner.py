import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_data(df):
    logger.info("Starting data cleaning step")
    
    original_rows = len(df)

    # --- Remove duplicates ---
    df = df.drop_duplicates()
    logger.debug(f"Removed {original_rows - len(df)} duplicate rows")

    # --- Trim strings ---
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # --- Enforce numeric types ---
    numeric_cols = ["phase", "value", "lowci", "highci"]#idk about the last three
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        logger.debug(f"Converted column '{col}' to numeric")

    # --- Enforce int type ---
    df["time period"] = pd.to_numeric(df["time period"], errors="coerce").astype("Int64")
    logger.debug("Converted 'time period' to nullable Int64")

    # --- Enforce datetime ---
    df["time period start date"] = pd.to_datetime(df["time period start date"], errors="coerce")
    logger.debug(f"Converted 'time period start date' to datetime")
    df["time period end date"] = pd.to_datetime(df["time period end date"], errors="coerce")
    logger.debug(f"Converted 'time period end date' to datetime")

    # --- Normalize text columns ---
    text_cols = [
        "indicator", "group", "state", "subgroup",
        "time period label", "confidence interval", "quartile range"
    ]
    for col in text_cols:
        df[col] = df[col].astype("string")
        logger.debug(f"Converted '{col}' to string")

    logger.info(f"Completed data cleaning.")
    return df
