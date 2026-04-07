"""
DataFlow Automation — Core ETL Pipeline
Author: Shebin S Illikkal
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self, source_config: Dict[str, Any]):
        self.config = source_config
        self.quality_report = {}

    def extract(self, source_name: str) -> pd.DataFrame:
        logger.info(f"Extracting from {source_name}")
        # Supports: CSV, JSON, SQL, API, S3, SFTP
        source = self.config['sources'][source_name]
        if source['type'] == 'csv':
            return pd.read_csv(source['path'])
        elif source['type'] == 'sql':
            from sqlalchemy import create_engine
            engine = create_engine(source['connection_string'])
            return pd.read_sql(source['query'], engine)
        raise ValueError(f"Unsupported source type: {source['type']}")

    def transform(self, df: pd.DataFrame, rules: Dict) -> pd.DataFrame:
        logger.info(f"Transforming {len(df):,} rows")
        # Auto type casting
        for col, dtype in rules.get('dtypes', {}).items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors='ignore')
        # Deduplication
        if rules.get('dedup_keys'):
            before = len(df)
            df = df.drop_duplicates(subset=rules['dedup_keys'], keep='last')
            logger.info(f"  Removed {before - len(df):,} duplicates")
        # Null handling
        for col, strategy in rules.get('fill_nulls', {}).items():
            if strategy == 'mean':    df[col] = df[col].fillna(df[col].mean())
            elif strategy == 'mode':  df[col] = df[col].fillna(df[col].mode()[0])
            elif strategy == 'drop':  df = df.dropna(subset=[col])
        self._check_quality(df, rules)
        return df

    def _check_quality(self, df: pd.DataFrame, rules: Dict):
        issues = []
        for col in rules.get('required_cols', []):
            null_pct = df[col].isnull().mean() * 100
            if null_pct > 5:
                issues.append(f"{col}: {null_pct:.1f}% nulls")
        self.quality_report = {
            'rows': len(df), 'issues': issues,
            'quality_score': max(0, 100 - len(issues) * 10)
        }
        logger.info(f"  Quality score: {self.quality_report['quality_score']}%")

    def load(self, df: pd.DataFrame, destination: str):
        logger.info(f"Loading {len(df):,} rows to {destination}")
        dest = self.config['destinations'][destination]
        if dest['type'] == 'postgresql':
            from sqlalchemy import create_engine
            engine = create_engine(dest['connection_string'])
            df.to_sql(dest['table'], engine, if_exists='append', index=False, chunksize=1000)
        elif dest['type'] == 'csv':
            df.to_csv(dest['path'], index=False)
        logger.info("  Load complete")

    def run(self, source: str, rules: Dict, destination: str) -> Dict:
        start = datetime.now()
        df = self.extract(source)
        df = self.transform(df, rules)
        self.load(df, destination)
        elapsed = (datetime.now() - start).seconds
        logger.info(f"Pipeline complete in {elapsed}s")
        return {**self.quality_report, 'elapsed_seconds': elapsed}
