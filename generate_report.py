import os
import json
import pandas as pd
import numpy as np
import pyodbc
import requests
import openai
import re
from datetime import datetime, timedelta
import yaml
import logging
from typing import Dict, List, Tuple, Any, Optional, Union
import time
from functools import wraps, lru_cache
import tempfile
import shutil
from abc import ABC, abstractmethod
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import csv
from concurrent.futures import ThreadPoolExecutor
import tenacity
from google.cloud import bigquery
from google.oauth2 import service_account
from simple_salesforce import Salesforce
import boto3
from botocore.exceptions import ClientError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.synapse.artifacts import ArtifactsClient
from msal import ConfidentialClientApplication
import networkx as nx
from openai import OpenAI
import jwt
import hashlib
import secrets
import unittest
from unittest.mock import patch, MagicMock
import prometheus_client
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import aiohttp
import redis
import itertools
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pydantic import BaseModel, validator
from contextlib import contextmanager
import gc
import zipfile
import xml.etree.ElementTree as ET
from xml.dom import minidom
import base64
import struct
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from streamlit_chat import message
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Protocol.KDF import PBKDF2
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import ast
import sys
import importlib.util
import inspect
import socket
import urllib.parse
from scipy import stats

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('powerbi_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Performance Monitoring Metrics
REQUEST_COUNT = Counter('powerbi_requests_total', 'Total Power BI requests', ['endpoint', 'status'])
REQUEST_DURATION = Histogram('powerbi_request_duration_seconds', 'Power BI request duration', ['endpoint'])
ACTIVE_CONNECTIONS = Gauge('powerbi_active_connections', 'Active Power BI connections')
ERROR_COUNT = Counter('powerbi_errors_total', 'Total Power BI errors', ['type'])
DATA_QUALITY_SCORE = Gauge('powerbi_data_quality_score', 'Data quality score', ['data_source'])
SCHEMA_DRIFT_DETECTED = Counter('powerbi_schema_drift_detected', 'Schema drift detected', ['data_source'])

# Start Prometheus metrics server
def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics server"""
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start Prometheus metrics server: {str(e)}")

# Configuration Validation with Pydantic
class AIConfig(BaseModel):
    api_key: str
    model: str = "gpt-4"
    temperature: float = 0.3
    max_tokens: int = 2000

class FabricConfig(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    pipeline_id: Optional[str] = None

class DataSourceConfig(BaseModel):
    type: str
    driver: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    file_path: Optional[str] = None
    url: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    paginate: Optional[bool] = False
    project_id: Optional[str] = None
    credentials_path: Optional[str] = None
    security_token: Optional[str] = None
    sandbox: Optional[bool] = False
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region: Optional[str] = "us-east-1"
    bucket_name: Optional[str] = None
    prefix: Optional[str] = ""
    host: Optional[str] = None
    port: Optional[int] = None
    sample_size: Optional[int] = 1000
    dataset_id: Optional[str] = None
    
    @validator('type')
    def validate_source_type(cls, v):
        allowed_types = ['sql_server', 'postgresql', 'csv', 'excel', 'api', 'bigquery', 
                        'salesforce', 's3', 'redshift', 'onelake', 'semantic_model']
        if v not in allowed_types:
            raise ValueError(f'Invalid source_type: {v}')
        return v

class DataQualityConfig(BaseModel):
    enabled: bool = True
    max_null_percentage: float = 30.0
    max_duplicate_percentage: float = 10.0
    check_outliers: bool = True
    outlier_method: str = "zscore"  # zscore, iqr
    outlier_threshold: float = 3.0
    check_data_types: bool = True
    check_value_ranges: bool = True
    custom_rules: List[Dict] = []

class EnterpriseSecurityConfig(BaseModel):
    conditional_access: bool = True
    mfa_required: bool = True
    device_compliance_required: bool = True
    ip_restrictions: List[str] = []

class ComplianceConfig(BaseModel):
    data_classification: str = "Confidential"
    retention_policy: str = "7_years"
    audit_logging: bool = True
    standards: List[str] = ["GDPR", "SOX", "HIPAA"]

class DeploymentConfig(BaseModel):
    environment: str = "production"
    pipeline_stages: List[str] = ["dev", "test", "prod"]
    auto_deployment: bool = False
    approval_required: bool = True

class MonitoringConfig(BaseModel):
    application_insights: str
    log_analytics_workspace: str
    prometheus_endpoint: str = "http://prometheus:9090"

class IntegrationConfig(BaseModel):
    azure_devops_project: str
    teams_webhook: str
    key_vault_url: str

class ScalingConfig(BaseModel):
    auto_scaling: Dict[str, Any] = {
        "enabled": True,
        "min_capacity": "F2",
        "max_capacity": "F64",
        "scale_triggers": {
            "cpu_threshold": 80,
            "memory_threshold": 85,
            "concurrent_users": 1000
        }
    }
    load_balancing: Dict[str, Any] = {
        "enabled": True,
        "strategy": "round_robin",
        "health_check_interval": 30
    }
    caching: Dict[str, Any] = {
        "redis_cluster": {
            "enabled": True,
            "nodes": 3,
            "memory_per_node": "8GB"
        }
    }
    performance_optimization: Dict[str, Any] = {
        "query_timeout": 300,
        "max_concurrent_queries": 50,
        "incremental_refresh": True
    }

class MultiGeoConfig(BaseModel):
    home_region: str = "West Europe"
    multi_geo_capacities: List[Dict[str, str]] = []
    data_residency_compliance: str = "GDPR"

class MainConfig(BaseModel):
    openai: AIConfig
    fabric: FabricConfig
    data_sources: Dict[str, DataSourceConfig]
    data_quality: DataQualityConfig = DataQualityConfig()
    max_workers: int = 4
    data_chunk_size: int = 10000
    enterprise_security: EnterpriseSecurityConfig = EnterpriseSecurityConfig()
    compliance: ComplianceConfig = ComplianceConfig()
    deployment: DeploymentConfig = DeploymentConfig()
    monitoring: MonitoringConfig
    integrations: IntegrationConfig
    scaling: ScalingConfig = ScalingConfig()
    multi_geo_config: MultiGeoConfig = MultiGeoConfig()
    max_sql_generation_try: int = 3
    max_python_script_check: int = 3
    sandbox_path: str = "/tmp/sandbox"

# Load configuration from YAML
def load_config(config_path: str = "config.yaml") -> Dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Validate configuration
        validated_config = MainConfig(**config_data)
        return validated_config.dict()
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Configuration validation error: {str(e)}")
        raise

# Dependencies & Installation while running the code itself
def check_and_install_dependencies():
    """Check and install required dependencies"""
    required_packages = [
        'pandas', 'numpy', 'pyodbc', 'requests', 'openai', 'pyyaml', 'sqlalchemy', 
        'google-cloud-bigquery', 'simple-salesforce', 'boto3', 'azure-identity', 
        'azure-storage-file-datalake', 'azure-synapse-artifacts', 'msal', 
        'networkx', 'PyJWT', 'hashlib', 'secrets', 'prometheus_client', 
        'aiohttp', 'redis', 'scikit-learn', 'pydantic', 'matplotlib', 'seaborn', 
        'plotly', 'streamlit', 'streamlit-chat', 'sqlparse', 'pycryptodome',
        'azure-keyvault-secrets', 'azure-identity', 'cryptography', 'msal[broker]',
        'scipy'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_').replace('[', '_').replace(']', ''))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"Installing missing packages: {', '.join(missing_packages)}")
        import subprocess
        subprocess.check_call(['pip', 'install'] + missing_packages)
        print("Dependencies installed successfully!")
    else:
        print("All dependencies are already installed.")

# Security Best Practices
def encrypt_sensitive_data(data: str, key: str) -> str:
    """Encrypt sensitive data using AES encryption"""
    # Generate a random salt
    salt = secrets.token_bytes(16)
    
    # Derive key using PBKDF2
    kdf = PBKDF2(key, salt, dkLen=32, count=100000)
    
    # Create cipher
    cipher = AES.new(kdf, AES.MODE_CBC)
    
    # Encrypt data
    ct_bytes = cipher.encrypt(pad(data.encode(), AES.block_size))
    
    # Return salt + iv + ciphertext as base64
    return base64.b64encode(salt + cipher.iv + ct_bytes).decode('utf-8')

def decrypt_sensitive_data(encrypted_data: str, key: str) -> str:
    """Decrypt sensitive data using AES encryption"""
    # Decode base64
    data = base64.b64decode(encrypted_data)
    
    # Extract salt, iv, and ciphertext
    salt = data[:16]
    iv = data[16:32]
    ct = data[32:]
    
    # Derive key using PBKDF2
    kdf = PBKDF2(key, salt, dkLen=32, count=100000)
    
    # Create cipher
    cipher = AES.new(kdf, AES.MODE_CBC, iv=iv)
    
    # Decrypt data
    pt = unpad(cipher.decrypt(ct), AES.block_size)
    
    return pt.decode('utf-8')

def generate_jwt_token(payload: Dict, secret_key: str, expires_in: int = 3600) -> str:
    """Generate a JWT token for authentication"""
    payload['exp'] = datetime.utcnow() + timedelta(seconds=expires_in)
    return jwt.encode(payload, secret_key, algorithm='HS256')

def verify_jwt_token(token: str, secret_key: str) -> Dict:
    """Verify a JWT token and return the payload"""
    try:
        return jwt.decode(token, secret_key, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidTokenError:
        raise ValueError("Invalid token")

# Retry decorator for API calls
def retry_with_backoff(retries=3, backoff_factor=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            @tenacity.retry(
                wait=tenacity.wait_exponential(multiplier=backoff_factor, min=1, max=10),
                stop=tenacity.stop_after_attempt(retries),
                retry=tenacity.retry_if_exception_type((requests.exceptions.RequestException, openai.error.OpenAIError))
            )
            def _retry():
                return func(*args, **kwargs)
            return _retry()
        return wrapper
    return decorator

# Memory Management for Large Datasets
@contextmanager
def memory_efficient_processing(chunk_size: int = 50000):
    """Context manager for memory-efficient data processing"""
    try:
        yield chunk_size
    finally:
        gc.collect()  # Force garbage collection

# Caching Implementation
class CacheManager:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        try:
            self.redis_client = redis.from_url(redis_url)
            logger.info("Redis cache connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed: {str(e)}. Using in-memory cache instead.")
            self.redis_client = None
            self.in_memory_cache = {}
    
    @lru_cache(maxsize=128)
    def get_cached_schema(self, data_source_hash: str) -> Optional[Dict]:
        """Cache frequently accessed schemas"""
        if self.redis_client:
            try:
                cached = self.redis_client.get(f"schema:{data_source_hash}")
                return json.loads(cached) if cached else None
            except Exception as e:
                logger.error(f"Redis error: {str(e)}")
                return None
        else:
            return self.in_memory_cache.get(data_source_hash)
    
    def set_cached_schema(self, data_source_hash: str, schema: Dict, expire: int = 3600) -> None:
        """Cache schema with expiration"""
        if self.redis_client:
            try:
                self.redis_client.setex(
                    f"schema:{data_source_hash}", 
                    expire, 
                    json.dumps(schema)
                )
            except Exception as e:
                logger.error(f"Redis error: {str(e)}")
                self.in_memory_cache[data_source_hash] = schema
        else:
            self.in_memory_cache[data_source_hash] = schema
    
    def clear_cache(self) -> None:
        """Clear all cached data"""
        if self.redis_client:
            try:
                self.redis_client.flushall()
            except Exception as e:
                logger.error(f"Redis error: {str(e)}")
        else:
            self.in_memory_cache.clear()

# Data Profiler
class DataProfiler:
    def __init__(self, config: Dict):
        self.config = config
        self.data_quality_config = config.get('data_quality', {})
    
    def profile_data(self, data: pd.DataFrame, data_source_name: str) -> Dict:
        """Profile data and return quality metrics"""
        profile = {
            "data_source": data_source_name,
            "timestamp": datetime.now().isoformat(),
            "row_count": len(data),
            "column_count": len(data.columns),
            "columns": {},
            "overall_score": 0,
            "issues": [],
            "recommendations": []
        }
        
        # Overall metrics
        profile["memory_usage"] = data.memory_usage(deep=True).sum()
        
        # Column-level metrics
        for col in data.columns:
            col_profile = {
                "name": col,
                "data_type": str(data[col].dtype),
                "null_count": data[col].isnull().sum(),
                "null_percentage": round(data[col].isnull().sum() / len(data) * 100, 2),
                "unique_count": data[col].nunique(),
                "duplicate_count": len(data) - data[col].nunique(),
                "duplicate_percentage": round((len(data) - data[col].nunique()) / len(data) * 100, 2)
            }
            
            # Add numeric-specific metrics
            if pd.api.types.is_numeric_dtype(data[col]):
                col_profile.update({
                    "min": float(data[col].min()) if not pd.isna(data[col].min()) else None,
                    "max": float(data[col].max()) if not pd.isna(data[col].max()) else None,
                    "mean": float(data[col].mean()) if not pd.isna(data[col].mean()) else None,
                    "median": float(data[col].median()) if not pd.isna(data[col].median()) else None,
                    "std": float(data[col].std()) if not pd.isna(data[col].std()) else None,
                    "outliers": self._detect_outliers(data[col])
                })
            
            # Add categorical-specific metrics
            elif pd.api.types.is_string_dtype(data[col]) or pd.api.types.is_categorical_dtype(data[col]):
                value_counts = data[col].value_counts()
                col_profile.update({
                    "top_values": value_counts.head(10).to_dict(),
                    "value_distribution": "uniform" if value_counts.std() < 1 else "skewed"
                })
            
            # Add date-specific metrics
            elif pd.api.types.is_datetime64_dtype(data[col]):
                col_profile.update({
                    "min_date": data[col].min().isoformat() if not pd.isna(data[col].min()) else None,
                    "max_date": data[col].max().isoformat() if not pd.isna(data[col].max()) else None,
                    "date_range_days": (data[col].max() - data[col].min()).days if not (pd.isna(data[col].min()) or pd.isna(data[col].max())) else None
                })
            
            profile["columns"][col] = col_profile
        
        # Apply rule-based checks
        issues = self._apply_rule_based_checks(data, profile)
        profile["issues"] = issues
        
        # Calculate overall score
        profile["overall_score"] = self._calculate_overall_score(profile)
        
        # Update Prometheus metric
        DATA_QUALITY_SCORE.labels(data_source=data_source_name).set(profile["overall_score"])
        
        return profile
    
    def _detect_outliers(self, series: pd.Series) -> List[int]:
        """Detect outliers in a numeric series"""
        if self.data_quality_config.get('outlier_method') == 'zscore':
            z_scores = np.abs(stats.zscore(series.dropna()))
            return np.where(z_scores > self.data_quality_config.get('outlier_threshold', 3.0))[0].tolist()
        elif self.data_quality_config.get('outlier_method') == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            return series[(series < lower_bound) | (series > upper_bound)].index.tolist()
        else:
            return []
    
    def _apply_rule_based_checks(self, data: pd.DataFrame, profile: Dict) -> List[Dict]:
        """Apply rule-based checks to identify data quality issues"""
        issues = []
        
        # Check for high null percentages
        max_null_percentage = self.data_quality_config.get('max_null_percentage', 30.0)
        for col_name, col_profile in profile["columns"].items():
            if col_profile["null_percentage"] > max_null_percentage:
                issues.append({
                    "type": "high_null_percentage",
                    "severity": "warning" if col_profile["null_percentage"] < 50 else "error",
                    "column": col_name,
                    "message": f"Column '{col_name}' has {col_profile['null_percentage']}% null values, exceeding threshold of {max_null_percentage}%"
                })
        
        # Check for high duplicate percentages
        max_duplicate_percentage = self.data_quality_config.get('max_duplicate_percentage', 10.0)
        for col_name, col_profile in profile["columns"].items():
            if col_profile["duplicate_percentage"] > max_duplicate_percentage:
                issues.append({
                    "type": "high_duplicate_percentage",
                    "severity": "warning",
                    "column": col_name,
                    "message": f"Column '{col_name}' has {col_profile['duplicate_percentage']}% duplicate values, exceeding threshold of {max_duplicate_percentage}%"
                })
        
        # Check for data type consistency
        if self.data_quality_config.get('check_data_types', True):
            for col in data.columns:
                # Check if numeric columns have non-numeric values
                if pd.api.types.is_numeric_dtype(data[col]):
                    non_numeric = data[col].apply(lambda x: isinstance(x, (int, float, np.number)))
                    if not non_numeric.all():
                        issues.append({
                            "type": "data_type_inconsistency",
                            "severity": "warning",
                            "column": col,
                            "message": f"Column '{col}' is defined as numeric but contains non-numeric values"
                        })
        
        # Check for value ranges
        if self.data_quality_config.get('check_value_ranges', True):
            for col_name, col_profile in profile["columns"].items():
                if "min" in col_profile and "max" in col_profile:
                    # Check for negative values in columns that shouldn't have them
                    if col_name.lower().endswith(('_id', 'id', 'count', 'quantity', 'amount')) and col_profile["min"] < 0:
                        issues.append({
                            "type": "unexpected_negative_values",
                            "severity": "warning",
                            "column": col_name,
                            "message": f"Column '{col_name}' contains negative values but appears to be a count/ID field"
                        })
        
        # Apply custom rules
        for rule in self.data_quality_config.get('custom_rules', []):
            rule_type = rule.get('type')
            if rule_type == 'column_pattern':
                pattern = rule.get('pattern')
                columns_to_check = rule.get('columns', [])
                for col in columns_to_check:
                    if col in data.columns:
                        if not re.match(pattern, str(data[col].iloc[0])):
                            issues.append({
                                "type": "custom_rule_violation",
                                "severity": rule.get('severity', 'warning'),
                                "column": col,
                                "message": f"Column '{col}' violates custom pattern rule: {pattern}"
                            })
        
        return issues
    
    def _calculate_overall_score(self, profile: Dict) -> float:
        """Calculate an overall data quality score (0-100)"""
        # Start with a perfect score
        score = 100.0
        
        # Deduct points for issues
        for issue in profile["issues"]:
            if issue["severity"] == "error":
                score -= 20
            else:  # warning
                score -= 5
        
        # Deduct points for high null percentages
        for col_name, col_profile in profile["columns"].items():
            if col_profile["null_percentage"] > 50:
                score -= 30
            elif col_profile["null_percentage"] > 20:
                score -= 10
        
        # Deduct points for high duplicate percentages
        for col_name, col_profile in profile["columns"].items():
            if col_profile["duplicate_percentage"] > 50:
                score -= 20
            elif col_profile["duplicate_percentage"] > 20:
                score -= 5
        
        # Ensure score is between 0 and 100
        return max(0, min(100, score))

# Schema Drift Detector
class SchemaDriftDetector:
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
    
    def detect_schema_drift(self, data_source_name: str, current_schema: Dict) -> Dict:
        """Detect schema drift compared to previously cached schema"""
        # Generate hash for current schema
        current_schema_hash = hashlib.md5(json.dumps(current_schema, sort_keys=True).encode()).hexdigest()
        
        # Get cached schema
        cached_schema = self.cache_manager.get_cached_schema(data_source_name)
        
        if not cached_schema:
            # No cached schema, this is the first time we're seeing this data source
            self.cache_manager.set_cached_schema(data_source_name, current_schema)
            return {
                "drift_detected": False,
                "message": "First time analyzing this data source, no previous schema to compare against",
                "changes": []
            }
        
        # Generate hash for cached schema
        cached_schema_hash = hashlib.md5(json.dumps(cached_schema, sort_keys=True).encode()).hexdigest()
        
        # Compare hashes
        if current_schema_hash == cached_schema_hash:
            return {
                "drift_detected": False,
                "message": "No schema drift detected",
                "changes": []
            }
        
        # Detailed comparison
        changes = self._compare_schemas(cached_schema, current_schema)
        
        # Update cached schema
        self.cache_manager.set_cached_schema(data_source_name, current_schema)
        
        # Record metric if drift was detected
        if changes:
            SCHEMA_DRIFT_DETECTED.labels(data_source=data_source_name).inc()
        
        return {
            "drift_detected": len(changes) > 0,
            "message": f"Schema drift detected with {len(changes)} changes",
            "changes": changes
        }
    
    def _compare_schemas(self, old_schema: Dict, new_schema: Dict) -> List[Dict]:
        """Compare two schemas and identify changes"""
        changes = []
        
        # Check for new tables
        old_tables = set(old_schema.keys())
        new_tables = set(new_schema.keys())
        
        for table in new_tables - old_tables:
            changes.append({
                "type": "table_added",
                "table": table,
                "severity": "info"
            })
        
        for table in old_tables - new_tables:
            changes.append({
                "type": "table_removed",
                "table": table,
                "severity": "warning"
            })
        
        # Check for changes in existing tables
        for table in old_tables & new_tables:
            old_columns = {col["name"]: col for col in old_schema[table].get("columns", [])}
            new_columns = {col["name"]: col for col in new_schema[table].get("columns", [])}
            
            old_column_names = set(old_columns.keys())
            new_column_names = set(new_columns.keys())
            
            # Check for new columns
            for column in new_column_names - old_column_names:
                changes.append({
                    "type": "column_added",
                    "table": table,
                    "column": column,
                    "severity": "info"
                })
            
            # Check for removed columns
            for column in old_column_names - new_column_names:
                changes.append({
                    "type": "column_removed",
                    "table": table,
                    "column": column,
                    "severity": "warning"
                })
            
            # Check for changed columns
            for column in old_column_names & new_column_names:
                old_col = old_columns[column]
                new_col = new_columns[column]
                
                if old_col["type"] != new_col["type"]:
                    changes.append({
                        "type": "column_type_changed",
                        "table": table,
                        "column": column,
                        "old_type": old_col["type"],
                        "new_type": new_col["type"],
                        "severity": "warning"
                    })
        
        return changes

# Enterprise Security Manager
class EnterpriseSecurityManager:
    def __init__(self, config: Dict):
        self.config = config
        self.conditional_access_policies = []
        
    def setup_conditional_access(self) -> Dict:
        """Configure Microsoft Entra Conditional Access"""
        return {
            "policies": [
                {
                    "name": "FabricMFAPolicy",
                    "conditions": {
                        "applications": ["https://analysis.windows.net/powerbi/api/"],
                        "users": "all",
                        "locations": "trusted_ips_only"
                    },
                    "controls": {
                        "mfa": "required",
                        "device_compliance": "required"
                    }
                }
            ]
        }
    
    def validate_service_principal_permissions(self) -> bool:
        """Validate SPN has required Fabric permissions"""
        required_scopes = [
            "https://analysis.windows.net/powerbi/api/.default",
            "Workspace.ReadWrite.All",
            "Dataset.ReadWrite.All",
            "Pipeline.Deploy"
        ]
        
        try:
            # Get access token
            app = ConfidentialClientApplication(
                client_id=self.config['fabric']['client_id'],
                authority=f"https://login.microsoftonline.com/{self.config['fabric']['tenant_id']}",
                client_credential=self.config['fabric']['client_secret']
            )
            
            result = app.acquire_token_for_client(scopes=required_scopes)
            
            if "access_token" in result:
                return True
            else:
                logger.error(f"Token acquisition failed: {result.get('error_description')}")
                return False
        except Exception as e:
            logger.error(f"Error validating service principal permissions: {str(e)}")
            return False

# Fabric Deployment Pipeline
class FabricDeploymentPipeline:
    def __init__(self, config: Dict):
        self.config = config
        self.pipeline_id = config.get('fabric', {}).get('pipeline_id')
        
    def create_deployment_pipeline(self) -> Dict:
        """Create Fabric deployment pipeline"""
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        
        pipeline_config = {
            "displayName": "AI Generated Reports Pipeline",
            "description": "Automated AI-powered BI deployment pipeline"
        }
        
        response = requests.post(
            "https://api.powerbi.com/v1.0/myorg/pipelines",
            headers=headers,
            json=pipeline_config
        )
        
        if response.status_code == 201:
            pipeline_data = response.json()
            self.pipeline_id = pipeline_data.get('id')
            return pipeline_data
        else:
            raise Exception(f"Pipeline creation failed: {response.text}")
    
    def deploy_to_pipeline_stage(self, stage: str, content_id: str) -> Dict:
        """Deploy content through pipeline stages (Dev -> Test -> Prod)"""
        if not self.pipeline_id:
            raise Exception("Pipeline ID not set. Create pipeline first.")
            
        stage_mapping = {"dev": 0, "test": 1, "prod": 2}
        
        deployment_payload = {
            "sourceStageOrder": stage_mapping.get(stage, 0),
            "options": {
                "allowCreateArtifact": True,
                "allowOverwriteArtifact": True
            },
            "note": f"AI-generated deployment to {stage}"
        }
        
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        
        response = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/pipelines/{self.pipeline_id}/deployAll",
            headers=headers,
            json=deployment_payload
        )
        
        return response.json()
    
    def _get_access_token(self) -> str:
        """Get Power BI access token"""
        app = ConfidentialClientApplication(
            client_id=self.config['fabric']['client_id'],
            authority=f"https://login.microsoftonline.com/{self.config['fabric']['tenant_id']}",
            client_credential=self.config['fabric']['client_secret']
        )
        
        result = app.acquire_token_for_client(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )
        
        if "access_token" in result:
            return result["access_token"]
        else:
            raise Exception(f"Token acquisition failed: {result.get('error_description')}")

# Fabric Compliance Manager
class FabricComplianceManager:
    def __init__(self, config: Dict):
        self.config = config
        
    def setup_audit_logging(self) -> Dict:
        """Configure comprehensive audit logging"""
        return {
            "audit_policies": [
                {
                    "name": "AIReportGeneration",
                    "events": ["ReportCreate", "DataAccess", "UserQuery"],
                    "retention_days": 2555,  # 7 years for compliance
                    "export_to": "Azure_Log_Analytics"
                }
            ],
            "compliance_standards": self.config.get('compliance', {}).get('standards', ["GDPR", "SOX", "HIPAA", "SOC2"]),
            "data_classification": self.config.get('compliance', {}).get('data_classification', "Confidential")
        }
    
    def generate_compliance_report(self) -> Dict:
        """Generate compliance status report"""
        return {
            "status": "compliant",
            "last_audit": datetime.now().isoformat(),
            "findings": [],
            "standards": self.config.get('compliance', {}).get('standards', []),
            "data_classification": self.config.get('compliance', {}).get('data_classification', "Confidential")
        }

# Enterprise Integration Hub
class EnterpriseIntegrationHub:
    def __init__(self, config: Dict):
        self.config = config
        
    def setup_azure_devops_integration(self) -> Dict:
        """Integrate with Azure DevOps for CI/CD"""
        return {
            "project_url": self.config.get('integrations', {}).get('azure_devops_project'),
            "build_pipeline": "ai-powerbi-build",
            "release_pipeline": "ai-powerbi-release",
            "artifacts": ["pbit_templates", "dax_measures", "documentation"]
        }
    
    def setup_teams_notifications(self) -> Dict:
        """Configure Microsoft Teams notifications"""
        return {
            "webhook_url": self.config.get('integrations', {}).get('teams_webhook'),
            "notification_events": [
                "report_generated", "deployment_complete", 
                "error_occurred", "compliance_alert", "schema_drift", "data_quality_issue"
            ]
        }
    
    def setup_power_automate_flows(self) -> List[Dict]:
        """Configure Power Automate workflows"""
        return [
            {
                "name": "AI Report Approval",
                "trigger": "report_generation_complete",
                "approvers": ["business_owner", "data_steward"],
                "auto_deploy_on_approval": True
            },
            {
                "name": "Data Anomaly Detection",
                "trigger": "data_quality_check",
                "threshold": "accuracy < 95%",
                "action": "alert_data_team"
            },
            {
                "name": "Schema Drift Alert",
                "trigger": "schema_drift_detected",
                "action": "alert_data_team"
            }
        ]
    
    def send_teams_notification(self, message: str, event_type: str) -> bool:
        """Send notification to Microsoft Teams"""
        webhook_url = self.config.get('integrations', {}).get('teams_webhook')
        if not webhook_url:
            logger.warning("Teams webhook URL not configured")
            return False
            
        try:
            payload = {
                "text": f"[{event_type.upper()}] {message}",
                "sections": [
                    {
                        "activityTitle": "Power BI AI Generator",
                        "activitySubtitle": f"Event: {event_type}",
                        "activityText": message,
                        "activityImage": "https://adaptivecards.io/content/cats/3.png"
                    }
                ]
            }
            
            response = requests.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error sending Teams notification: {str(e)}")
            return False

# Security Hardening Manager
class SecurityHardeningManager:
    def __init__(self, config: Dict):
        self.config = config
        self.key_vault_client = self._setup_key_vault()
        
    def _setup_key_vault(self):
        """Configure Azure Key Vault for secrets management"""
        key_vault_url = self.config.get('integrations', {}).get('key_vault_url')
        if not key_vault_url:
            logger.warning("Key Vault URL not configured")
            return None
            
        try:
            credential = DefaultAzureCredential()
            return SecretClient(
                vault_url=key_vault_url,
                credential=credential
            )
        except Exception as e:
            logger.error(f"Error setting up Key Vault: {str(e)}")
            return None
    
    def encrypt_sensitive_config(self, config: Dict) -> Dict:
        """Encrypt sensitive configuration data"""
        if not self.key_vault_client:
            logger.warning("Key Vault client not available")
            return config
            
        encrypted_config = config.copy()
        
        # List of sensitive keys to encrypt
        sensitive_keys = [
            'openai.api_key', 
            'fabric.client_secret',
            'data_sources.*.password', 
            'data_sources.*.access_key_id',
            'data_sources.*.secret_access_key'
        ]
        
        for key_path in sensitive_keys:
            # Handle nested keys
            if '.' in key_path:
                parts = key_path.split('.')
                root_key = parts[0]
                nested_key = parts[1]
                
                if root_key in encrypted_config and nested_key in encrypted_config[root_key]:
                    vault_name = f"ai-powerbi-{key_path.replace('.', '-')}"
                    try:
                        self.key_vault_client.set_secret(vault_name, str(encrypted_config[root_key][nested_key]))
                        encrypted_config[root_key][nested_key] = f"@Microsoft.KeyVault({vault_name})"
                    except Exception as e:
                        logger.error(f"Error encrypting {key_path}: {str(e)}")
            else:
                if key_path in encrypted_config:
                    vault_name = f"ai-powerbi-{key_path}"
                    try:
                        self.key_vault_client.set_secret(vault_name, str(encrypted_config[key_path]))
                        encrypted_config[key_path] = f"@Microsoft.KeyVault({vault_name})"
                    except Exception as e:
                        logger.error(f"Error encrypting {key_path}: {str(e)}")
        
        return encrypted_config
    
    def decrypt_sensitive_config(self, config: Dict) -> Dict:
        """Decrypt sensitive configuration data"""
        if not self.key_vault_client:
            logger.warning("Key Vault client not available")
            return config
            
        decrypted_config = config.copy()
        
        # Find all Key Vault references
        def decrypt_value(value):
            if isinstance(value, str) and value.startswith("@Microsoft.KeyVault("):
                vault_name = value.replace("@Microsoft.KeyVault(", "").replace(")", "")
                try:
                    secret = self.key_vault_client.get_secret(vault_name)
                    return secret.value
                except Exception as e:
                    logger.error(f"Error decrypting {vault_name}: {str(e)}")
                    return value
            return value
        
        # Recursively decrypt all values
        def decrypt_dict(d):
            for key, value in d.items():
                if isinstance(value, dict):
                    d[key] = decrypt_dict(value)
                elif isinstance(value, list):
                    d[key] = [decrypt_item(item) for item in value]
                else:
                    d[key] = decrypt_value(value)
            return d
        
        def decrypt_item(item):
            if isinstance(item, dict):
                return decrypt_dict(item)
            elif isinstance(item, list):
                return [decrypt_item(i) for i in item]
            else:
                return decrypt_value(item)
        
        return decrypt_dict(decrypted_config)

# Data Source Abstract Class
class DataSource(ABC):
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.schema = None
    
    @abstractmethod
    def connect(self) -> bool:
        """Connect to the data source"""
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict:
        """Get the schema of the data source"""
        pass
    
    @abstractmethod
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from the data source"""
        pass

# SQL Server Data Source
class SQLServerDataSource(DataSource):
    def connect(self) -> bool:
        try:
            connection_string = (
                f"DRIVER={{{self.config.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['username']};"
                f"PWD={self.config['password']}"
            )
            self.connection = pyodbc.connect(connection_string)
            logger.info("Connected to SQL Server")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            cursor = self.connection.cursor()
            schema = {}
            
            # Get tables
            tables = cursor.tables(tableType='TABLE').fetchall()
            for table in tables:
                table_name = table.table_name
                schema[table_name] = {"columns": []}
                
                # Get columns
                columns = cursor.columns(table=table_name).fetchall()
                for column in columns:
                    schema[table_name]["columns"].append({
                        "name": column.column_name,
                        "type": column.type_name
                    })
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get SQL Server schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first table for sample data
            table_name = list(self.schema.keys())[0] if self.schema else None
            if not table_name:
                return pd.DataFrame()
            
            query = f"SELECT TOP {limit} * FROM {table_name}"
            return pd.read_sql(query, self.connection)
        except Exception as e:
            logger.error(f"Failed to get sample data from SQL Server: {str(e)}")
            return pd.DataFrame()

# PostgreSQL Data Source
class PostgreSQLDataSource(DataSource):
    def connect(self) -> bool:
        try:
            connection_string = (
                f"postgresql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config.get('port', 5432)}/{self.config['database']}"
            )
            self.connection = create_engine(connection_string)
            logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            schema = {}
            with self.connection.connect() as conn:
                # Get tables
                tables = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)).fetchall()
                
                for table in tables:
                    table_name = table[0]
                    schema[table_name] = {"columns": []}
                    
                    # Get columns
                    columns = conn.execute(text("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' AND table_name = :table_name
                    """), {"table_name": table_name}).fetchall()
                    
                    for column in columns:
                        schema[table_name]["columns"].append({
                            "name": column[0],
                            "type": column[1]
                        })
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first table for sample data
            table_name = list(self.schema.keys())[0] if self.schema else None
            if not table_name:
                return pd.DataFrame()
            
            query = text(f"SELECT * FROM {table_name} LIMIT {limit}")
            with self.connection.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Failed to get sample data from PostgreSQL: {str(e)}")
            return pd.DataFrame()

# CSV Data Source
class CSVDataSource(DataSource):
    def connect(self) -> bool:
        try:
            if not os.path.exists(self.config['file_path']):
                logger.error(f"CSV file not found: {self.config['file_path']}")
                return False
            
            # For CSV, we don't need a persistent connection
            self.connection = True
            logger.info("CSV file accessible")
            return True
        except Exception as e:
            logger.error(f"Failed to access CSV file: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            df = pd.read_csv(self.config['file_path'], nrows=1)
            schema = {
                "csv_data": {
                    "columns": [
                        {"name": col, "type": str(df[col].dtype)}
                        for col in df.columns
                    ]
                }
            }
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get CSV schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            return pd.read_csv(self.config['file_path'], nrows=limit)
        except Exception as e:
            logger.error(f"Failed to get sample data from CSV: {str(e)}")
            return pd.DataFrame()

# Excel Data Source
class ExcelDataSource(DataSource):
    def connect(self) -> bool:
        try:
            if not os.path.exists(self.config['file_path']):
                logger.error(f"Excel file not found: {self.config['file_path']}")
                return False
            
            # For Excel, we don't need a persistent connection
            self.connection = True
            logger.info("Excel file accessible")
            return True
        except Exception as e:
            logger.error(f"Failed to access Excel file: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            # Get all sheet names
            xls = pd.ExcelFile(self.config['file_path'])
            schema = {}
            
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(self.config['file_path'], sheet_name=sheet_name, nrows=1)
                schema[sheet_name] = {
                    "columns": [
                        {"name": col, "type": str(df[col].dtype)}
                        for col in df.columns
                    ]
                }
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get Excel schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first sheet for sample data
            sheet_name = list(self.schema.keys())[0] if self.schema else None
            if not sheet_name:
                return pd.DataFrame()
            
            return pd.read_excel(self.config['file_path'], sheet_name=sheet_name, nrows=limit)
        except Exception as e:
            logger.error(f"Failed to get sample data from Excel: {str(e)}")
            return pd.DataFrame()

# API Data Source
class APIDataSource(DataSource):
    def connect(self) -> bool:
        try:
            # For API, we don't need a persistent connection
            self.connection = True
            logger.info("API endpoint accessible")
            return True
        except Exception as e:
            logger.error(f"Failed to access API endpoint: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            # Get a sample response to infer schema
            headers = self.config.get('headers', {})
            params = self.config.get('params', {})
            
            response = requests.get(
                self.config['url'],
                headers=headers,
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # If data is a list, use first item
                if isinstance(data, list) and data:
                    data = data[0]
                
                # Infer schema from keys
                schema = {
                    "api_data": {
                        "columns": [
                            {"name": key, "type": type(value).__name__}
                            for key, value in data.items()
                        ]
                    }
                }
                
                self.schema = schema
                return schema
            else:
                logger.error(f"API request failed with status code: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Failed to get API schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            headers = self.config.get('headers', {})
            params = self.config.get('params', {})
            paginate = self.config.get('paginate', False)
            
            if paginate:
                # Handle pagination
                all_data = []
                page = 1
                while len(all_data) < limit:
                    params['page'] = page
                    response = requests.get(
                        self.config['url'],
                        headers=headers,
                        params=params
                    )
                    
                    if response.status_code == 200:
                        page_data = response.json()
                        if isinstance(page_data, list):
                            all_data.extend(page_data)
                            if len(page_data) == 0:
                                break
                        else:
                            all_data.append(page_data)
                            break
                    else:
                        logger.error(f"API request failed with status code: {response.status_code}")
                        break
                    
                    page += 1
                
                return pd.DataFrame(all_data[:limit])
            else:
                # Single request
                response = requests.get(
                    self.config['url'],
                    headers=headers,
                    params=params
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        return pd.DataFrame(data[:limit])
                    else:
                        return pd.DataFrame([data])
                else:
                    logger.error(f"API request failed with status code: {response.status_code}")
                    return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to get sample data from API: {str(e)}")
            return pd.DataFrame()

# BigQuery Data Source
class BigQueryDataSource(DataSource):
    def connect(self) -> bool:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.config['credentials_path']
            )
            self.connection = bigquery.Client(credentials=credentials, project=self.config['project_id'])
            logger.info("Connected to BigQuery")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            schema = {}
            datasets = list(self.connection.list_datasets())
            
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                tables = list(self.connection.list_tables(dataset_id))
                
                for table in tables:
                    table_id = table.table_id
                    table_ref = self.connection.dataset(dataset_id).table(table_id)
                    table_obj = self.connection.get_table(table_ref)
                    
                    full_table_name = f"{dataset_id}.{table_id}"
                    schema[full_table_name] = {
                        "columns": [
                            {"name": field.name, "type": field.field_type}
                            for field in table_obj.schema
                        ]
                    }
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get BigQuery schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first table for sample data
            table_name = list(self.schema.keys())[0] if self.schema else None
            if not table_name:
                return pd.DataFrame()
            
            query = f"SELECT * FROM `{self.config['project_id']}.{table_name}` LIMIT {limit}"
            return self.connection.query(query).to_dataframe()
        except Exception as e:
            logger.error(f"Failed to get sample data from BigQuery: {str(e)}")
            return pd.DataFrame()

# Salesforce Data Source
class SalesforceDataSource(DataSource):
    def connect(self) -> bool:
        try:
            self.connection = Salesforce(
                username=self.config['username'],
                password=self.config['password'],
                security_token=self.config['security_token'],
                sandbox=self.config.get('sandbox', False)
            )
            logger.info("Connected to Salesforce")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            schema = {}
            
            # Get all objects
            global_describe = self.connection.describe()
            
            for sobject in global_describe['sobjects']:
                if sobject['queryable']:
                    object_name = sobject['name']
                    schema[object_name] = {"columns": []}
                    
                    # Get fields
                    object_describe = self.connection.__getattr__(object_name).describe()
                    for field in object_describe['fields']:
                        schema[object_name]["columns"].append({
                            "name": field['name'],
                            "type": field['type']
                        })
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get Salesforce schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first object for sample data
            object_name = list(self.schema.keys())[0] if self.schema else None
            if not object_name:
                return pd.DataFrame()
            
            query = f"SELECT * FROM {object_name} LIMIT {limit}"
            result = self.connection.query_all(query)
            
            # Convert to DataFrame - FIXED: Iterate over result['records']
            records = []
            for record in result['records']:
                records.append({field: record[field] for field in record})
            
            return pd.DataFrame(records)
        except Exception as e:
            logger.error(f"Failed to get sample data from Salesforce: {str(e)}")
            return pd.DataFrame()

# S3 Data Source
class S3DataSource(DataSource):
    def connect(self) -> bool:
        try:
            self.connection = boto3.client(
                's3',
                aws_access_key_id=self.config['access_key_id'],
                aws_secret_access_key=self.config['secret_access_key'],
                region_name=self.config.get('region', 'us-east-1')
            )
            logger.info("Connected to S3")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            bucket_name = self.config['bucket_name']
            prefix = self.config.get('prefix', '')
            
            # List objects in bucket
            objects = self.connection.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            # Get first CSV/JSON file for schema inference
            for obj in objects.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') or key.endswith('.json'):
                    # Download file to temp location
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        self.connection.download_fileobj(bucket_name, key, temp_file)
                        temp_path = temp_file.name
                    
                    try:
                        if key.endswith('.csv'):
                            df = pd.read_csv(temp_path, nrows=1)
                        else:  # JSON
                            df = pd.read_json(temp_path, nrows=1)
                        
                        schema = {
                            key: {
                                "columns": [
                                    {"name": col, "type": str(df[col].dtype)}
                                    for col in df.columns
                                ]
                            }
                        }
                        
                        self.schema = schema
                        return schema
                    finally:
                        os.unlink(temp_path)
            
            logger.warning("No CSV or JSON files found in S3 bucket")
            return {}
        except Exception as e:
            logger.error(f"Failed to get S3 schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            bucket_name = self.config['bucket_name']
            prefix = self.config.get('prefix', '')
            
            # List objects in bucket
            objects = self.connection.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            # Get first CSV/JSON file for sample data
            for obj in objects.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') or key.endswith('.json'):
                    # Download file to temp location
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        self.connection.download_fileobj(bucket_name, key, temp_file)
                        temp_path = temp_file.name
                    
                    try:
                        if key.endswith('.csv'):
                            df = pd.read_csv(temp_path, nrows=limit)
                        else:  # JSON
                            df = pd.read_json(temp_path, nrows=limit)
                        
                        return df
                    finally:
                        os.unlink(temp_path)
            
            logger.warning("No CSV or JSON files found in S3 bucket")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to get sample data from S3: {str(e)}")
            return pd.DataFrame()

# OneLake Data Source
class OneLakeDataSource(DataSource):
    def __init__(self, config: Dict):
        super().__init__(config)
        self.workspace_id = config.get('workspace_id')
        self.client = self._get_onelake_client()
    
    def _get_onelake_client(self) -> DataLakeServiceClient:
        """Get OneLake client with correct endpoint format"""
        account_url = "https://onelake.dfs.fabric.microsoft.com/"
        credential = DefaultAzureCredential()
        return DataLakeServiceClient(account_url=account_url, credential=credential)
    
    def connect(self) -> bool:
        try:
            # Test connection by listing file systems
            self.client.get_file_system_client(self.workspace_id).get_file_system_properties()
            logger.info("Connected to OneLake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OneLake: {str(e)}")
            return False
    
    def get_schema(self) -> Dict:
        if not self.connection:
            if not self.connect():
                return {}
        
        try:
            schema = {}
            file_system_client = self.client.get_file_system_client(self.workspace_id)
            
            # List directories
            paths = file_system_client.get_paths()
            
            for path in paths:
                if path.is_directory:
                    dir_name = path.name
                    schema[dir_name] = {"columns": []}
                    
                    # List files in directory
                    dir_paths = file_system_client.get_paths(path.name)
                    for file_path in dir_paths:
                        if not file_path.is_directory and (file_path.name.endswith('.parquet') or file_path.name.endswith('.csv')):
                            # Get file schema (simplified for example)
                            schema[dir_name]["columns"].append({
                                "name": file_path.name.split('/')[-1].split('.')[0],
                                "type": "string"  # Simplified
                            })
            
            self.schema = schema
            return schema
        except Exception as e:
            logger.error(f"Failed to get OneLake schema: {str(e)}")
            return {}
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Get first directory for sample data
            dir_name = list(self.schema.keys())[0] if self.schema else None
            if not dir_name:
                return pd.DataFrame()
            
            file_system_client = self.client.get_file_system_client(self.workspace_id)
            dir_paths = file_system_client.get_paths(dir_name)
            
            # Get first file for sample data
            for file_path in dir_paths:
                if not file_path.is_directory and (file_path.name.endswith('.parquet') or file_path.name.endswith('.csv')):
                    file_client = file_system_client.get_file_client(file_path.name)
                    
                    # Download file to temp location
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        download = file_client.download_file()
                        download.readinto(temp_file)
                        temp_path = temp_file.name
                    
                    try:
                        if file_path.name.endswith('.parquet'):
                            df = pd.read_parquet(temp_path)
                        else:  # CSV
                            df = pd.read_csv(temp_path)
                        
                        return df.head(limit)
                    finally:
                        os.unlink(temp_path)
            
            logger.warning("No data files found in OneLake directory")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to get sample data from OneLake: {str(e)}")
            return pd.DataFrame()

# Python Script Sanitizer
class PythonScriptSanitizer:
    def __init__(self, sandbox_path: str = "/tmp/sandbox"):
        self.sandbox_path = sandbox_path
        # Relaxed disallowed imports - only dangerous ones
        self.disallowed_imports = {
            'os', 'subprocess', 'sys', 'shutil', 'glob', 'tempfile', 'pathlib',
            'requests', 'urllib', 'http', 'socket', 'ftplib', 'smtplib', 'poplib',
            'imaplib', 'nntplib', 'telnetlib', 'uuid', 'platform', 'ctypes',
            'pickle', 'shelve', 'dbm', 'sqlite3', 'pymongo', 'redis', 'psycopg2',
            'mysql-connector', 'cx_Oracle', 'pyodbc', 'sqlalchemy'
        }
        # Relaxed disallowed functions - only dangerous ones
        self.disallowed_functions = {
            'open', 'file', 'input', 'raw_input', 'exec', 'eval', 'compile',
            '__import__', 'reload', 'exit', 'quit'
        }
    
    def sanitize_python_script(self, script: str) -> Dict:
        """Check Python script for disallowed operations"""
        try:
            # Parse the script into an AST
            tree = ast.parse(script)
            
            # Check for disallowed imports
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in self.disallowed_imports:
                            return {
                                "is_safe": False,
                                "error": f"Disallowed import: {alias.name}",
                                "suggestion": f"Replace {alias.name} with a safer alternative"
                            }
                elif isinstance(node, ast.ImportFrom):
                    if node.module in self.disallowed_imports:
                        return {
                            "is_safe": False,
                            "error": f"Disallowed import from: {node.module}",
                            "suggestion": f"Replace {node.module} with a safer alternative"
                        }
                
                # Check for disallowed function calls
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name) and node.func.id in self.disallowed_functions:
                        return {
                            "is_safe": False,
                            "error": f"Disallowed function call: {node.func.id}",
                            "suggestion": f"Replace {node.func.id} with a safer alternative"
                        }
                
                # Check for file operations outside sandbox
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == 'open':
                        if len(node.args) > 0 and isinstance(node.args[0], ast.Str):
                            file_path = node.args[0].s
                            if not file_path.startswith(self.sandbox_path):
                                return {
                                    "is_safe": False,
                                    "error": f"File access outside sandbox: {file_path}",
                                    "suggestion": f"Use files within {self.sandbox_path}"
                                }
                
                # Check for network calls
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name):
                        if node.func.value.id in ['requests', 'urllib', 'http', 'socket']:
                            return {
                                "is_safe": False,
                                "error": f"Network call detected: {node.func.value.id}.{node.func.attr}",
                                "suggestion": "Avoid network calls in generated scripts"
                            }
                
                # Check for subprocess usage
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == 'subprocess':
                        return {
                            "is_safe": False,
                            "error": f"Subprocess call detected: {node.func.attr}",
                            "suggestion": "Avoid subprocess calls in generated scripts"
                        }
            
            # If we get here, the script is safe
            return {"is_safe": True}
        except SyntaxError as e:
            return {
                "is_safe": False,
                "error": f"Syntax error in Python script: {str(e)}",
                "suggestion": "Fix the syntax errors in the script"
            }
        except Exception as e:
            return {
                "is_safe": False,
                "error": f"Error sanitizing Python script: {str(e)}",
                "suggestion": "Review the script for potential issues"
            }

# Table Selection Function
def select_table_list(schema: Dict, requirements: str, use_llm: bool = True, config: Dict = None) -> List[str]:
    """Select relevant tables from schema based on requirements"""
    if not use_llm:
        # Simple heuristic-based selection
        all_tables = list(schema.keys())
        
        # Extract keywords from requirements
        keywords = re.findall(r'\b\w+\b', requirements.lower())
        
        # Score tables based on keyword matches in table names
        table_scores = {}
        for table in all_tables:
            score = 0
            table_lower = table.lower()
            for keyword in keywords:
                if keyword in table_lower:
                    score += 1
            table_scores[table] = score
        
        # Sort by score and return top tables
        sorted_tables = sorted(table_scores.items(), key=lambda x: x[1], reverse=True)
        return [table for table, score in sorted_tables[:5]]  # Return top 5 tables
    else:
        # LLM-based selection
        try:
            # Use config instead of environment variable
            openai_api_key = config.get('openai', {}).get('api_key') if config else None
            if not openai_api_key:
                logger.warning("OpenAI API key not found in config, falling back to heuristic selection")
                return select_table_list(schema, requirements, use_llm=False, config=config)
            
            openai_client = OpenAI(api_key=openai_api_key)
            
            # Prepare prompt for LLM
            prompt = f"""
            Given the following database schema and user requirements, select the most relevant tables for generating the requested report.
            
            Schema:
            {json.dumps(schema, indent=2)}
            
            Requirements:
            {requirements}
            
            Return a JSON array of table names that are most relevant to the requirements. Focus on tables that contain the core data needed to fulfill the requirements.
            """
            
            response = openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert database analyst who selects relevant tables for report generation."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=500
            )
            
            # Parse response
            response_text = response.choices[0].message.content
            try:
                table_list = json.loads(response_text)
                if isinstance(table_list, list):
                    return table_list
                else:
                    logger.warning("LLM did not return a list, falling back to heuristic selection")
                    return select_table_list(schema, requirements, use_llm=False, config=config)
            except json.JSONDecodeError:
                logger.warning("Failed to parse LLM response as JSON, falling back to heuristic selection")
                return select_table_list(schema, requirements, use_llm=False, config=config)
        except Exception as e:
            logger.error(f"Error using LLM for table selection: {str(e)}")
            return select_table_list(schema, requirements, use_llm=False, config=config)

# Decision Nodes
def make_sql_decision(error: Exception, attempt: int, max_attempts: int) -> Dict:
    """Make decision about SQL generation error"""
    if attempt >= max_attempts:
        return {
            "decision": "abort",
            "message": "Maximum SQL generation attempts reached",
            "error": str(error),
            "guidance": "Please simplify your requirements or provide more specific instructions"
        }
    else:
        return {
            "decision": "retry",
            "message": f"SQL generation failed (attempt {attempt}/{max_attempts}), retrying",
            "error": str(error),
            "guidance": "Adjusting SQL generation approach..."
        }

def report_generation_decision(error: Exception, attempt: int, max_attempts: int) -> Dict:
    """Make decision about report generation error"""
    if attempt >= max_attempts:
        return {
            "decision": "abort",
            "message": "Maximum report generation attempts reached",
            "error": str(error),
            "guidance": "Please check your data source configuration and try again"
        }
    else:
        return {
            "decision": "retry",
            "message": f"Report generation failed (attempt {attempt}/{max_attempts}), retrying",
            "error": str(error),
            "guidance": "Adjusting report generation approach..."
        }

# Power BI Report Generator
class PowerBIReportGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.cache_manager = CacheManager()
        self.data_profiler = DataProfiler(config)
        self.schema_drift_detector = SchemaDriftDetector(self.cache_manager)
        self.security_manager = EnterpriseSecurityManager(config)
        self.pipeline = FabricDeploymentPipeline(config)
        self.compliance_manager = FabricComplianceManager(config)
        self.integration_hub = EnterpriseIntegrationHub(config)
        self.security_hardening = SecurityHardeningManager(config)
        self.script_sanitizer = PythonScriptSanitizer(config.get('sandbox_path', '/tmp/sandbox'))
        
    def generate_report(self, data_source_name: str, report_requirements: str) -> Dict:
        """Generate a Power BI report based on data source and requirements"""
        try:
            # Get data source
            data_source_config = self.config['data_sources'].get(data_source_name)
            if not data_source_config:
                raise ValueError(f"Data source '{data_source_name}' not found in configuration")
            
            # Create data source instance
            data_source = self._create_data_source(data_source_config)
            
            # Get data schema
            schema = data_source.get_schema()
            if not schema:
                raise ValueError("Failed to retrieve data schema")
            
            # Detect schema drift
            drift_result = self.schema_drift_detector.detect_schema_drift(data_source_name, schema)
            if drift_result['drift_detected']:
                logger.warning(f"Schema drift detected for {data_source_name}: {drift_result['message']}")
                self.integration_hub.send_teams_notification(
                    f"Schema drift detected for {data_source_name}: {len(drift_result['changes'])} changes",
                    "schema_drift"
                )
            
            # Get sample data
            sample_data = data_source.get_sample_data()
            if sample_data.empty:
                raise ValueError("Failed to retrieve sample data")
            
            # Profile data for quality issues
            if self.config.get('data_quality', {}).get('enabled', True):
                data_profile = self.data_profiler.profile_data(sample_data, data_source_name)
                
                # Check for critical data quality issues
                critical_issues = [issue for issue in data_profile['issues'] if issue.get('severity') == 'error']
                if critical_issues:
                    error_msg = f"Critical data quality issues detected in {data_source_name}: {len(critical_issues)} issues"
                    logger.error(error_msg)
                    self.integration_hub.send_teams_notification(error_msg, "data_quality_issue")
                    
                    # Continue with report generation but log the issues
                    logger.info(f"Data quality score for {data_source_name}: {data_profile['overall_score']}/100")
            
            # Select relevant tables
            selected_tables = select_table_list(schema, report_requirements, config=self.config)
            logger.info(f"Selected tables: {selected_tables}")
            
            # Generate report using AI with iterative regeneration
            report_definition = self._generate_report_definition(
                schema, sample_data, report_requirements, selected_tables
            )
            
            # Create Power BI report
            report_id = self._create_powerbi_report(report_definition)
            
            # Deploy through pipeline if configured
            if self.config['fabric'].get('pipeline_id'):
                self.pipeline.deploy_to_pipeline_stage('dev', report_id)
            
            # Send notification
            self.integration_hub.send_teams_notification(
                f"Report generated successfully for {data_source_name}",
                "report_generated"
            )
            
            return {
                "status": "success",
                "report_id": report_id,
                "report_definition": report_definition,
                "selected_tables": selected_tables,
                "data_profile": data_profile if self.config.get('data_quality', {}).get('enabled', True) else None,
                "schema_drift": drift_result
            }
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            ERROR_COUNT.labels(type='report_generation').inc()
            raise
    
    def _create_data_source(self, config: Dict) -> DataSource:
        """Create data source instance based on configuration"""
        source_type = config.get('type')
        
        if source_type == 'sql_server':
            return SQLServerDataSource(config)
        elif source_type == 'postgresql':
            return PostgreSQLDataSource(config)
        elif source_type == 'csv':
            return CSVDataSource(config)
        elif source_type == 'excel':
            return ExcelDataSource(config)
        elif source_type == 'api':
            return APIDataSource(config)
        elif source_type == 'bigquery':
            return BigQueryDataSource(config)
        elif source_type == 'salesforce':
            return SalesforceDataSource(config)
        elif source_type == 's3':
            return S3DataSource(config)
        elif source_type == 'onelake':
            return OneLakeDataSource(config)
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")
    
    def _generate_report_definition(self, schema: Dict, sample_data: pd.DataFrame, 
                                  requirements: str, selected_tables: List[str]) -> Dict:
        """Generate report definition using AI with iterative regeneration"""
        max_sql_attempts = self.config.get('max_sql_generation_try', 3)
        max_python_attempts = self.config.get('max_python_script_check', 3)
        
        # SQL generation loop
        sql_attempts = 0
        sql_valid = False
        sql_queries = []
        
        while sql_attempts < max_sql_attempts and not sql_valid:
            try:
                # Prepare prompt for AI
                prompt = f"""
                Generate SQL queries for a Power BI report based on the following:
                
                Selected Tables:
                {json.dumps({table: schema.get(table, {}) for table in selected_tables}, indent=2)}
                
                Sample Data:
                {sample_data.head(10).to_json()}
                
                Report Requirements:
                {requirements}
                
                Generate SQL queries that will extract the necessary data for the report.
                Return a JSON object with a "queries" array containing SQL query strings.
                """
                
                # Call OpenAI API
                openai_client = OpenAI(api_key=self.config['openai']['api_key'])
                
                response = openai_client.chat.completions.create(
                    model=self.config['openai']['model'],
                    messages=[
                        {"role": "system", "content": "You are an expert SQL query designer for Power BI reports."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.config['openai']['temperature'],
                    max_tokens=self.config['openai']['max_tokens']
                )
                
                # Parse AI response
                response_text = response.choices[0].message.content
                sql_data = json.loads(response_text)
                sql_queries = sql_data.get('queries', [])
                
                # Validate SQL queries
                for query in sql_queries:
                    # Simple validation - check if it's a valid SQL statement
                    if not re.match(r'^\s*(SELECT|WITH)', query, re.IGNORECASE):
                        raise ValueError(f"Invalid SQL query: {query}")
                
                sql_valid = True
            except Exception as e:
                sql_attempts += 1
                decision = make_sql_decision(e, sql_attempts, max_sql_attempts)
                
                if decision['decision'] == 'abort':
                    raise Exception(f"SQL generation failed: {decision['error']}")
                
                logger.warning(f"SQL generation attempt {sql_attempts} failed: {str(e)}")
        
        # Python script generation loop
        python_attempts = 0
        python_valid = False
        python_script = ""
        
        while python_attempts < max_python_attempts and not python_valid:
            try:
                # Prepare prompt for AI
                prompt = f"""
                Generate a Python script for data transformation for a Power BI report based on the following:
                
                SQL Queries:
                {json.dumps(sql_queries, indent=2)}
                
                Sample Data:
                {sample_data.head(10).to_json()}
                
                Report Requirements:
                {requirements}
                
                Generate a Python script that will transform the data for the report.
                The script should use pandas for data manipulation and matplotlib/plotly for visualization.
                Return a JSON object with a "script" field containing the Python script.
                """
                
                # Call OpenAI API
                openai_client = OpenAI(api_key=self.config['openai']['api_key'])
                
                response = openai_client.chat.completions.create(
                    model=self.config['openai']['model'],
                    messages=[
                        {"role": "system", "content": "You are an expert Python script designer for Power BI data transformation."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.config['openai']['temperature'],
                    max_tokens=self.config['openai']['max_tokens']
                )
                
                # Parse AI response
                response_text = response.choices[0].message.content
                python_data = json.loads(response_text)
                python_script = python_data.get('script', '')
                
                # Sanitize Python script
                sanitization_result = self.script_sanitizer.sanitize_python_script(python_script)
                if not sanitization_result['is_safe']:
                    raise ValueError(f"Python script failed sanitization: {sanitization_result['error']}")
                
                python_valid = True
            except Exception as e:
                python_attempts += 1
                decision = make_sql_decision(e, python_attempts, max_python_attempts)
                
                if decision['decision'] == 'abort':
                    raise Exception(f"Python script generation failed: {decision['error']}")
                
                logger.warning(f"Python script generation attempt {python_attempts} failed: {str(e)}")
        
        # Generate final report definition
        prompt = f"""
        Generate a Power BI report definition based on the following:
        
        Selected Tables:
        {json.dumps({table: schema.get(table, {}) for table in selected_tables}, indent=2)}
        
        SQL Queries:
        {json.dumps(sql_queries, indent=2)}
        
        Python Script:
        {python_script}
        
        Sample Data:
        {sample_data.head(10).to_json()}
        
        Report Requirements:
        {requirements}
        
        The report definition should include:
        1. Visualizations (charts, tables, etc.)
        2. Data model relationships
        3. DAX measures if needed
        4. Report layout
        """
        
        # Call OpenAI API
        openai_client = OpenAI(api_key=self.config['openai']['api_key'])
        
        response = openai_client.chat.completions.create(
            model=self.config['openai']['model'],
            messages=[
                {"role": "system", "content": "You are an expert Power BI report designer."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.config['openai']['temperature'],
            max_tokens=self.config['openai']['max_tokens']
        )
        
        # Parse AI response
        report_definition = json.loads(response.choices[0].message.content)
        
        # Add SQL queries and Python script to the report definition
        report_definition['sql_queries'] = sql_queries
        report_definition['python_script'] = python_script
        report_definition['selected_tables'] = selected_tables
        
        return report_definition
    
    def _create_powerbi_report(self, report_definition: Dict) -> str:
        """Create Power BI report from definition using PBIX/PBIT import"""
        try:
            # Get access token
            access_token = self._get_powerbi_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            
            # Create a temporary PBIT file
            with tempfile.NamedTemporaryFile(suffix='.pbit', delete=False) as temp_pbit:
                pbit_path = temp_pbit.name
            
            try:
                # Create a complete PBIT file structure
                with zipfile.ZipFile(pbit_path, 'w') as pbit_zip:
                    # Add report definition
                    pbit_zip.writestr('Report/Report.json', json.dumps(report_definition))
                    
                    # Add DataMashup (Power Query)
                    datamashup = self._create_datamashup(report_definition)
                    pbit_zip.writestr('Report/DataMashup', datamashup)
                    
                    # Add DataModelSchema
                    datamodel_schema = self._create_datamodel_schema(report_definition)
                    pbit_zip.writestr('DataModelSchema', datamodel_schema)
                    
                    # Add Metadata
                    metadata = self._create_metadata(report_definition)
                    pbit_zip.writestr('Metadata', metadata)
                    
                    # Add content types
                    pbit_zip.writestr('[Content_Types].xml', '''<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="application/json" />
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" />
  <Override PartName="/Report/Report.json" ContentType="application/json" />
  <Override PartName="/Report/DataMashup" ContentType="application/octet-stream" />
  <Override PartName="/DataModelSchema" ContentType="application/json" />
  <Override PartName="/Metadata" ContentType="application/json" />
</Types>''')
                    
                    # Add relationships
                    pbit_zip.writestr('_rels/.rels', '''<?xml version="1.0" encoding="utf-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Type="http://schemas.microsoft.com/packaging/2010/07/manifest" Target="/Report/Report.json" Id="R1234567890" />
  <Relationship Type="http://schemas.microsoft.com/packaging/2010/07/datamashup" Target="/Report/DataMashup" Id="R1234567891" />
  <Relationship Type="http://schemas.microsoft.com/packaging/2010/07/datamodelschema" Target="/DataModelSchema" Id="R1234567892" />
  <Relationship Type="http://schemas.microsoft.com/packaging/2010/07/metadata" Target="/Metadata" Id="R1234567893" />
</Relationships>''')
                    
                    # Add report relationships
                    pbit_zip.writestr('Report/_rels/.rels', '''<?xml version="1.0" encoding="utf-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Type="http://schemas.microsoft.com/packaging/2010/07/datamashup" Target="/DataMashup" Id="R1234567894" />
</Relationships>''')
                
                # Create report in Power BI
                workspace_id = self.config['fabric']['workspace_id']
                report_name = report_definition.get('title', 'AI Generated Report')
                
                # Don't manually set Content-Type for multipart/form-data
                with open(pbit_path, 'rb') as pbit_file:
                    response = requests.post(
                        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/imports?datasetDisplayName={report_name}",
                        headers=headers,
                        files={'file': (os.path.basename(pbit_path), pbit_file)}
                    )
                
                if response.status_code == 202:
                    import_id = response.json().get('id')
                    
                    # Wait for import to complete
                    while True:
                        status_response = requests.get(
                            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/imports/{import_id}",
                            headers=headers
                        )
                        
                        status = status_response.json().get('importState')
                        if status == 'Succeeded':
                            report_id = status_response.json().get('reports')[0].get('id')
                            return report_id
                        elif status == 'Failed':
                            raise Exception("Report import failed")
                        
                        time.sleep(2)
                else:
                    raise Exception(f"Failed to create report: {response.text}")
            finally:
                # Clean up temporary file
                if os.path.exists(pbit_path):
                    os.unlink(pbit_path)
        except Exception as e:
            logger.error(f"Error creating Power BI report: {str(e)}")
            raise
    
    def _create_datamashup(self, report_definition: Dict) -> bytes:
        """Create DataMashup (Power Query) content"""
        # This is a simplified version of the DataMashup file
        # In a real implementation, this would be more complex
        datamashup = {
            "version": "1.0.0",
            "queries": [
                {
                    "name": "Query1",
                    "query": "let\n    Source = #table({\"Column1\"}, {{\"Value1\"}})\nin\n    Source"
                }
            ]
        }
        
        # Convert to bytes (DataMashup is a binary format)
        return json.dumps(datamashup).encode('utf-8')
    
    def _create_datamodel_schema(self, report_definition: Dict) -> str:
        """Create DataModelSchema content"""
        # This is a simplified version of the DataModelSchema
        # In a real implementation, this would be more complex
        datamodel_schema = {
            "model": {
                "tables": [
                    {
                        "name": "Table1",
                        "columns": [
                            {
                                "name": "Column1",
                                "dataType": "string"
                            }
                        ]
                    }
                ],
                "relationships": []
            }
        }
        
        return json.dumps(datamodel_schema)
    
    def _create_metadata(self, report_definition: Dict) -> str:
        """Create Metadata content"""
        # This is a simplified version of the Metadata
        # In a real implementation, this would be more complex
        metadata = {
            "version": "1.0.0",
            "created": datetime.now().isoformat(),
            "modified": datetime.now().isoformat(),
            "author": "Power BI AI Generator",
            "title": report_definition.get('title', 'AI Generated Report')
        }
        
        return json.dumps(metadata)
    
    def _get_powerbi_access_token(self) -> str:
        """Get Power BI access token"""
        app = ConfidentialClientApplication(
            client_id=self.config['fabric']['client_id'],
            authority=f"https://login.microsoftonline.com/{self.config['fabric']['tenant_id']}",
            client_credential=self.config['fabric']['client_secret']
        )
        
        result = app.acquire_token_for_client(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )
        
        if "access_token" in result:
            return result["access_token"]
        else:
            raise Exception(f"Token acquisition failed: {result.get('error_description')}")

# Query Execution Service
class QueryExecutionService:
    def __init__(self, config: Dict):
        self.config = config
        
    def execute_query(self, query: str, data_source_name: str) -> pd.DataFrame:
        """Execute a query against a data source"""
        try:
            # Get data source
            data_source_config = self.config['data_sources'].get(data_source_name)
            if not data_source_config:
                raise ValueError(f"Data source '{data_source_name}' not found in configuration")
            
            # Create data source instance
            data_source = self._create_data_source(data_source_config)
            
            # Call connect() before executing query
            if not data_source.connect():
                raise ValueError(f"Failed to connect to data source '{data_source_name}'")
            
            # Execute query (simplified for example)
            if isinstance(data_source, (SQLServerDataSource, PostgreSQLDataSource)):
                return pd.read_sql(query, data_source.connection)
            elif isinstance(data_source, BigQueryDataSource):
                return data_source.connection.query(query).to_dataframe()
            else:
                raise ValueError("Query execution not supported for this data source type")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _create_data_source(self, config: Dict) -> DataSource:
        """Create data source instance based on configuration"""
        source_type = config.get('type')
        
        if source_type == 'sql_server':
            return SQLServerDataSource(config)
        elif source_type == 'postgresql':
            return PostgreSQLDataSource(config)
        elif source_type == 'bigquery':
            return BigQueryDataSource(config)
        else:
            raise ValueError(f"Query execution not supported for data source type: {source_type}")
    
    def execute_queries_batch(self, queries: List[Dict]) -> List[Dict]:
        """Execute multiple queries and return results"""
        results = []
        
        for query_info in queries:
            try:
                query = query_info.get('query')
                data_source = query_info.get('data_source')
                
                result_df = self.execute_query(query, data_source)
                
                # Parse ExecuteQueries results by column names
                result = {
                    "query_id": query_info.get('id'),
                    "status": "success",
                    "data": result_df.to_dict('records'),
                    "columns": list(result_df.columns)
                }
                
                results.append(result)
            except Exception as e:
                results.append({
                    "query_id": query_info.get('id'),
                    "status": "error",
                    "error": str(e)
                })
        
        return results

# Streamlit App
def streamlit_app():
    """Streamlit application for Power BI report generation"""
    st.set_page_config(
        page_title="Power BI AI Generator",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("Power BI AI Generator")
    st.markdown("Generate Power BI reports using AI")
    
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        st.error(f"Failed to load configuration: {str(e)}")
        return
    
    # Initialize components
    report_generator = PowerBIReportGenerator(config)
    query_service = QueryExecutionService(config)
    
    # Sidebar
    st.sidebar.title("Configuration")
    
    # Data source selection
    data_sources = list(config['data_sources'].keys())
    selected_data_source = st.sidebar.selectbox("Select Data Source", data_sources)
    
    # Report requirements
    report_requirements = st.text_area(
        "Report Requirements",
        "Create a sales dashboard with monthly revenue trends and top products"
    )
    
    # Generate report button
    if st.button("Generate Report"):
        with st.spinner("Generating report..."):
            try:
                # Generate report
                report_result = report_generator.generate_report(
                    data_source_name=selected_data_source,
                    report_requirements=report_requirements
                )
                
                # Display success message
                st.success(f"Report generated successfully with ID: {report_result['report_id']}")
                
                # Display data quality profile if available
                if report_result.get('data_profile'):
                    st.subheader("Data Quality Profile")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Data Quality Score", f"{report_result['data_profile']['overall_score']}/100")
                    with col2:
                        st.metric("Issues Found", len(report_result['data_profile']['issues']))
                    
                    # Display issues
                    if report_result['data_profile']['issues']:
                        st.subheader("Data Quality Issues")
                        for issue in report_result['data_profile']['issues']:
                            severity_color = "red" if issue.get('severity') == 'error' else "orange"
                            st.markdown(f"<span style='color: {severity_color}'>**{issue.get('severity', 'warning').upper()}**: {issue.get('message')}</span>", unsafe_allow_html=True)
                
                # Display schema drift if available
                if report_result.get('schema_drift', {}).get('drift_detected'):
                    st.subheader("Schema Drift Detected")
                    st.warning(f"{report_result['schema_drift']['message']}")
                    
                    # Display changes
                    for change in report_result['schema_drift']['changes']:
                        change_type = change.get('type')
                        severity = change.get('severity', 'info')
                        
                        if change_type == 'table_added':
                            st.info(f"Table added: {change.get('table')}")
                        elif change_type == 'table_removed':
                            st.error(f"Table removed: {change.get('table')}")
                        elif change_type == 'column_added':
                            st.info(f"Column added: {change.get('table')}.{change.get('column')}")
                        elif change_type == 'column_removed':
                            st.error(f"Column removed: {change.get('table')}.{change.get('column')}")
                        elif change_type == 'column_type_changed':
                            st.warning(f"Column type changed: {change.get('table')}.{change.get('column')} from {change.get('old_type')} to {change.get('new_type')}")
                
                # Display selected tables
                st.subheader("Selected Tables")
                st.json(report_result['selected_tables'])
                
                # Display SQL queries
                if 'sql_queries' in report_result['report_definition']:
                    st.subheader("SQL Queries")
                    for i, query in enumerate(report_result['report_definition']['sql_queries']):
                        st.code(query, language="sql")
                
                # Display Python script
                if 'python_script' in report_result['report_definition']:
                    st.subheader("Python Script")
                    st.code(report_result['report_definition']['python_script'], language="python")
                
            except Exception as e:
                st.error(f"Failed to generate report: {str(e)}")
    
    # Query execution section
    st.header("Query Execution")
    
    # Query input
    query = st.text_area("SQL Query", "SELECT * FROM sales_data LIMIT 10")
    
    # Execute query button
    if st.button("Execute Query"):
        with st.spinner("Executing query..."):
            try:
                # Execute query
                result_df = query_service.execute_query(query, selected_data_source)
                
                # Display results
                st.subheader("Query Results")
                st.dataframe(result_df)
                
            except Exception as e:
                st.error(f"Failed to execute query: {str(e)}")

# Main Application
if __name__ == "__main__":
    # Start metrics server
    start_metrics_server(port=8000)
    
    try:
        # Load configuration
        config = load_config()
        
        # Check if running in Streamlit
        if 'streamlit' in sys.modules:
            streamlit_app()
        else:
            # Initialize components
            report_generator = PowerBIReportGenerator(config)
            query_service = QueryExecutionService(config)
            
            # Example: Generate a report
            report_result = report_generator.generate_report(
                data_source_name="sales_data",
                report_requirements="Create a sales dashboard with monthly revenue trends and top products"
            )
            
            logger.info(f"Report generated with ID: {report_result['report_id']}")
            
            # Example: Execute a query
            query_result = query_service.execute_query(
                query="SELECT * FROM sales_data LIMIT 10",
                data_source_name="sales_data"
            )
            
            logger.info(f"Query executed successfully, returned {len(query_result)} rows")
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        ERROR_COUNT.labels(type='application_error').inc()