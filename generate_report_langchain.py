import os
import re
import json
import time
import gc
import base64
import logging
import tempfile
import zipfile
from datetime import datetime, timedelta
from functools import wraps, lru_cache
from typing import Dict, List, Optional, Any, Union, Tuple
# Standard library imports
import ast
import secrets
import hashlib
# Third-party imports
import pandas as pd
import numpy as np
import requests
import pyodbc
import yaml
import jwt
from scipy import stats
# Crypto imports
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Protocol.KDF import PBKDF2
# Azure imports
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.keyvault.secrets import SecretClient
from azure.synapse.artifacts import ArtifactsClient
from msal import ConfidentialClientApplication
# Microsoft Fabric imports
from microsoft.fabric.core import FabricClient
from microsoft.fabric.items import ItemClient
from microsoft.fabric.workspaces import WorkspaceClient
# Google Cloud imports
from google.cloud import bigquery
from google.oauth2 import service_account
# Salesforce imports
from simple_salesforce import Salesforce
# Monitoring and caching imports
import redis
import tenacity
from prometheus_client import Counter, Gauge, Histogram, start_http_server
# OpenAI imports
import openai
from openai import OpenAI
# LangChain imports (updated to latest versions)
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain.chains import LLMChain, SequentialChain, TransformChain
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.callbacks import CallbackManager
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.tools import tool
from langchain.memory import ConversationBufferMemory
# SQL parsing
import sqlparse
# Pydantic imports
from pydantic import BaseModel, field_validator
# Power BI Template handling
from powerbi_template import PowerBITemplate  # This would be a custom module

# Environment variable support
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('powerbi_generator.log')
    ]
)
logger = logging.getLogger(__name__)

# === Prometheus Metrics ===
REQUEST_COUNT = Counter('powerbi_requests_total', 'Total Power BI requests', ['endpoint', 'status'])
REQUEST_DURATION = Histogram('powerbi_request_duration_seconds', 'Power BI request duration', ['endpoint'])
ACTIVE_CONNECTIONS = Gauge('powerbi_active_connections', 'Active Power BI connections')
ERROR_COUNT = Counter('powerbi_errors_total', 'Total Power BI errors', ['type'])
DATA_QUALITY_SCORE = Gauge('powerbi_data_quality_score', 'Data quality score', ['data_source'])
SCHEMA_DRIFT_DETECTED = Counter('powerbi_schema_drift_detected', 'Schema drift detected', ['data_source'])
FABRIC_DEPLOYMENT_COUNT = Counter('fabric_deployment_total', 'Total Fabric deployments', ['status'])

def start_metrics_server(port: int = 8000) -> None:
    """Start Prometheus metrics server."""
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start Prometheus metrics server: {str(e)}")

# === Configuration Models with Pydantic ===
class AIConfig(BaseModel):
    api_key: str
    model: str = "gpt-4"
    temperature: float = 0.3
    max_tokens: int = 2000
    max_retries: int = 3
    azure_endpoint: Optional[str] = None
    azure_version: Optional[str] = None
    use_azure: bool = False

class FabricConfig(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    pipeline_id: Optional[str] = None
    capacity_id: Optional[str] = None
    api_endpoint: str = "https://api.fabric.microsoft.com/v1"

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
    connection_string: Optional[str] = None

    @field_validator('type')
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        allowed_types = [
            'sql_server', 'postgresql', 'mysql', 'csv', 'excel', 'api', 
            'bigquery', 'salesforce', 's3', 'redshift', 'onelake', 'semantic_model',
            'oracle', 'snowflake', 'mongodb'
        ]
        if v not in allowed_types:
            raise ValueError(f'Invalid source_type: {v}. Allowed types: {", ".join(allowed_types)}')
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
    custom_rules: List[Dict[str, Any]] = []

class SecurityConfig(BaseModel):
    conditional_access: bool = True
    mfa_required: bool = True
    device_compliance_required: bool = True
    ip_restrictions: List[str] = []
    encryption_key: Optional[str] = None

class MonitoringConfig(BaseModel):
    application_insights: str = ""
    log_analytics_workspace: str = ""
    prometheus_endpoint: str = "http://prometheus:9090"
    redis_url: str = "redis://localhost:6379"

class VisualizationConfig(BaseModel):
    theme: str = "standard"
    color_palette: List[str] = ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"]
    font_family: str = "Segoe UI"
    default_visualization_types: List[str] = ["column", "line", "pie", "card", "table"]

class MainConfig(BaseModel):
    openai: AIConfig
    fabric: FabricConfig
    data_sources: Dict[str, DataSourceConfig]
    data_quality: DataQualityConfig = DataQualityConfig()
    security: SecurityConfig = SecurityConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    visualization: VisualizationConfig = VisualizationConfig()
    max_workers: int = 4
    data_chunk_size: int = 10000
    max_sql_generation_try: int = 3
    max_python_script_check: int = 3
    sandbox_path: str = "/tmp/sandbox"
    enable_fabric_deployment: bool = True

def create_default_config() -> Dict[str, Any]:
    """Create a default configuration when config file is missing."""
    return {
        "openai": {
            "api_key": os.getenv("OPENAI_API_KEY", ""),
            "model": "gpt-4",
            "temperature": 0.3,
            "max_tokens": 2000,
            "max_retries": 3,
            "use_azure": False
        },
        "fabric": {
            "tenant_id": os.getenv("FABRIC_TENANT_ID", ""),
            "client_id": os.getenv("FABRIC_CLIENT_ID", ""),
            "client_secret": os.getenv("FABRIC_CLIENT_SECRET", ""),
            "workspace_id": os.getenv("FABRIC_WORKSPACE_ID", ""),
            "api_endpoint": "https://api.fabric.microsoft.com/v1"
        },
        "data_sources": {},
        "monitoring": {
            "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379")
        },
        "enable_fabric_deployment": True
    }

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load and validate configuration with environment variable overrides."""
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
        else:
            logger.warning(f"Configuration file not found: {config_path}, using defaults")
            config_data = create_default_config()
        
        # Override with environment variables
        if 'OPENAI_API_KEY' in os.environ:
            config_data.setdefault('openai', {})['api_key'] = os.environ['OPENAI_API_KEY']
        
        if 'REDIS_URL' in os.environ:
            config_data.setdefault('monitoring', {})['redis_url'] = os.environ['REDIS_URL']
        
        validated_config = MainConfig(**config_data)
        logger.info("Configuration loaded and validated successfully.")
        return validated_config.model_dump()
        
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Configuration validation error: {str(e)}")
        raise

# === Security Functions ===
def sanitize_log_data(data: str) -> str:
    """Remove sensitive information from log data."""
    # Remove potential API keys, passwords, tokens
    patterns = [
        r'(api[_-]?key["\s]*[:=]["\s]*)[^"\s]+',
        r'(password["\s]*[:=]["\s]*)[^"\s]+',
        r'(token["\s]*[:=]["\s]*)[^"\s]+',
        r'(secret["\s]*[:=]["\s]*)[^"\s]+',
    ]
    
    sanitized = data
    for pattern in patterns:
        sanitized = re.sub(pattern, r'\1***REDACTED***', sanitized, flags=re.IGNORECASE)
    
    return sanitized

def encrypt_sensitive_data(data: str, key: str) -> str:
    """Encrypt sensitive data using AES."""
    salt = secrets.token_bytes(16)
    kdf = PBKDF2(key, salt, dkLen=32, count=200000)  # Increased iterations
    cipher = AES.new(kdf, AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data.encode(), AES.block_size))
    return base64.b64encode(salt + cipher.iv + ct_bytes).decode('utf-8')

def decrypt_sensitive_data(encrypted_data: str, key: str) -> str:
    """Decrypt sensitive data using AES."""
    try:
        data = base64.b64decode(encrypted_data)
        salt, iv, ct = data[:16], data[16:32], data[32:]
        kdf = PBKDF2(key, salt, dkLen=32, count=200000)
        cipher = AES.new(kdf, AES.MODE_CBC, iv=iv)
        pt = unpad(cipher.decrypt(ct), AES.block_size)
        return pt.decode('utf-8')
    except Exception as e:
        logger.error(f"Decryption failed: {sanitize_log_data(str(e))}")
        raise ValueError("Failed to decrypt data")

def generate_jwt_token(payload: Dict[str, Any], secret_key: str, expires_in: int = 3600) -> str:
    """Generate JWT token with expiration."""
    payload['exp'] = datetime.utcnow() + timedelta(seconds=expires_in)
    payload['iat'] = datetime.utcnow()
    return jwt.encode(payload, secret_key, algorithm='HS256')

def verify_jwt_token(token: str, secret_key: str) -> Dict[str, Any]:
    """Verify JWT token and return payload."""
    try:
        return jwt.decode(token, secret_key, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidTokenError:
        raise ValueError("Invalid token")

# === Retry Decorator ===
def retry_with_backoff(retries: int = 3, backoff_factor: float = 1.0):
    """Retry decorator with exponential backoff."""
    def decorator(func):
        @wraps(func)
        @tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=backoff_factor, min=1, max=10),
            stop=tenacity.stop_after_attempt(retries),
            retry=tenacity.retry_if_exception_type((
                requests.exceptions.RequestException,
                openai.RateLimitError,
                openai.APIConnectionError,
                openai.APITimeoutError
            ))
        )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

# === Cache Manager ===
class CacheManager:
    """Redis-backed cache manager with in-memory fallback."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_client = None
        self.in_memory_cache: Dict[str, Any] = {}
        
        try:
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info("Redis cache connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed: {sanitize_log_data(str(e))}. Using in-memory cache.")
            self.redis_client = None
    
    def _generate_key(self, prefix: str, key: str) -> str:
        """Generate cache key with prefix."""
        return f"{prefix}:{hashlib.md5(key.encode()).hexdigest()}"
    
    def get_cached_schema(self, data_source_name: str) -> Optional[Dict[str, Any]]:
        """Get cached schema for data source."""
        cache_key = self._generate_key("schema", data_source_name)
        
        if self.redis_client:
            try:
                cached = self.redis_client.get(cache_key)
                return json.loads(cached) if cached else None
            except Exception as e:
                logger.error(f"Redis get error: {str(e)}")
                return self.in_memory_cache.get(cache_key)
        else:
            return self.in_memory_cache.get(cache_key)
    
    def set_cached_schema(self, data_source_name: str, schema: Dict[str, Any], expire: int = 3600) -> None:
        """Cache schema for data source."""
        cache_key = self._generate_key("schema", data_source_name)
        
        if self.redis_client:
            try:
                self.redis_client.setex(cache_key, expire, json.dumps(schema))
            except Exception as e:
                logger.error(f"Redis set error: {str(e)}")
                self.in_memory_cache[cache_key] = schema
        else:
            self.in_memory_cache[cache_key] = schema
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        if self.redis_client:
            try:
                self.redis_client.flushall()
            except Exception as e:
                logger.error(f"Redis clear error: {str(e)}")
        
        self.in_memory_cache.clear()

# === Data Quality Profiler ===
class DataProfiler:
    """Comprehensive data quality profiler."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_quality_config = config.get('data_quality', {})
    
    def profile_data(self, data: pd.DataFrame, data_source_name: str) -> Dict[str, Any]:
        """Generate comprehensive data profile."""
        if data.empty:
            return self._empty_profile(data_source_name)
        
        profile = {
            "data_source": data_source_name,
            "timestamp": datetime.now().isoformat(),
            "row_count": len(data),
            "column_count": len(data.columns),
            "columns": {},
            "overall_score": 0,
            "issues": [],
            "recommendations": [],
            "memory_usage": data.memory_usage(deep=True).sum()
        }
        
        # Profile each column
        for col in data.columns:
            try:
                col_profile = self._profile_column(data[col], col)
                profile["columns"][col] = col_profile
            except Exception as e:
                logger.warning(f"Error profiling column {col}: {str(e)}")
                profile["columns"][col] = {"error": str(e)}
        
        # Apply quality checks
        profile["issues"] = self._apply_quality_checks(data, profile)
        profile["overall_score"] = self._calculate_overall_score(profile)
        
        # Update metrics
        DATA_QUALITY_SCORE.labels(data_source=data_source_name).set(profile["overall_score"])
        
        return profile
    
    def _empty_profile(self, data_source_name: str) -> Dict[str, Any]:
        """Generate profile for empty dataset."""
        return {
            "data_source": data_source_name,
            "timestamp": datetime.now().isoformat(),
            "row_count": 0,
            "column_count": 0,
            "columns": {},
            "overall_score": 0,
            "issues": [{"type": "empty_dataset", "severity": "error", "message": "Dataset is empty"}],
            "recommendations": ["Check data source connection and query"],
            "memory_usage": 0
        }
    
    def _profile_column(self, series: pd.Series, col_name: str) -> Dict[str, Any]:
        """Profile individual column."""
        col_profile = {
            "name": col_name,
            "data_type": str(series.dtype),
            "null_count": int(series.isnull().sum()),
            "null_percentage": round(series.isnull().sum() / len(series) * 100, 2) if len(series) > 0 else 0,
            "unique_count": int(series.nunique()),
        }
        
        # Calculate duplicates
        col_profile["duplicate_count"] = len(series) - col_profile["unique_count"]
        col_profile["duplicate_percentage"] = round(col_profile["duplicate_count"] / len(series) * 100, 2) if len(series) > 0 else 0
        
        # Type-specific profiling
        if pd.api.types.is_numeric_dtype(series):
            col_profile.update(self._profile_numeric_column(series))
        elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            col_profile.update(self._profile_text_column(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            col_profile.update(self._profile_datetime_column(series))
        
        return col_profile
    
    def _profile_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile numeric column."""
        profile = {}
        try:
            clean_series = series.dropna()
            if not clean_series.empty:
                profile.update({
                    "min": float(clean_series.min()),
                    "max": float(clean_series.max()),
                    "mean": float(clean_series.mean()),
                    "median": float(clean_series.median()),
                    "std": float(clean_series.std()),
                    "skewness": float(stats.skew(clean_series)),
                    "kurtosis": float(stats.kurtosis(clean_series))
                })
                
                if self.data_quality_config.get('check_outliers', True):
                    profile["outliers"] = self._detect_outliers(clean_series)
        except Exception as e:
            logger.warning(f"Error profiling numeric column: {str(e)}")
            profile["error"] = str(e)
            
        return profile
    
    def _profile_text_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile text column."""
        profile = {}
        try:
            clean_series = series.dropna()
            if not clean_series.empty:
                value_counts = clean_series.value_counts()
                profile.update({
                    "top_values": value_counts.head(10).to_dict(),
                    "value_distribution": "uniform" if value_counts.std() < 1 else "skewed",
                    "avg_length": float(clean_series.astype(str).str.len().mean()),
                    "max_length": int(clean_series.astype(str).str.len().max()),
                    "min_length": int(clean_series.astype(str).str.len().min())
                })
        except Exception as e:
            logger.warning(f"Error profiling text column: {str(e)}")
            profile["error"] = str(e)
            
        return profile
    
    def _profile_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile datetime column."""
        profile = {}
        try:
            clean_series = series.dropna()
            if not clean_series.empty:
                profile.update({
                    "min_date": clean_series.min().isoformat(),
                    "max_date": clean_series.max().isoformat(),
                    "date_range_days": int((clean_series.max() - clean_series.min()).days)
                })
        except Exception as e:
            logger.warning(f"Error profiling datetime column: {str(e)}")
            profile["error"] = str(e)
            
        return profile
    
    def _detect_outliers(self, series: pd.Series) -> List[int]:
        """Detect outliers using specified method."""
        method = self.data_quality_config.get('outlier_method', 'zscore')
        threshold = self.data_quality_config.get('outlier_threshold', 3.0)
        
        try:
            if method == 'zscore' and series.std() != 0:
                z_scores = np.abs(stats.zscore(series))
                return series[z_scores > threshold].index.tolist()
            elif method == 'iqr':
                Q1 = series.quantile(0.25)
                Q3 = series.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                return series[(series < lower_bound) | (series > upper_bound)].index.tolist()
        except Exception as e:
            logger.warning(f"Error detecting outliers: {str(e)}")
            
        return []
    
    def _apply_quality_checks(self, data: pd.DataFrame, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply data quality checks."""
        issues = []
        max_null = self.data_quality_config.get('max_null_percentage', 30.0)
        max_dup = self.data_quality_config.get('max_duplicate_percentage', 10.0)
        
        for col_name, col_profile in profile["columns"].items():
            if isinstance(col_profile, dict) and "error" not in col_profile:
                # Check null percentage
                null_pct = col_profile.get("null_percentage", 0)
                if null_pct > max_null:
                    issues.append({
                        "type": "high_null_percentage",
                        "severity": "error" if null_pct > 50 else "warning",
                        "column": col_name,
                        "message": f"Column '{col_name}' has {null_pct}% null values (threshold: {max_null}%)"
                    })
                
                # Check duplicate percentage
                dup_pct = col_profile.get("duplicate_percentage", 0)
                if dup_pct > max_dup:
                    issues.append({
                        "type": "high_duplicate_percentage", 
                        "severity": "warning",
                        "column": col_name,
                        "message": f"Column '{col_name}' has {dup_pct}% duplicate values (threshold: {max_dup}%)"
                    })
        
        return issues
    
    def _calculate_overall_score(self, profile: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        score = 100.0
        
        # Deduct for issues
        for issue in profile["issues"]:
            if issue.get("severity") == "error":
                score -= 20
            else:
                score -= 5
        
        # Deduct for high null/duplicate percentages
        for col_profile in profile["columns"].values():
            if isinstance(col_profile, dict) and "error" not in col_profile:
                null_pct = col_profile.get("null_percentage", 0)
                if null_pct > 50:
                    score -= 15
                elif null_pct > 20:
                    score -= 5
                
                dup_pct = col_profile.get("duplicate_percentage", 0)
                if dup_pct > 50:
                    score -= 10
                elif dup_pct > 20:
                    score -= 3
        
        return max(0, min(100, round(score, 2)))

# === Schema Drift Detection ===
class SchemaDriftDetector:
    """Detect schema changes over time."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
    
    def detect_schema_drift(self, data_source_name: str, current_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Detect schema drift by comparing with cached schema."""
        current_schema_hash = self._hash_schema(current_schema)
        cached_schema = self.cache_manager.get_cached_schema(data_source_name)
        
        if cached_schema is None:
            self.cache_manager.set_cached_schema(data_source_name, current_schema)
            return {
                "drift_detected": False,
                "message": "First time analyzing this data source",
                "changes": []
            }
        
        cached_schema_hash = self._hash_schema(cached_schema)
        if current_schema_hash == cached_schema_hash:
            return {
                "drift_detected": False,
                "message": "No schema drift detected",
                "changes": []
            }
        
        changes = self._compare_schemas(cached_schema, current_schema)
        self.cache_manager.set_cached_schema(data_source_name, current_schema)
        
        if changes:
            SCHEMA_DRIFT_DETECTED.labels(data_source=data_source_name).inc()
        
        return {
            "drift_detected": len(changes) > 0,
            "message": f"Schema drift detected with {len(changes)} changes",
            "changes": changes
        }
    
    def _hash_schema(self, schema: Dict[str, Any]) -> str:
        """Generate hash for schema comparison."""
        schema_json = json.dumps(schema, sort_keys=True, default=str)
        return hashlib.md5(schema_json.encode()).hexdigest()
    
    def _compare_schemas(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compare two schemas and return list of changes."""
        changes = []
        old_tables = set(old_schema.keys())
        new_tables = set(new_schema.keys())
        
        # Check for added/removed tables
        for table in new_tables - old_tables:
            changes.append({
                "type": "table_added",
                "table": table,
                "severity": "info",
                "message": f"Table '{table}' was added"
            })
        
        for table in old_tables - new_tables:
            changes.append({
                "type": "table_removed",
                "table": table,
                "severity": "warning",
                "message": f"Table '{table}' was removed"
            })
        
        # Check for column changes in existing tables
        for table in old_tables & new_tables:
            old_columns = {col["name"]: col for col in old_schema[table].get("columns", [])}
            new_columns = {col["name"]: col for col in new_schema[table].get("columns", [])}
            
            old_col_names = set(old_columns.keys())
            new_col_names = set(new_columns.keys())
            
            # Check for added/removed columns
            for col in new_col_names - old_col_names:
                changes.append({
                    "type": "column_added",
                    "table": table,
                    "column": col,
                    "severity": "info",
                    "message": f"Column '{col}' was added to table '{table}'"
                })
            
            for col in old_col_names - new_col_names:
                changes.append({
                    "type": "column_removed",
                    "table": table,
                    "column": col,
                    "severity": "warning",
                    "message": f"Column '{col}' was removed from table '{table}'"
                })
            
            # Check for type changes in existing columns
            for col in old_col_names & new_col_names:
                if old_columns[col].get("type") != new_columns[col].get("type"):
                    changes.append({
                        "type": "column_type_changed",
                        "table": table,
                        "column": col,
                        "old_type": old_columns[col].get("type"),
                        "new_type": new_columns[col].get("type"),
                        "severity": "warning",
                        "message": f"Column '{col}' type changed from '{old_columns[col].get('type')}' to '{new_columns[col].get('type')}'"
                    })
        
        return changes

# === Python Script Sanitizer ===
class PythonScriptSanitizer:
    """Sanitize Python scripts for security."""
    
    def __init__(self, sandbox_path: str = "/tmp/sandbox"):
        self.sandbox_path = sandbox_path
        self.disallowed_imports = {
            'os', 'subprocess', 'sys', 'shutil', 'glob', 'tempfile', 'pathlib',
            'requests', 'urllib', 'http', 'socket', 'ftplib', 'smtplib', 
            'poplib', 'imaplib', 'nntplib', 'telnetlib', 'uuid', 'platform', 
            'ctypes', 'pickle', 'shelve', 'dbm', 'sqlite3', 'pymongo', 
            'redis', 'psycopg2', 'mysql', 'cx_Oracle', 'pyodbc', 'sqlalchemy'
        }
        self.disallowed_functions = {
            'open', 'file', 'input', 'raw_input', 'exec', 'eval', 'compile',
            '__import__', 'reload', 'exit', 'quit', 'execfile'
        }
    
    def sanitize_python_script(self, script: str) -> Dict[str, Any]:
        """Sanitize Python script and return safety assessment."""
        try:
            tree = ast.parse(script)
            
            for node in ast.walk(tree):
                # Check imports
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in self.disallowed_imports:
                            return {
                                "is_safe": False,
                                "error": f"Disallowed import: {alias.name}",
                                "suggestion": f"Replace {alias.name} with a safer alternative"
                            }
                
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module in self.disallowed_imports:
                        return {
                            "is_safe": False,
                            "error": f"Disallowed import from: {node.module}",
                            "suggestion": f"Replace {node.module} with a safer alternative"
                        }
                
                # Check function calls
                elif isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name) and node.func.id in self.disallowed_functions:
                        return {
                            "is_safe": False,
                            "error": f"Disallowed function call: {node.func.id}",
                            "suggestion": f"Replace {node.func.id} with a safer alternative"
                        }
                    
                    # Check for network and subprocess calls
                    if isinstance(node.func, ast.Attribute):
                        if isinstance(node.func.value, ast.Name):
                            if node.func.value.id in ['requests', 'urllib', 'http', 'socket']:
                                return {
                                    "is_safe": False,
                                    "error": f"Network call detected: {node.func.value.id}.{node.func.attr}",
                                    "suggestion": "Avoid network calls in generated scripts"
                                }
                            
                            if node.func.value.id == 'subprocess':
                                return {
                                    "is_safe": False,
                                    "error": f"Subprocess call detected: {node.func.attr}",
                                    "suggestion": "Avoid subprocess calls in generated scripts"
                                }
                
                # Check file access (sandbox enforcement)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'open':
                    for arg in node.args:
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                            if not arg.value.startswith(self.sandbox_path):
                                return {
                                    "is_safe": False,
                                    "error": f"File access outside sandbox: {arg.value}",
                                    "suggestion": f"Use files within sandbox: {self.sandbox_path}"
                                }
            
            return {"is_safe": True}
            
        except SyntaxError as e:
            return {
                "is_safe": False,
                "error": f"Syntax error: {str(e)}",
                "suggestion": "Fix syntax errors in the script"
            }
        except Exception as e:
            return {
                "is_safe": False,
                "error": f"Error sanitizing script: {str(e)}",
                "suggestion": "Review the script for potential issues"
            }

# === SQL Query Validator ===
class SQLQueryValidator:
    """Validate SQL queries for safety and correctness."""
    
    def __init__(self):
        self.dangerous_keywords = {
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE',
            'EXEC', 'EXECUTE', 'xp_', 'sp_', '--', '/*', '*/'
        }
    
    def validate_sql_queries(self, queries: List[str]) -> Dict[str, Any]:
        """Validate list of SQL queries."""
        if not queries:
            return {"is_valid": False, "error": "No queries provided"}
        
        validation_results = []
        
        for i, query in enumerate(queries):
            result = self.validate_single_query(query, i)
            validation_results.append(result)
            
            if not result["is_valid"]:
                return {
                    "is_valid": False,
                    "error": f"Query {i+1} failed validation: {result['error']}",
                    "query_index": i,
                    "query": query[:100] + "..." if len(query) > 100 else query
                }
        
        return {
            "is_valid": True,
            "message": f"All {len(queries)} queries passed validation",
            "query_count": len(queries)
        }
    
    def validate_single_query(self, query: str, index: int = 0) -> Dict[str, Any]:
        """Validate a single SQL query."""
        try:
            # Basic checks
            if not query or not query.strip():
                return {"is_valid": False, "error": "Empty query"}
            
            query_upper = query.upper().strip()
            
            # Check for dangerous keywords
            for keyword in self.dangerous_keywords:
                if keyword in query_upper:
                    return {
                        "is_valid": False,
                        "error": f"Dangerous keyword detected: {keyword}"
                    }
            
            # Must start with SELECT or WITH
            if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
                return {
                    "is_valid": False,
                    "error": "Query must start with SELECT or WITH"
                }
            
            # Try to parse with sqlparse
            parsed = sqlparse.parse(query)
            if not parsed:
                return {"is_valid": False, "error": "Failed to parse SQL query"}
            
            # Basic structure validation
            if len(parsed) != 1:
                return {"is_valid": False, "error": "Query contains multiple statements"}
            
            return {"is_valid": True, "message": "Query validation passed"}
            
        except Exception as e:
            return {"is_valid": False, "error": f"Validation error: {str(e)}"}

# === Table Selection Logic ===
def select_table_list(schema: Dict[str, Any], requirements: str, use_llm: bool = True, config: Optional[Dict[str, Any]] = None) -> List[str]:
    """Select most relevant tables for report generation."""
    
    if not schema:
        logger.warning("Empty schema provided for table selection")
        return []
    
    if not use_llm or not config:
        return _heuristic_table_selection(schema, requirements)
    
    try:
        return _llm_table_selection(schema, requirements, config)
    except Exception as e:
        logger.error(f"LLM table selection failed: {str(e)}")
        return _heuristic_table_selection(schema, requirements)

def _heuristic_table_selection(schema: Dict[str, Any], requirements: str) -> List[str]:
    """Heuristic-based table selection."""
    all_tables = list(schema.keys())
    keywords = re.findall(r'\w+', requirements.lower())
    
    table_scores = {}
    for table in all_tables:
        score = 0
        table_lower = table.lower()
        
        # Score based on keyword matches
        for keyword in keywords:
            if keyword in table_lower:
                score += 2
            # Check column names too
            for col in schema[table].get("columns", []):
                if keyword in col.get("name", "").lower():
                    score += 1
        
        # Boost score for common business tables
        business_keywords = ['sales', 'revenue', 'customer', 'product', 'order', 'transaction']
        for bk in business_keywords:
            if bk in table_lower:
                score += 3
        
        table_scores[table] = score
    
    # Return top 5 tables sorted by score
    sorted_tables = sorted(table_scores.items(), key=lambda x: x[1], reverse=True)
    return [table for table, score in sorted_tables[:5] if score > 0]

def _llm_table_selection(schema: Dict[str, Any], requirements: str, config: Dict[str, Any]) -> List[str]:
    """LLM-based table selection."""
    openai_config = config.get('openai', {})
    api_key = openai_config.get('api_key')
    
    if not api_key:
        raise ValueError("OpenAI API key not found in config")
    
    # Summarize schema to avoid token limits
    schema_summary = _summarize_schema(schema)
    
    if openai_config.get('use_azure', False):
        llm = AzureChatOpenAI(
            azure_deployment=openai_config.get('model', 'gpt-4'),
            azure_endpoint=openai_config.get('azure_endpoint'),
            api_version=openai_config.get('azure_version', '2023-12-01-preview'),
            api_key=api_key,
            temperature=0.2
        )
    else:
        llm = ChatOpenAI(
            api_key=api_key,
            model=openai_config.get('model', 'gpt-4'),
            temperature=0.2
        )
    
    system_message = SystemMessage(
        content="You are an expert database analyst. Select the most relevant tables for generating a Power BI report based on the requirements."
    )
    
    human_message = HumanMessage(
        content=f"""
Database Schema Summary:
{json.dumps(schema_summary, indent=2)}
Report Requirements:
{requirements}
Select the most relevant tables for this report. Return a JSON array containing only the table names as strings.
Focus on tables that directly relate to the requirements and can provide meaningful insights.
Example response format:
["table1", "table2", "table3"]
"""
    )
    
    try:
        response = llm.invoke([system_message, human_message])
        response_text = response.content.strip()
        
        # Try to extract JSON from response
        json_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
        if json_match:
            table_list = json.loads(json_match.group())
            if isinstance(table_list, list) and all(isinstance(t, str) for t in table_list):
                # Validate tables exist in schema
                valid_tables = [t for t in table_list if t in schema]
                return valid_tables[:10]  # Limit to 10 tables max
        
        logger.warning("LLM returned invalid table list format")
        return _heuristic_table_selection(schema, requirements)
        
    except Exception as e:
        logger.error(f"LLM table selection error: {str(e)}")
        raise

def _summarize_schema(schema: Dict[str, Any], max_tables: int = 50) -> Dict[str, Any]:
    """Summarize schema to reduce token usage."""
    summary = {}
    
    # Take only the first max_tables tables
    tables_to_include = list(schema.keys())[:max_tables]
    
    for table_name in tables_to_include:
        table_info = schema[table_name]
        columns = table_info.get("columns", [])
        
        # Limit columns per table
        column_summary = []
        for col in columns[:20]:  # Max 20 columns per table
            column_summary.append({
                "name": col.get("name"),
                "type": col.get("type")
            })
        
        summary[table_name] = {
            "column_count": len(columns),
            "columns": column_summary
        }
    
    return summary

# === Decision Making Functions ===
def make_sql_decision(error: Exception, attempt: int, max_attempts: int) -> Dict[str, Any]:
    """Make decision on SQL generation retry."""
    if attempt >= max_attempts:
        return {
            "decision": "abort",
            "message": "Maximum SQL generation attempts reached",
            "error": str(error),
            "guidance": "Simplify requirements or check data source connectivity"
        }
    
    return {
        "decision": "retry",
        "message": f"Retrying SQL generation (attempt {attempt}/{max_attempts})",
        "error": str(error),
        "guidance": "Adjusting generation approach based on error"
    }

def make_python_decision(error: Exception, attempt: int, max_attempts: int) -> Dict[str, Any]:
    """Make decision on Python script generation retry."""
    if attempt >= max_attempts:
        return {
            "decision": "abort",
            "message": "Maximum Python script generation attempts reached",
            "error": str(error),
            "guidance": "Review script requirements and security constraints"
        }
    
    return {
        "decision": "retry",
        "message": f"Retrying Python script generation (attempt {attempt}/{max_attempts})",
        "error": str(error),
        "guidance": "Adjusting script generation to meet security requirements"
    }

# === Data Source Base Classes ===
class BaseDataSource:
    """Base class for all data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema information from data source."""
        raise NotImplementedError("Subclasses must implement get_schema method")
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from data source."""
        raise NotImplementedError("Subclasses must implement get_sample_data method")
    
    def test_connection(self) -> bool:
        """Test connection to data source."""
        try:
            schema = self.get_schema()
            return len(schema) > 0
        except Exception:
            return False
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        if hasattr(self, 'connection') and self.connection:
            try:
                self.connection.close()
            except Exception:
                pass

class SQLServerDataSource(BaseDataSource):
    """SQL Server data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_string = self._build_connection_string()
        self.sql_validator = SQLQueryValidator()
    
    def _build_connection_string(self) -> str:
        """Build SQL Server connection string."""
        driver = self.config.get('driver', 'ODBC Driver 17 for SQL Server')
        server = self.config.get('server', 'localhost')
        database = self.config.get('database')
        username = self.config.get('username')
        password = self.config.get('password')
        
        if not all([server, database, username, password]):
            raise ValueError("Missing required SQL Server connection parameters")
        
        return f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;"
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema from SQL Server."""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string, timeout=30)
            cursor = connection.cursor()
            
            schema = {}
            
            # Get all user tables
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            tables = cursor.fetchall()
            
            for table_schema, table_name in tables:
                full_table_name = f"{table_schema}.{table_name}"
                
                # Get columns for each table
                cursor.execute("""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, table_schema, table_name)
                
                columns = cursor.fetchall()
                
                schema[full_table_name] = {
                    "schema": table_schema,
                    "name": table_name,
                    "columns": [
                        {
                            "name": col[0],
                            "type": col[1],
                            "nullable": col[2] == 'YES',
                            "default": col[3],
                            "max_length": col[4],
                            "precision": col[5],
                            "scale": col[6],
                            "position": col[7]
                        }
                        for col in columns
                    ]
                }
            
            logger.info(f"Retrieved schema for {len(schema)} tables from SQL Server")
            return schema
            
        except pyodbc.Error as e:
            logger.error(f"SQL Server connection error: {sanitize_log_data(str(e))}")
            raise ConnectionError(f"Failed to connect to SQL Server: {str(e)}")
        except Exception as e:
            logger.error(f"Error getting SQL Server schema: {sanitize_log_data(str(e))}")
            raise
        finally:
            if connection:
                connection.close()
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from SQL Server."""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string, timeout=30)
            
            # Get the first table with data
            cursor = connection.cursor()
            cursor.execute("""
                SELECT TOP 1 CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) as full_name
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            table_result = cursor.fetchone()
            
            if table_result:
                table_name = table_result[0]
                # Use parameterized query to prevent SQL injection
                query = f"SELECT TOP {limit} * FROM {table_name}"
                
                # Basic validation of table name
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table_name):
                    raise ValueError(f"Invalid table name: {table_name}")
                
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} sample rows from {table_name}")
                return df
            else:
                logger.warning("No tables found in database")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting sample data: {sanitize_log_data(str(e))}")
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()

class PostgreSQLDataSource(BaseDataSource):
    """PostgreSQL data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_string = self._build_connection_string()
        self.sql_validator = SQLQueryValidator()
    
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 5432)
        database = self.config.get('database')
        username = self.config.get('username')
        password = self.config.get('password')
        
        if not all([host, database, username, password]):
            raise ValueError("Missing required PostgreSQL connection parameters")
        
        return f"host={host} port={port} dbname={database} user={username} password={password}"
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema from PostgreSQL."""
        import psycopg2
        import psycopg2.extras
        
        connection = None
        try:
            connection = psycopg2.connect(self.connection_string)
            cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            schema = {}
            
            # Get all user tables
            cursor.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE' 
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
            
            tables = cursor.fetchall()
            
            for table in tables:
                table_schema = table['table_schema']
                table_name = table['table_name']
                full_table_name = f"{table_schema}.{table_name}"
                
                # Get columns for each table
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale,
                        ordinal_position
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_schema, table_name))
                
                columns = cursor.fetchall()
                
                schema[full_table_name] = {
                    "schema": table_schema,
                    "name": table_name,
                    "columns": [
                        {
                            "name": col['column_name'],
                            "type": col['data_type'],
                            "nullable": col['is_nullable'] == 'YES',
                            "default": col['column_default'],
                            "max_length": col['character_maximum_length'],
                            "precision": col['numeric_precision'],
                            "scale": col['numeric_scale'],
                            "position": col['ordinal_position']
                        }
                        for col in columns
                    ]
                }
            
            logger.info(f"Retrieved schema for {len(schema)} tables from PostgreSQL")
            return schema
            
        except Exception as e:
            logger.error(f"Error getting PostgreSQL schema: {sanitize_log_data(str(e))}")
            raise
        finally:
            if connection:
                connection.close()
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from PostgreSQL."""
        import psycopg2
        import psycopg2.extras
        
        connection = None
        try:
            connection = psycopg2.connect(self.connection_string)
            
            # Get the first table with data
            cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE' 
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                LIMIT 1
            """)
            
            table_result = cursor.fetchone()
            
            if table_result:
                table_schema = table_result['table_schema']
                table_name = table_result['table_name']
                full_table_name = f"{table_schema}.{table_name}"
                
                # Basic validation of table name
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', full_table_name):
                    raise ValueError(f"Invalid table name: {full_table_name}")
                
                df = pd.read_sql(f"SELECT * FROM {full_table_name} LIMIT {limit}", connection)
                logger.info(f"Retrieved {len(df)} sample rows from {full_table_name}")
                return df
            else:
                logger.warning("No tables found in database")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting sample data: {sanitize_log_data(str(e))}")
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()

class CSVDataSource(BaseDataSource):
    """CSV file data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = config.get('file_path')
        if not self.file_path or not os.path.exists(self.file_path):
            raise ValueError(f"CSV file not found: {self.file_path}")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema from CSV file."""
        try:
            # Read just the header to get column information
            df_sample = pd.read_csv(self.file_path, nrows=100)
            
            columns = []
            for col in df_sample.columns:
                dtype = str(df_sample[col].dtype)
                columns.append({
                    "name": col,
                    "type": self._pandas_to_sql_type(dtype),
                    "nullable": df_sample[col].isnull().any(),
                    "default": None
                })
            
            table_name = os.path.basename(self.file_path).replace('.csv', '')
            schema = {
                table_name: {
                    "name": table_name,
                    "columns": columns
                }
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error reading CSV schema: {str(e)}")
            raise
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from CSV file."""
        try:
            df = pd.read_csv(self.file_path, nrows=limit)
            logger.info(f"Retrieved {len(df)} rows from CSV file")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV data: {str(e)}")
            return pd.DataFrame()
    
    def _pandas_to_sql_type(self, pandas_type: str) -> str:
        """Convert pandas dtype to SQL type."""
        type_mapping = {
            'object': 'VARCHAR',
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'FLOAT',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'category': 'VARCHAR'
        }
        return type_mapping.get(pandas_type, 'VARCHAR')

class BigQueryDataSource(BaseDataSource):
    """Google BigQuery data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        
        if not self.project_id:
            raise ValueError("BigQuery project_id is required")
        
        if self.credentials_path and os.path.exists(self.credentials_path):
            self.credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
        else:
            # Use default credentials
            self.credentials = None
        
        self.client = bigquery.Client(
            project=self.project_id,
            credentials=self.credentials
        )
    
    def get_schema(self) -> Dict[str, Any]:
        """Get schema from BigQuery."""
        try:
            schema = {}
            
            # Get all datasets in the project
            datasets = list(self.client.list_datasets())
            
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                
                # Get all tables in the dataset
                tables = list(self.client.list_tables(dataset))
                
                for table in tables:
                    table_id = table.table_id
                    full_table_name = f"{dataset_id}.{table_id}"
                    
                    # Get table schema
                    table_ref = self.client.get_table(table)
                    columns = []
                    
                    for field in table_ref.schema:
                        columns.append({
                            "name": field.name,
                            "type": field.field_type,
                            "nullable": field.is_nullable,
                            "default": None,
                            "description": field.description
                        })
                    
                    schema[full_table_name] = {
                        "schema": dataset_id,
                        "name": table_id,
                        "columns": columns,
                        "description": table_ref.description,
                        "created": table_ref.created.isoformat() if table_ref.created else None,
                        "modified": table_ref.modified.isoformat() if table_ref.modified else None
                    }
            
            logger.info(f"Retrieved schema for {len(schema)} tables from BigQuery")
            return schema
            
        except Exception as e:
            logger.error(f"Error getting BigQuery schema: {str(e)}")
            raise
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """Get sample data from BigQuery."""
        try:
            # Get the first table with data
            datasets = list(self.client.list_datasets())
            
            if datasets:
                dataset = datasets[0]
                tables = list(self.client.list_tables(dataset))
                
                if tables:
                    table = tables[0]
                    table_id = table.table_id
                    dataset_id = dataset.dataset_id
                    full_table_name = f"{dataset_id}.{table_id}"
                    
                    # Query sample data
                    query = f"SELECT * FROM `{self.project_id}.{full_table_name}` LIMIT {limit}"
                    df = self.client.query(query).to_dataframe()
                    
                    logger.info(f"Retrieved {len(df)} sample rows from {full_table_name}")
                    return df
            
            logger.warning("No tables found in BigQuery project")
            return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting sample data from BigQuery: {str(e)}")
            return pd.DataFrame()

# === Data Source Factory ===
class DataSourceFactory:
    """Factory for creating data source instances."""
    
    @staticmethod
    def create_data_source(config: Dict[str, Any]) -> BaseDataSource:
        """Create appropriate data source based on configuration."""
        source_type = config.get('type')
        
        if source_type == 'sql_server':
            return SQLServerDataSource(config)
        elif source_type == 'postgresql':
            return PostgreSQLDataSource(config)
        elif source_type == 'csv':
            return CSVDataSource(config)
        elif source_type == 'bigquery':
            return BigQueryDataSource(config)
        else:
            raise NotImplementedError(f"Data source type '{source_type}' not implemented")

# === Fabric Integration ===
class FabricIntegration:
    """Integration with Microsoft Fabric for report deployment."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tenant_id = config.get('tenant_id')
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.workspace_id = config.get('workspace_id')
        self.api_endpoint = config.get('api_endpoint', 'https://api.fabric.microsoft.com/v1')
        
        if not all([self.tenant_id, self.client_id, self.client_secret, self.workspace_id]):
            raise ValueError("Missing required Fabric configuration parameters")
        
        self.access_token = self._get_access_token()
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def _get_access_token(self) -> str:
        """Get access token for Fabric API."""
        authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority_url
        )
        
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" not in result:
            raise ValueError(f"Failed to get access token: {result.get('error_description')}")
        
        return result["access_token"]
    
    def deploy_report(self, report_definition: Dict[str, Any], report_name: str) -> Dict[str, Any]:
        """Deploy Power BI report to Fabric workspace."""
        try:
            # Create a new report in the workspace
            create_report_url = f"{self.api_endpoint}/workspaces/{self.workspace_id}/reports"
            
            report_payload = {
                "displayName": report_name,
                "description": f"Generated report: {report_name}",
                "type": "Report",
                "definition": {
                    "parts": [
                        {
                            "path": "report.json",
                            "payload": report_definition,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            response = requests.post(
                create_report_url,
                headers=self.headers,
                json=report_payload
            )
            
            if response.status_code == 201:  # Created
                report_data = response.json()
                report_id = report_data.get('id')
                
                logger.info(f"Successfully deployed report to Fabric with ID: {report_id}")
                FABRIC_DEPLOYMENT_COUNT.labels(status='success').inc()
                
                return {
                    "status": "success",
                    "report_id": report_id,
                    "workspace_id": self.workspace_id,
                    "report_url": report_data.get('webUrl'),
                    "embed_url": report_data.get('embedUrl')
                }
            else:
                error_msg = f"Failed to deploy report: {response.status_code} - {response.text}"
                logger.error(error_msg)
                FABRIC_DEPLOYMENT_COUNT.labels(status='failed').inc()
                
                return {
                    "status": "error",
                    "error": error_msg,
                    "status_code": response.status_code
                }
                
        except Exception as e:
            error_msg = f"Error deploying report to Fabric: {str(e)}"
            logger.error(error_msg)
            FABRIC_DEPLOYMENT_COUNT.labels(status='failed').inc()
            
            return {
                "status": "error",
                "error": error_msg
            }
    
    def get_workspace_info(self) -> Dict[str, Any]:
        """Get information about the Fabric workspace."""
        try:
            workspace_url = f"{self.api_endpoint}/workspaces/{self.workspace_id}"
            response = requests.get(workspace_url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get workspace info: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting workspace info: {str(e)}")
            return {}

# === Power BI Report Generator ===
class PowerBIReportGenerator:
    """Main class for generating Power BI reports using LangChain."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.cache_manager = CacheManager(config.get('monitoring', {}).get('redis_url', 'redis://localhost:6379'))
        self.data_profiler = DataProfiler(config)
        self.schema_drift_detector = SchemaDriftDetector(self.cache_manager)
        self.script_sanitizer = PythonScriptSanitizer(config.get('sandbox_path', '/tmp/sandbox'))
        self.sql_validator = SQLQueryValidator()
        
        # Initialize OpenAI client
        openai_config = config.get('openai', {})
        if not openai_config.get('api_key'):
            raise ValueError("OpenAI API key is required")
        
        if openai_config.get('use_azure', False):
            self.llm = AzureChatOpenAI(
                azure_deployment=openai_config.get('model', 'gpt-4'),
                azure_endpoint=openai_config.get('azure_endpoint'),
                api_version=openai_config.get('azure_version', '2023-12-01-preview'),
                api_key=openai_config['api_key'],
                temperature=openai_config.get('temperature', 0.3),
                max_tokens=openai_config.get('max_tokens', 2000)
            )
        else:
            self.llm = ChatOpenAI(
                api_key=openai_config['api_key'],
                model=openai_config.get('model', 'gpt-4'),
                temperature=openai_config.get('temperature', 0.3),
                max_tokens=openai_config.get('max_tokens', 2000)
            )
        
        # Initialize Fabric integration if enabled
        self.fabric_integration = None
        if config.get('enable_fabric_deployment', True):
            try:
                self.fabric_integration = FabricIntegration(config.get('fabric', {}))
            except Exception as e:
                logger.warning(f"Failed to initialize Fabric integration: {str(e)}")
        
        # Initialize Power BI template handler
        self.template_handler = PowerBITemplate(config.get('visualization', {}))
        
        # Initialize memory for conversation context
        self.memory = ConversationBufferMemory()
        
        logger.info("PowerBI Report Generator initialized successfully")
    
    @retry_with_backoff(retries=3)
    def generate_report(self, data_source_name: str, report_requirements: str) -> Dict[str, Any]:
        """Generate Power BI report based on data source and requirements."""
        start_time = time.time()
        
        try:
            with REQUEST_DURATION.labels(endpoint='generate_report').time():
                # Validate inputs
                if not data_source_name or not report_requirements:
                    raise ValueError("Data source name and requirements are required")
                
                # Get data source configuration
                data_source_config = self.config['data_sources'].get(data_source_name)
                if not data_source_config:
                    raise ValueError(f"Data source '{data_source_name}' not found in configuration")
                
                # Create data source
                data_source = DataSourceFactory.create_data_source(data_source_config)
                
                # Test connection
                if not data_source.test_connection():
                    raise ConnectionError(f"Failed to connect to data source '{data_source_name}'")
                
                # Get schema and detect drift
                schema = data_source.get_schema()
                if not schema:
                    raise ValueError(f"No schema available for data source '{data_source_name}'")
                
                drift_result = self.schema_drift_detector.detect_schema_drift(data_source_name, schema)
                if drift_result.get('drift_detected'):
                    logger.warning(f"Schema drift detected for {data_source_name}: {drift_result['message']}")
                
                # Get sample data for analysis
                sample_data = data_source.get_sample_data(self.config.get('data_chunk_size', 1000))
                if sample_data.empty:
                    logger.warning(f"No sample data available from {data_source_name}")
                
                # Perform data quality analysis if enabled
                data_profile = None
                if self.config.get('data_quality', {}).get('enabled', True):
                    data_profile = self.data_profiler.profile_data(sample_data, data_source_name)
                    
                    # Check for critical issues
                    critical_issues = [i for i in data_profile.get('issues', []) if i.get('severity') == 'error']
                    if critical_issues:
                        logger.warning(f"Critical data quality issues found in {data_source_name}: {len(critical_issues)} issues")
                
                # Select relevant tables
                selected_tables = select_table_list(schema, report_requirements, config=self.config)
                if not selected_tables:
                    logger.warning("No relevant tables selected for report generation")
                    selected_tables = list(schema.keys())[:5]  # Fallback to first 5 tables
                
                # Generate SQL queries
                sql_queries = self._generate_sql_queries(schema, sample_data, report_requirements, selected_tables)
                
                # Generate Python script
                python_script = self._generate_python_script(sql_queries, sample_data, report_requirements)
                
                # Generate DAX measures
                dax_measures = self._generate_dax_measures(schema, sample_data, report_requirements, selected_tables)
                
                # Generate visualizations
                visualizations = self._generate_visualizations(schema, sample_data, report_requirements, selected_tables)
                
                # Generate report definition
                report_definition = self._generate_report_definition(
                    selected_tables, sql_queries, python_script, dax_measures, 
                    visualizations, sample_data, report_requirements
                )
                
                # Create Power BI report
                report_id = self._create_powerbi_report(report_definition, report_requirements)
                
                # Deploy to Fabric if enabled
                fabric_deployment = None
                if self.fabric_integration and self.config.get('enable_fabric_deployment', True):
                    fabric_deployment = self.fabric_integration.deploy_report(
                        report_definition, 
                        f"Generated Report - {data_source_name}"
                    )
                
                # Record metrics
                REQUEST_COUNT.labels(endpoint='generate_report', status='success').inc()
                
                result = {
                    "status": "success",
                    "report_id": report_id,
                    "data_source": data_source_name,
                    "execution_time": round(time.time() - start_time, 2),
                    "report_definition": report_definition,
                    "selected_tables": selected_tables,
                    "sql_queries": sql_queries,
                    "dax_measures": dax_measures,
                    "visualizations": visualizations,
                    "data_profile": data_profile,
                    "schema_drift": drift_result,
                    "fabric_deployment": fabric_deployment,
                    "metadata": {
                        "generated_at": datetime.now().isoformat(),
                        "table_count": len(selected_tables),
                        "query_count": len(sql_queries),
                        "measure_count": len(dax_measures) if dax_measures else 0,
                        "visualization_count": len(visualizations) if visualizations else 0,
                        "sample_row_count": len(sample_data) if not sample_data.empty else 0
                    }
                }
                
                logger.info(f"Successfully generated report for {data_source_name} in {result['execution_time']}s")
                return result
                
        except Exception as e:
            ERROR_COUNT.labels(type='report_generation').inc()
            REQUEST_COUNT.labels(endpoint='generate_report', status='error').inc()
            logger.error(f"Report generation failed for {data_source_name}: {sanitize_log_data(str(e))}")
            raise
        
        finally:
            # Cleanup data source connection
            if 'data_source' in locals():
                data_source.cleanup()
    
    def _generate_sql_queries(self, schema: Dict[str, Any], sample_data: pd.DataFrame, 
                            requirements: str, selected_tables: List[str]) -> List[str]:
        """Generate SQL queries using LangChain."""
        
        sql_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="You are an expert SQL developer. Generate SQL queries for a Power BI report."),
            HumanMessage(content=f"""
Selected Tables: {json.dumps(selected_tables)}
Schema Information:
{json.dumps(_summarize_schema(schema), indent=2)}
Sample Data (first 10 rows):
{sample_data.head(10).to_json(orient='records') if not sample_data.empty else "{}"}
Report Requirements:
{requirements}
Generate appropriate SQL queries that:
1. Only use SELECT statements (no DML operations)
2. Focus on the selected tables
3. Address the report requirements
4. Are optimized for Power BI consumption
5. Include proper JOINs where relationships exist
Return a JSON object with this exact format:
{{"queries": ["SELECT query1", "SELECT query2"]}}
Ensure all queries start with SELECT or WITH statements only.
""")
        ])
        
        # Create chain
        sql_chain = LLMChain(llm=self.llm, prompt=sql_prompt, verbose=False)
        
        # Generate queries with retry logic
        for attempt in range(1, self.config.get('max_sql_generation_try', 3) + 1):
            try:
                result = sql_chain.run({})
                
                # Parse result
                sql_data = json.loads(result)
                sql_queries = sql_data.get("queries", [])
                
                # Validate queries
                validation_result = self.sql_validator.validate_sql_queries(sql_queries)
                if not validation_result["is_valid"]:
                    raise ValueError(f"SQL validation failed: {validation_result['error']}")
                
                logger.info(f"Generated {len(sql_queries)} valid SQL queries")
                return sql_queries
                
            except Exception as e:
                decision = make_sql_decision(e, attempt, self.config.get('max_sql_generation_try', 3))
                logger.warning(f"SQL generation attempt {attempt} failed: {str(e)}")
                
                if decision['decision'] == 'abort':
                    raise Exception(f"SQL generation failed after {attempt} attempts: {decision['error']}")
        
        return []
    
    def _generate_python_script(self, sql_queries: List[str], sample_data: pd.DataFrame, 
                               requirements: str) -> str:
        """Generate Python script using LangChain."""
        
        python_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="You are an expert Python developer specializing in data transformation for Power BI."),
            HumanMessage(content=f"""
SQL Queries:
{json.dumps(sql_queries)}
Sample Data:
{sample_data.head(10).to_json(orient='records') if not sample_data.empty else "{}"}
Report Requirements:
{requirements}
Generate a Python script that:
1. Uses pandas for data manipulation
2. Includes proper data transformations
3. Creates calculated columns where needed
4. Handles data quality issues
5. Prepares data optimally for Power BI
6. Uses only safe, allowed libraries (pandas, numpy, matplotlib, plotly)
7. Does not include any network calls or file system access outside sandbox
Return a JSON object with this exact format:
{{"script": "python_code_here"}}
The script should be production-ready and follow best practices.
""")
        ])
        
        # Create chain
        python_chain = LLMChain(llm=self.llm, prompt=python_prompt, verbose=False)
        
        # Generate script with retry logic
        for attempt in range(1, self.config.get('max_python_script_check', 3) + 1):
            try:
                result = python_chain.run({})
                
                # Parse result
                script_data = json.loads(result)
                python_script = script_data.get("script", "")
                
                # Validate script security
                sanitization_result = self.script_sanitizer.sanitize_python_script(python_script)
                if not sanitization_result.get("is_safe", False):
                    raise ValueError(f"Script security validation failed: {sanitization_result.get('error')}")
                
                logger.info("Generated secure Python script successfully")
                return python_script
                
            except Exception as e:
                decision = make_python_decision(e, attempt, self.config.get('max_python_script_check', 3))
                logger.warning(f"Python script generation attempt {attempt} failed: {str(e)}")
                
                if decision['decision'] == 'abort':
                    raise Exception(f"Python script generation failed after {attempt} attempts: {decision['error']}")
        
        return ""
    
    def _generate_dax_measures(self, schema: Dict[str, Any], sample_data: pd.DataFrame, 
                              requirements: str, selected_tables: List[str]) -> List[Dict[str, Any]]:
        """Generate DAX measures using LangChain."""
        
        dax_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="You are a DAX expert for Power BI. Generate DAX measures for a Power BI report."),
            HumanMessage(content=f"""
Selected Tables: {json.dumps(selected_tables)}
Schema Information:
{json.dumps(_summarize_schema(schema), indent=2)}
Sample Data (first 10 rows):
{sample_data.head(10).to_json(orient='records') if not sample_data.empty else "{}"}
Report Requirements:
{requirements}
Generate DAX measures that:
1. Address the report requirements
2. Follow DAX best practices
3. Include proper error handling
4. Are optimized for performance
5. Include comments explaining their purpose
Return a JSON object with this exact format:
{{
    "measures": [
        {{
            "name": "MeasureName",
            "table": "TableName",
            "expression": "DAX expression here",
            "description": "Description of what the measure does"
        }}
    ]
}}
""")
        ])
        
        # Create chain
        dax_chain = LLMChain(llm=self.llm, prompt=dax_prompt, verbose=False)
        
        try:
            result = dax_chain.run({})
            
            # Parse result
            dax_data = json.loads(result)
            dax_measures = dax_data.get("measures", [])
            
            logger.info(f"Generated {len(dax_measures)} DAX measures")
            return dax_measures
            
        except Exception as e:
            logger.error(f"Error generating DAX measures: {str(e)}")
            return []
    
    def _generate_visualizations(self, schema: Dict[str, Any], sample_data: pd.DataFrame, 
                                requirements: str, selected_tables: List[str]) -> List[Dict[str, Any]]:
        """Generate visualization specifications using LangChain."""
        
        viz_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="You are a Power BI visualization expert. Generate visualization specifications for a Power BI report."),
            HumanMessage(content=f"""
Selected Tables: {json.dumps(selected_tables)}
Schema Information:
{json.dumps(_summarize_schema(schema), indent=2)}
Sample Data (first 10 rows):
{sample_data.head(10).to_json(orient='records') if not sample_data.empty else "{}"}
Report Requirements:
{requirements}
Generate visualization specifications that:
1. Address the report requirements
2. Follow Power BI visualization best practices
3. Include appropriate chart types for the data
4. Specify data bindings and formatting
5. Include titles and labels
Return a JSON object with this exact format:
{{
    "visualizations": [
        {{
            "type": "chart_type",
            "title": "Visualization Title",
            "dataFields": [
                {{
                    "name": "Field Name",
                    "source": "Table.Field"
                }}
            ],
            "categoryField": "Table.CategoryField",
            "formatting": {{
                "colors": ["#01B8AA", "#374649"],
                "showLabels": true
            }}
        }}
    ]
}}
Supported chart types: column, bar, line, pie, donut, scatter, map, card, table, matrix
""")
        ])
        
        # Create chain
        viz_chain = LLMChain(llm=self.llm, prompt=viz_prompt, verbose=False)
        
        try:
            result = viz_chain.run({})
            
            # Parse result
            viz_data = json.loads(result)
            visualizations = viz_data.get("visualizations", [])
            
            logger.info(f"Generated {len(visualizations)} visualizations")
            return visualizations
            
        except Exception as e:
            logger.error(f"Error generating visualizations: {str(e)}")
            return []
    
    def _generate_report_definition(self, selected_tables: List[str], sql_queries: List[str], 
                                  python_script: str, dax_measures: List[Dict[str, Any]],
                                  visualizations: List[Dict[str, Any]], sample_data: pd.DataFrame, 
                                  requirements: str) -> Dict[str, Any]:
        """Generate comprehensive report definition."""
        
        definition_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="You are a Power BI expert. Create a comprehensive report definition."),
            HumanMessage(content=f"""
Selected Tables: {json.dumps(selected_tables)}
SQL Queries: {json.dumps(sql_queries)}
Python Script: {python_script}
DAX Measures: {json.dumps(dax_measures)}
Visualizations: {json.dumps(visualizations)}
Sample Data: {sample_data.head(10).to_json(orient='records') if not sample_data.empty else "{}"}
Requirements: {requirements}
Generate a complete Power BI report definition including:
1. Data model with relationships
2. Report pages and layout
3. Visual placement and formatting
4. Filters and slicers
5. Drill-through configurations
6. Bookmarks and navigation
Return a detailed JSON object with the report specification.
""")
        ])
        
        # Create chain
        definition_chain = LLMChain(llm=self.llm, prompt=definition_prompt, verbose=False)
        
        try:
            result = definition_chain.run({})
            
            # Try to parse as JSON, fallback to text
            try:
                report_definition = json.loads(result)
            except json.JSONDecodeError:
                report_definition = {"definition": result, "format": "text"}
            
            # Add generated components
            report_definition.update({
                "sql_queries": sql_queries,
                "python_script": python_script,
                "dax_measures": dax_measures,
                "visualizations": visualizations,
                "selected_tables": selected_tables,
                "generated_at": datetime.now().isoformat()
            })
            
            # Use template handler to enhance the report definition
            report_definition = self.template_handler.enhance_report_definition(report_definition)
            
            return report_definition
            
        except Exception as e:
            logger.error(f"Error generating report definition: {str(e)}")
            # Return minimal definition
            return {
                "sql_queries": sql_queries,
                "python_script": python_script,
                "dax_measures": dax_measures,
                "visualizations": visualizations,
                "selected_tables": selected_tables,
                "error": str(e),
                "generated_at": datetime.now().isoformat()
            }
    
    def _create_powerbi_report(self, report_definition: Dict[str, Any], report_requirements: str) -> str:
        """Create Power BI report file."""
        # Generate a unique report ID
        report_id = f"report_{int(time.time())}_{secrets.token_hex(4)}"
        
        try:
            # Create PBIX file using template handler
            pbix_path = self.template_handler.create_pbix_file(report_definition, report_id)
            
            # Save report definition to file for debugging
            os.makedirs("reports", exist_ok=True)
            with open(f"reports/{report_id}.json", 'w') as f:
                json.dump(report_definition, f, indent=2, default=str)
            
            logger.info(f"Created Power BI report with ID: {report_id}")
            return report_id
            
        except Exception as e:
            logger.error(f"Error creating Power BI report: {str(e)}")
            raise

# === Power BI Template Handler (Stub) ===
class PowerBITemplate:
    """Handle Power BI template creation and manipulation."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.theme = config.get('theme', 'standard')
        self.color_palette = config.get('color_palette', ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"])
        self.font_family = config.get('font_family', 'Segoe UI')
        self.default_visualization_types = config.get('default_visualization_types', ["column", "line", "pie", "card", "table"])
    
    def enhance_report_definition(self, report_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance report definition with template styling and formatting."""
        # Apply theme and styling
        if "theme" not in report_definition:
            report_definition["theme"] = {
                "name": self.theme,
                "colorPalette": self.color_palette,
                "fontFamily": self.font_family
            }
        
        # Apply default formatting to visualizations
        if "visualizations" in report_definition:
            for viz in report_definition["visualizations"]:
                if "formatting" not in viz:
                    viz["formatting"] = {
                        "colors": self.color_palette,
                        "fontFamily": self.font_family
                    }
        
        return report_definition
    
    def create_pbix_file(self, report_definition: Dict[str, Any], report_id: str) -> str:
        """Create a PBIX file from the report definition."""
        # In a real implementation, this would:
        # 1. Create a PBIX file structure
        # 2. Add data model
        # 3. Add report pages and visuals
        # 4. Add DAX measures
        # 5. Save the PBIX file
        
        # For now, create a placeholder
        pbix_path = f"reports/{report_id}.pbix"
        
        try:
            # Create a minimal PBIX file structure
            with zipfile.ZipFile(pbix_path, 'w') as pbix:
                # Add report layout
                pbix.writestr("Report/Layout", json.dumps(report_definition.get("layout", {})))
                
                # Add data model
                pbix.writestr("DataModel", json.dumps(report_definition.get("dataModel", {})))
                
                # Add metadata
                pbix.writestr("Metadata", json.dumps({
                    "created": datetime.now().isoformat(),
                    "version": "1.0",
                    "generator": "PowerBI Report Generator"
                }))
            
            logger.info(f"Created PBIX file: {pbix_path}")
            return pbix_path
            
        except Exception as e:
            logger.error(f"Error creating PBIX file: {str(e)}")
            raise

# === Main Application ===
def main():
    """Main application entry point."""
    try:
        # Start metrics server
        start_metrics_server(8000)
        
        # Load configuration
        config = load_config()
        
        # Validate required configuration
        if not config.get('openai', {}).get('api_key'):
            logger.error("OpenAI API key is required")
            return
        
        if not config.get('data_sources'):
            logger.error("At least one data source must be configured")
            return
        
        # Initialize report generator
        generator = PowerBIReportGenerator(config)
        
        # Example usage
        data_source_names = list(config['data_sources'].keys())
        if data_source_names:
            data_source_name = data_source_names[0]
            report_requirements = """
            Generate a comprehensive sales dashboard showing:
            1. Monthly sales trends over time
            2. Top performing products by revenue
            3. Sales by region/territory with map visualization
            4. Customer analysis with segmentation
            5. Revenue metrics and KPIs with year-over-year comparisons
            6. Product category performance
            """
            
            logger.info(f"Generating report for data source: {data_source_name}")
            result = generator.generate_report(data_source_name, report_requirements)
            
            logger.info(f"✅ Report generated successfully!")
            logger.info(f"📊 Report ID: {result['report_id']}")
            logger.info(f"⏱️  Execution time: {result['execution_time']}s")
            logger.info(f"📋 Tables used: {len(result['selected_tables'])}")
            logger.info(f"🔍 Data quality score: {result['data_profile']['overall_score'] if result['data_profile'] else 'N/A'}")
            
            if result.get('fabric_deployment'):
                if result['fabric_deployment'].get('status') == 'success':
                    logger.info(f"☁️  Report deployed to Fabric with ID: {result['fabric_deployment']['report_id']}")
                    logger.info(f"🔗 Report URL: {result['fabric_deployment'].get('report_url', 'N/A')}")
                else:
                    logger.error(f"❌ Fabric deployment failed: {result['fabric_deployment'].get('error')}")
            
        else:
            logger.error("No data sources configured for report generation")
    
    except Exception as e:
        logger.error(f"Application error: {sanitize_log_data(str(e))}")
        ERROR_COUNT.labels(type='application_error').inc()

if __name__ == "__main__":
    main()