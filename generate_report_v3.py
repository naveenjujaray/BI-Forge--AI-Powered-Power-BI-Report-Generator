import os
import io
import re
import json
import time
import gc
import base64
import logging
import tempfile
import zipfile
import asyncio
import threading
from datetime import datetime, timedelta
from functools import wraps, lru_cache
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager

# Standard library imports
import ast
import secrets
import hashlib
from collections import defaultdict

# Third-party imports
import pandas as pd
import numpy as np
import requests
import pyodbc
import yaml
import jwt
from scipy import stats
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from circuitbreaker import circuit
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import tenacity

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
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient
from azure.eventhub.exceptions import EventHubError

# Microsoft Fabric imports
from microsoft.fabric.core import FabricClient
from microsoft.fabric.items import ItemClient
from microsoft.fabric.workspaces import WorkspaceClient

# Google Cloud imports
from google.cloud import bigquery
from google.oauth2 import service_account

# Salesforce imports
from simple_salesforce import Salesforce

# OpenAI imports
import openai
from openai import OpenAI

# LangChain imports
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain.chains import LLMChain, SequentialChain, TransformChain
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.callbacks import CallbackManager
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.tools import tool
from langchain.memory import ConversationBufferMemory
from langgraph import StateGraph, MessageGraph
from langsmith import Client
from langsmith.run_helpers import traceable

# SQL parsing
import sqlparse

# Pydantic imports
from pydantic import BaseModel, field_validator

# Database connection pooling
from sqlalchemy import create_engine, pool

# FastAPI imports
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

# Kafka imports (for real-time streaming)
try:
    from kafka import KafkaConsumer, KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Machine learning imports (for advanced analytics)
try:
    from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
    from sklearn.preprocessing import LabelEncoder
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Airflow imports (optional)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Testing imports
import pytest
from unittest.mock import Mock, patch

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
LANGCHAIN_OPERATION_COUNT = Counter('langchain_operations_total', 'Total LangChain operations', ['operation'])
WORKER_COUNT = Gauge('powerbi_worker_count', 'Current number of workers')
STREAMING_DATA_PROCESSED = Counter('streaming_data_processed_total', 'Total streaming data processed', ['source'])
HUMAN_INTERACTION_COUNT = Counter('human_interaction_total', 'Total human interactions', ['type'])
COPILOT_ACTION_COUNT = Counter('copilot_action_total', 'Total Copilot actions', ['action'])
ML_PREDICTION_COUNT = Counter('ml_prediction_total', 'Total ML predictions', ['model'])
NLQ_COUNT = Counter('nlq_query_total', 'Total natural language queries', ['status'])

# Rate limiting setup
limiter = Limiter(key_func=get_remote_address)

def start_metrics_server(port: int = 8000) -> None:
    """Start Prometheus metrics server."""
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start Prometheus metrics server: {str(e)}")

# In generate_report_v3.py, add robust API client
class APIClient:
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_retries),
            wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
            retry=tenacity.retry_if_exception_type((requests.exceptions.Timeout, 
                                                     requests.exceptions.ConnectionError))
        )
        
        self.get_with_retry = retry_strategy(self._get)
        self.post_with_retry = retry_strategy(self._post)
    
    def _get(self, endpoint: str, params: Dict = None):
        response = self.session.get(
            f"{self.base_url}/{endpoint}",
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()
    
    def _post(self, endpoint: str, data: Dict):
        response = self.session.post(
            f"{self.base_url}/{endpoint}",
            json=data,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()
    
    # In generate_report_v3.py, add memory management utilities
def process_data_in_chunks(data: pd.DataFrame, process_func: Callable, chunk_size: int = 10000):
    """Process large DataFrames in chunks to avoid memory issues."""
    chunks = [data[i:i + chunk_size] for i in range(0, data.shape[0], chunk_size)]
    results = []
    
    for chunk in chunks:
        try:
            result = process_func(chunk)
            results.append(result)
            # Explicitly free memory
            del chunk
            gc.collect()
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            continue
    
    return pd.concat(results) if results else pd.DataFrame()

# Example usage in DataAnalystAgent
def analyze_large_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze large dataset with memory management."""
    
    def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
        # Process each chunk
        return chunk.describe()
    
    # Process in chunks
    summary_stats = process_data_in_chunks(df, chunk_size=5000, process_func=process_chunk)
    
    return {
        "status": "success",
        "summary_stats": summary_stats.to_dict(),
        "rows_processed": len(df)
    }
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
    langsmith_project: Optional[str] = None
    langsmith_endpoint: Optional[str] = None

class FabricConfig(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    pipeline_id: Optional[str] = None
    capacity_id: Optional[str] = None
    api_endpoint: str = "https://api.fabric.microsoft.com/v1"

class StreamingConfig(BaseModel):
    enabled: bool = False
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    event_hub_connection_string: Optional[str] = None
    event_hub_name: Optional[str] = None
    consumer_group: str = "$Default"
    checkpoint_interval: int = 30  # seconds
    batch_size: int = 100

class CopilotConfig(BaseModel):
    enabled: bool = False
    write_back_enabled: bool = False
    data_agents_enabled: bool = False
    grounding_context: List[str] = []
    terminology: Dict[str, str] = {}
    api_key: Optional[str] = None

class UIConfig(BaseModel):
    low_code_enabled: bool = False
    interactive_dashboards: bool = False
    mobile_responsive: bool = False
    theme: str = "standard"
    color_palette: List[str] = ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"]
    font_family: str = "Segoe UI"

class AdvancedAnalyticsConfig(BaseModel):
    predictive_modeling: bool = False
    natural_language_querying: bool = False
    automated_insights: bool = False
    model_path: str = "/models"
    confidence_threshold: float = 0.7

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
    warehouse: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None
    account: Optional[str] = None
    connection_pool_size: int = 5
    connection_timeout: int = 30
    streaming: bool = False

    @field_validator('type')
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        allowed_types = [
            'sql_server','postgresql','mysql','csv','excel','api',
            'bigquery','salesforce','s3','redshift','onelake','semantic_model',
            'oracle','snowflake','mongodb','synapse','kafka','event_hub'
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
    api_key_rotation_days: int = 90

class MonitoringConfig(BaseModel):
    application_insights: str = ""
    log_analytics_workspace: str = ""
    prometheus_endpoint: str = "http://prometheus:9090"
    redis_url: str = "redis://localhost:6379"
    circuit_breaker_timeout: int = 60
    rate_limit_requests: int = 100
    rate_limit_period: int = 60
    enable_advanced_monitoring: bool = True
    performance_benchmarking: bool = True
    data_lineage_tracking: bool = True

class VisualizationConfig(BaseModel):
    theme: str = "standard"
    color_palette: List[str] = ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"]
    font_family: str = "Segoe UI"
    default_visualization_types: List[str] = ["column", "line", "pie", "card", "table"]
    enable_custom_visuals: bool = True

class ScalingConfig(BaseModel):
    enabled: bool = False
    min_workers: int = 2
    max_workers: int = 10
    scale_up_threshold: float = 0.7  # Scale up when 70% of workers are busy
    scale_down_threshold: float = 0.3  # Scale down when 30% of workers are busy
    cooldown_period: int = 300  # 5 minutes cooldown between scaling operations
    distributed_processing: bool = False
    kubernetes_enabled: bool = False
    load_balancing: bool = False
    resource_optimization: bool = False

class ApiConfig(BaseModel):
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: List[str] = ["*"]
    docs_url: str = "/docs"
    openapi_url: str = "/openapi.json"

class MainConfig(BaseModel):
    openai: AIConfig
    fabric: FabricConfig
    data_sources: Dict[str, DataSourceConfig]
    data_quality: DataQualityConfig = DataQualityConfig()
    security: SecurityConfig = SecurityConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    visualization: VisualizationConfig = VisualizationConfig()
    scaling: ScalingConfig = ScalingConfig()
    api: ApiConfig = ApiConfig()
    streaming: StreamingConfig = StreamingConfig()
    copilot: CopilotConfig = CopilotConfig()
    ui: UIConfig = UIConfig()
    advanced_analytics: AdvancedAnalyticsConfig = AdvancedAnalyticsConfig()
    max_workers: int = 4
    data_chunk_size: int = 10000
    max_sql_generation_try: int = 3
    max_python_script_check: int = 3
    sandbox_path: str = "/tmp/sandbox"
    enable_fabric_deployment: bool = True
    enable_langsmith: bool = True
    enable_real_time_refresh: bool = False

def create_default_config() -> Dict[str, Any]:
    """Create a default configuration when config file is missing."""
    return {
        "openai": {
            "api_key": os.getenv("OPENAI_API_KEY", ""),
            "model": "gpt-4",
            "temperature": 0.3,
            "max_tokens": 2000,
            "max_retries": 3,
            "use_azure": False,
            "enable_langsmith": True
        },
        "fabric": {
            "tenant_id": os.getenv("FABRIC_TENANT_ID", ""),
            "client_id": os.getenv("FABRIC_CLIENT_ID", ""),
            "client_secret": os.getenv("FABRIC_CLIENT_SECRET", ""),
            "workspace_id": os.getenv("FABRIC_WORKSPACE_ID", ""),
            "api_endpoint": "https://api.fabric.microsoft.com/v1"
        },
        "data_sources": {},
        "streaming": {
            "enabled": False,
            "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            "kafka_topic": os.getenv("KAFKA_TOPIC", ""),
            "event_hub_connection_string": os.getenv("EVENT_HUB_CONNECTION_STRING", ""),
            "event_hub_name": os.getenv("EVENT_HUB_NAME", ""),
            "consumer_group": "$Default",
            "checkpoint_interval": 30,
            "batch_size": 100
        },
        "copilot": {
            "enabled": False,
            "write_back_enabled": False,
            "data_agents_enabled": False,
            "grounding_context": [],
            "terminology": {},
            "api_key": os.getenv("COPILOT_API_KEY", "")
        },
        "ui": {
            "low_code_enabled": False,
            "interactive_dashboards": False,
            "mobile_responsive": False,
            "theme": "standard",
            "color_palette": ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"],
            "font_family": "Segoe UI"
        },
        "advanced_analytics": {
            "predictive_modeling": False,
            "natural_language_querying": False,
            "automated_insights": False,
            "model_path": "/models",
            "confidence_threshold": 0.7
        },
        "monitoring": {
            "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379"),
            "circuit_breaker_timeout": 60,
            "rate_limit_requests": 100,
            "rate_limit_period": 60,
            "enable_advanced_monitoring": True,
            "performance_benchmarking": True,
            "data_lineage_tracking": True
        },
        "scaling": {
            "enabled": False,
            "min_workers": 2,
            "max_workers": 10,
            "scale_up_threshold": 0.7,
            "scale_down_threshold": 0.3,
            "cooldown_period": 300,
            "distributed_processing": False,
            "kubernetes_enabled": False,
            "load_balancing": False,
            "resource_optimization": False
        },
        "api": {
            "enabled": False,
            "host": "0.0.0.0",
            "port": 8000,
            "cors_origins": ["*"],
            "docs_url": "/docs",
            "openapi_url": "/openapi.json"
        },
        "enable_fabric_deployment": True,
        "enable_langsmith": True,
        "enable_real_time_refresh": False
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
        
        if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
            config_data.setdefault('streaming', {})['kafka_bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        
        if 'EVENT_HUB_CONNECTION_STRING' in os.environ:
            config_data.setdefault('streaming', {})['event_hub_connection_string'] = os.environ['EVENT_HUB_CONNECTION_STRING']
        
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
        r'(api[_-]?key["\s]*[:=]["\s]*)[^\s"]+',
        r'(password["\s]*[:=]["\s]*)[^\s"]+',
        r'(token["\s]*[:=]["\s]*)[^\s"]+',
        r'(secret["\s]*[:=]["\s]*)[^\s"]+',
    ]
    
    sanitized = data
    for pattern in patterns:
        sanitized = re.sub(pattern, r'\1***REDACTED***', sanitized, flags=re.IGNORECASE)
    
    return sanitized

def encrypt_sensitive_data(data: str, key: str) -> str:
    """Encrypt sensitive data using AES."""
    salt = secrets.token_bytes(16)
    kdf = PBKDF2(key, salt, dkLen=32, count=600000)  # Increased iterations
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

# === Rate Limiting and Circuit Breaker ===
class RateLimiter:
    """Simple token bucket rate limiter."""
    
    def __init__(self, max_requests: int, period: int):
        self.max_requests = max_requests
        self.period = period
        self.tokens = max_requests
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def consume(self) -> bool:
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            
            if elapsed > self.period:
                self.tokens = self.max_requests
                self.last_refill = now
            
            if self.tokens > 0:
                self.tokens -= 1
                return True
            return False

# === Real-Time Streaming Analytics ===
class RealTimeDataProcessor:
    """Process streaming data from Kafka or Event Hubs."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('streaming', {})
        self.enabled = self.config.get('enabled', False)
        self.kafka_bootstrap_servers = self.config.get('kafka_bootstrap_servers')
        self.kafka_topic = self.config.get('kafka_topic')
        self.event_hub_connection_string = self.config.get('event_hub_connection_string')
        self.event_hub_name = self.config.get('event_hub_name')
        self.consumer_group = self.config.get('consumer_group', '$Default')
        self.batch_size = self.config.get('batch_size', 100)
        self.checkpoint_interval = self.config.get('checkpoint_interval', 30)
        
        self.kafka_consumer = None
        self.kafka_producer = None
        self.event_hub_consumer = None
        self.event_hub_producer = None
        self.running = False
        self.processors = {}
        
        if self.enabled:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize Kafka or Event Hub clients based on configuration."""
        if self.kafka_bootstrap_servers and KAFKA_AVAILABLE:
            try:
                self.kafka_consumer = KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    group_id=self.consumer_group,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                     # Add connection timeout and retry settings
                    connections_max_idle_ms=300000,
                    request_timeout_ms=30000,
                    retry_backoff_ms=100,
                )
                
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logger.info("Kafka clients initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka clients: {str(e)}")
                  # Implement fallback mechanism
                self.enabled = False
        
        if self.event_hub_connection_string:
            try:
                self.event_hub_consumer = EventHubConsumerClient.from_connection_string(
                    conn_str=self.event_hub_connection_string,
                    consumer_group=self.consumer_group,
                    eventhub_name=self.event_hub_name
                )
                
                self.event_hub_producer = EventHubProducerClient.from_connection_string(
                    conn_str=self.event_hub_connection_string,
                    eventhub_name=self.event_hub_name
                )
                logger.info("Event Hub clients initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Event Hub clients: {str(e)}")
    
    def register_processor(self, data_source: str, processor_func: Callable):
        """Register a processor function for a specific data source."""
        self.processors[data_source] = processor_func
        logger.info(f"Registered processor for data source: {data_source}")
    
    def process_streaming_data(self, stream_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process streaming data based on configuration."""
        if not self.enabled:
            return {"status": "disabled", "message": "Streaming is not enabled"}
        
        source = stream_config.get('source', 'kafka')
        data_source = stream_config.get('data_source', 'default')
        
        if source == 'kafka' and self.kafka_consumer:
            return self._process_kafka_data(data_source)
        elif source == 'event_hub' and self.event_hub_consumer:
            return self._process_event_hub_data(data_source)
        else:
            return {"status": "error", "message": f"Unsupported streaming source: {source}"}
    
    def _process_kafka_data(self, data_source: str) -> Dict[str, Any]:
        """Process data from Kafka."""
        if data_source not in self.processors:
            return {"status": "error", "message": f"No processor registered for {data_source}"}
        
        processor = self.processors[data_source]
        processed_count = 0
        error_count = 0
        
        try:
            self.running = True
            for message in self.kafka_consumer:
                if not self.running:
                    break
                
                try:
                    # Process the message
                    result = processor(message.value)
                    processed_count += 1
                    
                    # Update metrics
                    STREAMING_DATA_PROCESSED.labels(source='kafka').inc()
                    
                    # Process in batches
                    if processed_count % self.batch_size == 0:
                        logger.info(f"Processed {processed_count} messages from Kafka")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing Kafka message: {str(e)}")
            
            return {
                "status": "completed",
                "processed_count": processed_count,
                "error_count": error_count
            }
            
        except Exception as e:
            logger.error(f"Error in Kafka processing: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _process_event_hub_data(self, data_source: str) -> Dict[str, Any]:
        """Process data from Event Hub."""
        if data_source not in self.processors:
            return {"status": "error", "message": f"No processor registered for {data_source}"}
        
        processor = self.processors[data_source]
        processed_count = 0
        error_count = 0
        
        try:
            self.running = True
            
            def on_event(partition_context, event):
                nonlocal processed_count, error_count
                
                try:
                    # Extract data from event
                    event_data = json.loads(event.body_as_str())
                    
                    # Process the event
                    result = processor(event_data)
                    processed_count += 1
                    
                    # Update metrics
                    STREAMING_DATA_PROCESSED.labels(source='event_hub').inc()
                    
                    # Process in batches
                    if processed_count % self.batch_size == 0:
                        logger.info(f"Processed {processed_count} events from Event Hub")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing Event Hub event: {str(e)}")
            
            # Start receiving events
            with self.event_hub_consumer:
                self.event_hub_consumer.receive(
                    on_event=on_event,
                    starting_position="-1",  # From the beginning
                    on_error=lambda error: logger.error(f"Event Hub error: {error}")
                )
            
            return {
                "status": "completed",
                "processed_count": processed_count,
                "error_count": error_count
            }
            
        except EventHubError as e:
            logger.error(f"Event Hub error: {str(e)}")
            return {"status": "error", "message": str(e)}
        except Exception as e:
            logger.error(f"Error in Event Hub processing: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def publish_to_stream(self, data: Dict[str, Any], source: str = 'kafka') -> Dict[str, Any]:
        """Publish data to a streaming source."""
        if source == 'kafka' and self.kafka_producer:
            try:
                future = self.kafka_producer.send(self.kafka_topic, value=data)
                result = future.get(timeout=10)  # Wait for send to complete
                return {"status": "success", "message": "Data published to Kafka"}
            except Exception as e:
                logger.error(f"Error publishing to Kafka: {str(e)}")
                return {"status": "error", "message": str(e)}
        
        elif source == 'event_hub' and self.event_hub_producer:
            try:
                event_data_batch = self.event_hub_producer.create_batch()
                event_data_batch.add(EventData(json.dumps(data).encode('utf-8')))
                self.event_hub_producer.send_batch(event_data_batch)
                return {"status": "success", "message": "Data published to Event Hub"}
            except Exception as e:
                logger.error(f"Error publishing to Event Hub: {str(e)}")
                return {"status": "error", "message": str(e)}
        
        else:
            return {"status": "error", "message": f"Unsupported streaming source: {source}"}
    
    def stop_processing(self):
        """Stop the streaming data processing."""
        self.running = False
        logger.info("Streaming data processing stopped")

# === Enhanced Human-in-the-Loop ===
class InteractiveReportBuilder:
    """Interactive report builder with human-in-the-loop capabilities."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ui_config = config.get('ui', {})
        self.low_code_enabled = self.ui_config.get('low_code_enabled', False)
        self.interactive_dashboards = self.ui_config.get('interactive_dashboards', False)
        self.mobile_responsive = self.ui_config.get('mobile_responsive', False)
        self.theme = self.ui_config.get('theme', 'standard')
        self.color_palette = self.ui_config.get('color_palette', ["#01B8AA", "#374649", "#FD625E", "#F2C80F", "#4BC0C0"])
        self.font_family = self.ui_config.get('font_family', 'Segoe UI')
        
        self.report_drafts = {}
        self.review_queue = []
        self.approved_reports = {}
        self.rejected_reports = {}
    
    def create_report_draft(self, report_config: Dict[str, Any]) -> str:
        """Create a new report draft for review."""
        draft_id = f"draft_{int(time.time())}_{secrets.token_hex(4)}"
        
        # Initialize the draft with basic structure
        draft = {
            "id": draft_id,
            "status": "draft",
            "created_at": datetime.now().isoformat(),
            "created_by": report_config.get('created_by', 'system'),
            "config": report_config,
            "pages": [],
            "visuals": [],
            "data_sources": [],
            "feedback": [],
            "approval_status": "pending"
        }
        
        self.report_drafts[draft_id] = draft
        self.review_queue.append(draft_id)
        
        logger.info(f"Created report draft: {draft_id}")
        HUMAN_INTERACTION_COUNT.labels(type='draft_created').inc()
        
        return draft_id
    
    def create_approval_workflow(self, report_draft_id: str) -> Dict[str, Any]:
        """Create an approval workflow for a report draft."""
        if report_draft_id not in self.report_drafts:
            return {"status": "error", "message": f"Report draft not found: {report_draft_id}"}
        
        draft = self.report_drafts[report_draft_id]
        draft["approval_status"] = "in_review"
        draft["review_started_at"] = datetime.now().isoformat()
        
        # Create a review interface
        review_interface = {
            "draft_id": report_draft_id,
            "title": f"Review Report: {draft.get('config', {}).get('title', 'Untitled')}",
            "description": f"Please review and provide feedback on this report draft.",
            "created_at": datetime.now().isoformat(),
            "reviewers": draft.get('config', {}).get('reviewers', []),
            "deadline": draft.get('config', {}).get('review_deadline', 
                                              (datetime.now() + timedelta(days=3)).isoformat()),
            "status": "pending_review",
            "feedback": []
        }
        
        logger.info(f"Created approval workflow for report draft: {report_draft_id}")
        HUMAN_INTERACTION_COUNT.labels(type='workflow_created').inc()
        
        return review_interface
    
    def submit_feedback(self, report_draft_id: str, feedback: Dict[str, Any]) -> Dict[str, Any]:
        """Submit feedback for a report draft."""
        if report_draft_id not in self.report_drafts:
            return {"status": "error", "message": f"Report draft not found: {report_draft_id}"}
        
        draft = self.report_drafts[report_draft_id]
        
        # Add feedback to the draft
        feedback_entry = {
            "id": f"feedback_{int(time.time())}_{secrets.token_hex(4)}",
            "timestamp": datetime.now().isoformat(),
            "reviewer": feedback.get('reviewer', 'anonymous'),
            "comments": feedback.get('comments', ''),
            "rating": feedback.get('rating', 3),  # 1-5 scale
            "suggestions": feedback.get('suggestions', []),
            "status": feedback.get('status', 'pending')  # pending, addressed, dismissed
        }
        
        draft["feedback"].append(feedback_entry)
        
        # Update metrics
        HUMAN_INTERACTION_COUNT.labels(type='feedback_submitted').inc()
        
        logger.info(f"Submitted feedback for report draft: {report_draft_id}")
        
        return {"status": "success", "feedback_id": feedback_entry["id"]}
    
    def approve_report(self, report_draft_id: str, approver: str, comments: str = "") -> Dict[str, Any]:
        """Approve a report draft."""
        if report_draft_id not in self.report_drafts:
            return {"status": "error", "message": f"Report draft not found: {report_draft_id}"}
        
        draft = self.report_drafts[report_draft_id]
        draft["approval_status"] = "approved"
        draft["approved_by"] = approver
        draft["approved_at"] = datetime.now().isoformat()
        draft["approval_comments"] = comments
        
        # Move from drafts to approved reports
        self.approved_reports[report_draft_id] = draft
        
        # Remove from review queue if present
        if report_draft_id in self.review_queue:
            self.review_queue.remove(report_draft_id)
        
        # Update metrics
        HUMAN_INTERACTION_COUNT.labels(type='report_approved').inc()
        
        logger.info(f"Approved report draft: {report_draft_id}")
        
        return {"status": "success", "message": "Report approved successfully"}
    
    def reject_report(self, report_draft_id: str, rejector: str, reason: str = "") -> Dict[str, Any]:
        """Reject a report draft."""
        if report_draft_id not in self.report_drafts:
            return {"status": "error", "message": f"Report draft not found: {report_draft_id}"}
        
        draft = self.report_drafts[report_draft_id]
        draft["approval_status"] = "rejected"
        draft["rejected_by"] = rejector
        draft["rejected_at"] = datetime.now().isoformat()
        draft["rejection_reason"] = reason
        
        # Move from drafts to rejected reports
        self.rejected_reports[report_draft_id] = draft
        
        # Remove from review queue if present
        if report_draft_id in self.review_queue:
            self.review_queue.remove(report_draft_id)
        
        # Update metrics
        HUMAN_INTERACTION_COUNT.labels(type='report_rejected').inc()
        
        logger.info(f"Rejected report draft: {report_draft_id}")
        
        return {"status": "success", "message": "Report rejected"}
    
    def create_low_code_interface(self, report_id: str) -> Dict[str, Any]:
        """Create a low-code interface for report customization."""
        if not self.low_code_enabled:
            return {"status": "error", "message": "Low-code interface is not enabled"}
        
        if report_id not in self.approved_reports:
            return {"status": "error", "message": f"Approved report not found: {report_id}"}
        
        report = self.approved_reports[report_id]
        
        # Create a low-code interface configuration
        interface_config = {
            "report_id": report_id,
            "title": report.get('config', {}).get('title', 'Untitled Report'),
            "description": report.get('config', {}).get('description', ''),
            "theme": self.theme,
            "color_palette": self.color_palette,
            "font_family": self.font_family,
            "pages": report.get('pages', []),
            "visuals": report.get('visuals', []),
            "data_sources": report.get('data_sources', []),
            "interactive_elements": {
                "filters": True,
                "drill_down": True,
                "tooltips": True,
                "bookmarks": True,
                "buttons": True
            },
            "mobile_optimized": self.mobile_responsive,
            "created_at": datetime.now().isoformat()
        }
        
        logger.info(f"Created low-code interface for report: {report_id}")
        HUMAN_INTERACTION_COUNT.labels(type='low_code_created').inc()
        
        return interface_config
    
    def create_interactive_dashboard(self, report_id: str) -> Dict[str, Any]:
        """Create an interactive dashboard from a report."""
        if not self.interactive_dashboards:
            return {"status": "error", "message": "Interactive dashboards are not enabled"}
        
        if report_id not in self.approved_reports:
            return {"status": "error", "message": f"Approved report not found: {report_id}"}
        
        report = self.approved_reports[report_id]
        
        # Create an interactive dashboard configuration
        dashboard_config = {
            "report_id": report_id,
            "title": f"Interactive Dashboard: {report.get('config', {}).get('title', 'Untitled')}",
            "description": report.get('config', {}).get('description', ''),
            "theme": self.theme,
            "color_palette": self.color_palette,
            "font_family": self.font_family,
            "layout": "grid",  # grid, freeform
            "real_time_updates": True,
            "auto_refresh_interval": 300,  # seconds
            "interactive_features": {
                "cross_filtering": True,
                "drill_through": True,
                "bookmarks": True,
                "tooltips": True,
                "comments": True,
                "sharing": True
            },
            "mobile_optimized": self.mobile_responsive,
            "created_at": datetime.now().isoformat()
        }
        
        logger.info(f"Created interactive dashboard for report: {report_id}")
        HUMAN_INTERACTION_COUNT.labels(type='dashboard_created').inc()
        
        return dashboard_config

# === Power BI Copilot Integration ===
class PowerBICopilot:
    """Integration with Power BI Copilot features."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.copilot_config = config.get('copilot', {})
        self.enabled = self.copilot_config.get('enabled', False)
        self.write_back_enabled = self.copilot_config.get('write_back_enabled', False)
        self.data_agents_enabled = self.copilot_config.get('data_agents_enabled', False)
        self.grounding_context = self.copilot_config.get('grounding_context', [])
        self.terminology = self.copilot_config.get('terminology', {})
        self.api_key = self.copilot_config.get('api_key')
        
        self.openai_config = config.get('openai', {})
        self.fabric_config = config.get('fabric', {})
        
        if self.enabled and self.api_key:
            self.client = OpenAI(api_key=self.api_key)
        else:
            self.client = None
    
    def generate_insights(self, data: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Generate insights from data using Copilot."""
        if not self.enabled or not self.client:
            return {"status": "error", "message": "Copilot is not enabled or configured"}
        
        try:
            # Prepare data summary
            data_summary = {
                "shape": data.shape,
                "columns": list(data.columns),
                "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()},
                "sample_data": data.head(5).to_dict(),
                "summary_stats": data.describe().to_dict() if not data.empty else {}
            }
            
            # Create prompt with grounding context
            context_prompt = "\n".join(self.grounding_context)
            terminology_prompt = "\n".join([f"{term}: {definition}" for term, definition in self.terminology.items()])
            
            system_prompt = f"""
            You are Power BI Copilot, an AI assistant that helps users analyze data and generate insights.
            
            Context:
            {context_prompt}
            
            Terminology:
            {terminology_prompt}
            
            Analyze the provided data and answer the user's question. Provide clear, actionable insights.
            """
            
            user_prompt = f"""
            Question: {question}
            
            Data Summary:
            {json.dumps(data_summary, indent=2)}
            """
            
            # Generate response
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            insights = response.choices[0].message.content
            
            # Update metrics
            COPILOT_ACTION_COUNT.labels(action='generate_insights').inc()
            
            return {
                "status": "success",
                "insights": insights,
                "question": question,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def write_back_data(self, dataset_id: str, updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Write data back to a dataset."""
        if not self.enabled or not self.write_back_enabled:
            return {"status": "error", "message": "Write-back is not enabled"}
        
        try:
            # This is a placeholder for the actual implementation
            # In a real scenario, this would use the Power BI API or Fabric API to write data back
            
            # Validate updates
            if not updates:
                return {"status": "error", "message": "No updates provided"}
            
            # Simulate write-back operation
            updated_rows = len(updates)
            
            # Update metrics
            COPILOT_ACTION_COUNT.labels(action='write_back').inc()
            
            return {
                "status": "success",
                "message": f"Successfully updated {updated_rows} rows in dataset {dataset_id}",
                "updated_rows": updated_rows,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error writing back data: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def create_data_agent(self, agent_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a data agent for automated analysis."""
        if not self.enabled or not self.data_agents_enabled:
            return {"status": "error", "message": "Data agents are not enabled"}
        
        try:
            agent_id = f"agent_{int(time.time())}_{secrets.token_hex(4)}"
            
            # Create agent configuration
            agent = {
                "id": agent_id,
                "name": agent_config.get('name', 'Unnamed Agent'),
                "description": agent_config.get('description', ''),
                "dataset_id": agent_config.get('dataset_id'),
                "schedule": agent_config.get('schedule', 'daily'),
                "tasks": agent_config.get('tasks', []),
                "created_at": datetime.now().isoformat(),
                "status": "active"
            }
            
            # Update metrics
            COPILOT_ACTION_COUNT.labels(action='create_agent').inc()
            
            logger.info(f"Created data agent: {agent_id}")
            
            return {
                "status": "success",
                "agent": agent,
                "message": "Data agent created successfully"
            }
            
        except Exception as e:
            logger.error(f"Error creating data agent: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def generate_dax_measures(self, table_name: str, columns: List[str], requirements: str) -> Dict[str, Any]:
        """Generate DAX measures based on requirements."""
        if not self.enabled or not self.client:
            return {"status": "error", "message": "Copilot is not enabled or configured"}
        
        try:
            # Create prompt with grounding context
            context_prompt = "\n".join(self.grounding_context)
            terminology_prompt = "\n".join([f"{term}: {definition}" for term, definition in self.terminology.items()])
            
            system_prompt = f"""
            You are Power BI Copilot, an AI assistant that helps users create DAX measures.
            
            Context:
            {context_prompt}
            
            Terminology:
            {terminology_prompt}
            
            Generate DAX measures based on the provided table, columns, and requirements.
            Return a JSON object with measure names as keys and DAX expressions as values.
            """
            
            user_prompt = f"""
            Table Name: {table_name}
            
            Columns:
            {', '.join(columns)}
            
            Requirements:
            {requirements}
            """
            
            # Generate response
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content
            
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                measures = json.loads(json_match.group())
            else:
                # Fallback: try to parse the response as a simple text format
                measures = {}
                lines = response_text.split('\n')
                current_measure = None
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('-') and not line.startswith('*'):
                        if '=' in line:
                            parts = line.split('=', 1)
                            measure_name = parts[0].strip()
                            dax_expression = parts[1].strip()
                            measures[measure_name] = dax_expression
            
            # Update metrics
            COPILOT_ACTION_COUNT.labels(action='generate_dax').inc()
            
            return {
                "status": "success",
                "measures": measures,
                "table_name": table_name,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating DAX measures: {str(e)}")
            return {"status": "error", "message": str(e)}

# === Advanced Analytics Integration ===
class AdvancedAnalyticsEngine:
    """Advanced analytics capabilities including predictive modeling and natural language querying."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.analytics_config = config.get('advanced_analytics', {})
        self.predictive_modeling = self.analytics_config.get('predictive_modeling', False)
        self.natural_language_querying = self.analytics_config.get('natural_language_querying', False)
        self.automated_insights = self.analytics_config.get('automated_insights', False)
        self.model_path = self.analytics_config.get('model_path', '/models')
        self.confidence_threshold = self.analytics_config.get('confidence_threshold', 0.7)
        
        self.openai_config = config.get('openai', {})
        
        # Create model directory if it doesn't exist
        os.makedirs(self.model_path, exist_ok=True)
        
        # Initialize models cache
        self.models = {}
        
        # Check if sklearn is available
        self.sklearn_available = SKLEARN_AVAILABLE
    
    def train_predictive_model(self, data: pd.DataFrame, target_column: str, model_type: str = 'regression') -> Dict[str, Any]:
        """Train a predictive model."""
        if not self.predictive_modeling or not self.sklearn_available:
            return {"status": "error", "message": "Predictive modeling is not enabled or sklearn is not available"}
        
        try:
            # Prepare data
            X = data.drop(columns=[target_column])
            y = data[target_column]
            
            # Handle categorical variables
            for col in X.select_dtypes(include=['object']).columns:
                le = LabelEncoder()
                X[col] = le.fit_transform(X[col].astype(str))
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Train model based on type
            if model_type == 'regression':
                model = RandomForestRegressor(n_estimators=100, random_state=42)
                model.fit(X_train, y_train)
                
                # Evaluate model
                y_pred = model.predict(X_test)
                mse = mean_squared_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                
                metrics = {
                    "mse": mse,
                    "rmse": mse ** 0.5,
                    "r2": r2
                }
                
            elif model_type == 'classification':
                model = RandomForestClassifier(n_estimators=100, random_state=42)
                model.fit(X_train, y_train)
                
                # Evaluate model
                y_pred = model.predict(X_test)
                accuracy = accuracy_score(y_test, y_pred)
                
                metrics = {
                    "accuracy": accuracy
                }
                
            else:
                return {"status": "error", "message": f"Unsupported model type: {model_type}"}
            
            # Save model
            model_id = f"model_{int(time.time())}_{secrets.token_hex(4)}"
            model_filename = os.path.join(self.model_path, f"{model_id}.pkl")
            joblib.dump(model, model_filename)
            
            # Store model info
            self.models[model_id] = {
                "id": model_id,
                "type": model_type,
                "target_column": target_column,
                "features": list(X.columns),
                "model_path": model_filename,
                "metrics": metrics,
                "created_at": datetime.now().isoformat()
            }
            
            # Update metrics
            ML_PREDICTION_COUNT.labels(model=model_type).inc()
            
            return {
                "status": "success",
                "model_id": model_id,
                "model_type": model_type,
                "metrics": metrics,
                "message": "Model trained successfully"
            }
            
        except Exception as e:
            logger.error(f"Error training predictive model: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def predict_with_model(self, model_id: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Make predictions using a trained model."""
        if not self.predictive_modeling or not self.sklearn_available:
            return {"status": "error", "message": "Predictive modeling is not enabled or sklearn is not available"}
        
        if model_id not in self.models:
            return {"status": "error", "message": f"Model not found: {model_id}"}
        
        try:
            model_info = self.models[model_id]
            
            # Load model
            model = joblib.load(model_info["model_path"])
            
            # Prepare data
            X = data[model_info["features"]]
            
            # Handle categorical variables
            for col in X.select_dtypes(include=['object']).columns:
                le = LabelEncoder()
                X[col] = le.fit_transform(X[col].astype(str))
            
            # Make predictions
            predictions = model.predict(X)
            
            # For classification models, also get prediction probabilities
            if model_info["type"] == 'classification':
                probabilities = model.predict_proba(X)
                result = {
                    "predictions": predictions.tolist(),
                    "probabilities": probabilities.tolist(),
                    "model_id": model_id,
                    "model_type": model_info["type"]
                }
            else:
                result = {
                    "predictions": predictions.tolist(),
                    "model_id": model_id,
                    "model_type": model_info["type"]
                }
            
            # Update metrics
            ML_PREDICTION_COUNT.labels(model=model_info["type"]).inc()
            
            return {
                "status": "success",
                "result": result,
                "message": "Predictions generated successfully"
            }
            
        except Exception as e:
            logger.error(f"Error making predictions: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def natural_language_to_sql(self, question: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert natural language question to SQL query."""
        if not self.natural_language_querying:
            return {"status": "error", "message": "Natural language querying is not enabled"}
        
        try:
            openai_config = self.openai_config
            api_key = openai_config.get('api_key')
            
            if not api_key:
                return {"status": "error", "message": "OpenAI API key not found"}
            
            # Initialize OpenAI client
            if openai_config.get('use_azure', False):
                client = AzureChatOpenAI(
                    azure_deployment=openai_config.get('model', 'gpt-4'),
                    azure_endpoint=openai_config.get('azure_endpoint'),
                    api_version=openai_config.get('azure_version', '2023-12-01-preview'),
                    api_key=api_key,
                    temperature=0.2
                )
            else:
                client = ChatOpenAI(
                    api_key=api_key,
                    model=openai_config.get('model', 'gpt-4'),
                    temperature=0.2
                )
            
            # Summarize schema to avoid token limits
            schema_summary = _summarize_schema(schema)
            
            system_message = SystemMessage(
                content="You are an expert SQL developer. Convert natural language questions to SQL queries based on the provided schema."
            )
            
            human_message = HumanMessage(
                content=f"""
Database Schema:
{json.dumps(schema_summary, indent=2)}

Question:
{question}

Convert this question to a SQL query. Return only the SQL query without any explanations.
"""
            )
            
            response = client.invoke([system_message, human_message])
            sql_query = response.content.strip()
            
            # Clean up the response
            sql_query = re.sub(r'^```sql\n?', '', sql_query)
            sql_query = re.sub(r'\n?```$', '', sql_query)
            
            # Update metrics
            NLQ_COUNT.labels(status='success').inc()
            
            return {
                "status": "success",
                "question": question,
                "sql_query": sql_query,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error converting natural language to SQL: {str(e)}")
            NLQ_COUNT.labels(status='error').inc()
            return {"status": "error", "message": str(e)}
    
    def generate_automated_insights(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate automated insights from data."""
        if not self.automated_insights:
            return {"status": "error", "message": "Automated insights are not enabled"}
        
        try:
            insights = []
            
            # Basic data insights
            if not data.empty:
                # Shape insights
                insights.append({
                    "type": "data_shape",
                    "message": f"The dataset contains {data.shape[0]} rows and {data.shape[1]} columns."
                })
                
                # Missing values insights
                missing_values = data.isnull().sum()
                if missing_values.sum() > 0:
                    missing_pct = (missing_values.sum() / (data.shape[0] * data.shape[1])) * 100
                    insights.append({
                        "type": "missing_values",
                        "message": f"The dataset has {missing_pct:.2f}% missing values.",
                        "severity": "warning" if missing_pct > 20 else "info"
                    })
                
                # Numeric columns insights
                numeric_cols = data.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    # Correlation insights
                    if len(numeric_cols) > 1:
                        corr_matrix = data[numeric_cols].corr()
                        high_corr_pairs = []
                        
                        for i in range(len(numeric_cols)):
                            for j in range(i+1, len(numeric_cols)):
                                if abs(corr_matrix.iloc[i, j]) > 0.7:
                                    high_corr_pairs.append((numeric_cols[i], numeric_cols[j], corr_matrix.iloc[i, j]))
                        
                        if high_corr_pairs:
                            insights.append({
                                "type": "high_correlation",
                                "message": f"Found {len(high_corr_pairs)} pairs of highly correlated columns.",
                                "details": high_corr_pairs
                            })
                    
                    # Outlier insights
                    for col in numeric_cols:
                        q1 = data[col].quantile(0.25)
                        q3 = data[col].quantile(0.75)
                        iqr = q3 - q1
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        outliers = data[(data[col] < lower_bound) | (data[col] > upper_bound)]
                        
                        if len(outliers) > 0:
                            outlier_pct = (len(outliers) / len(data)) * 100
                            insights.append({
                                "type": "outliers",
                                "message": f"Column '{col}' has {outlier_pct:.2f}% outliers.",
                                "column": col,
                                "outlier_count": len(outliers)
                            })
                
                # Categorical columns insights
                categorical_cols = data.select_dtypes(include=['object']).columns
                for col in categorical_cols:
                    value_counts = data[col].value_counts()
                    if len(value_counts) > 10:
                        insights.append({
                            "type": "high_cardinality",
                            "message": f"Column '{col}' has high cardinality with {len(value_counts)} unique values.",
                            "column": col
                        })
                    
                    # Check for imbalance
                    if len(value_counts) <= 10:
                        max_pct = (value_counts.iloc[0] / len(data)) * 100
                        if max_pct > 80:
                            insights.append({
                                "type": "imbalanced",
                                "message": f"Column '{col}' is imbalanced with '{value_counts.index[0]}' representing {max_pct:.2f}% of values.",
                                "column": col
                            })
            
            # Update metrics
            NLQ_COUNT.labels(status='success').inc()
            
            return {
                "status": "success",
                "insights": insights,
                "insight_count": len(insights),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating automated insights: {str(e)}")
            NLQ_COUNT.labels(status='error').inc()
            return {"status": "error", "message": str(e)}

# === AutoScaler ===
class AutoScaler:
    """Dynamic resource scaling for report generation with advanced features."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('scaling', {})
        self.enabled = self.config.get('enabled', False)
        self.min_workers = self.config.get('min_workers', 2)
        self.max_workers = self.config.get('max_workers', 10)
        self.scale_up_threshold = self.config.get('scale_up_threshold', 0.7)
        self.scale_down_threshold = self.config.get('scale_down_threshold', 0.3)
        self.cooldown_period = self.config.get('cooldown_period', 300)
        self.distributed_processing = self.config.get('distributed_processing', False)
        self.kubernetes_enabled = self.config.get('kubernetes_enabled', False)
        self.load_balancing = self.config.get('load_balancing', False)
        self.resource_optimization = self.config.get('resource_optimization', False)
        
        self.current_workers = self.min_workers
        self.last_scale_time = time.time()
        self.worker_pool = None
        self.active_tasks = 0
        self.lock = threading.Lock()
        
        # Resource utilization tracking
        self.cpu_utilization = 0.0
        self.memory_utilization = 0.0
        self.request_queue_length = 0
        
        if self.enabled:
            self.worker_pool = ThreadPoolExecutor(max_workers=self.max_workers)
            WORKER_COUNT.set(self.current_workers)
            logger.info(f"AutoScaler initialized with {self.current_workers} workers")
            
            # Start resource monitoring if resource optimization is enabled
            if self.resource_optimization:
                self._start_resource_monitoring()
    
    def _start_resource_monitoring(self):
        """Start monitoring system resources."""
        def monitor_resources():
            while self.enabled:
                try:
                    # Get CPU and memory utilization
                    # In a real implementation, this would use psutil or similar
                    # For now, we'll simulate with random values
                    self.cpu_utilization = min(100.0, max(0.0, self.cpu_utilization + (np.random.random() - 0.5) * 10))
                    self.memory_utilization = min(100.0, max(0.0, self.memory_utilization + (np.random.random() - 0.5) * 10))
                    
                    # Adjust scaling thresholds based on resource utilization
                    if self.cpu_utilization > 80 or self.memory_utilization > 80:
                        # Scale up more aggressively if resources are constrained
                        self.scale_up_threshold = max(0.5, self.scale_up_threshold - 0.1)
                    elif self.cpu_utilization < 30 and self.memory_utilization < 30:
                        # Scale down more aggressively if resources are underutilized
                        self.scale_down_threshold = min(0.5, self.scale_down_threshold + 0.1)
                    
                    time.sleep(10)  # Check every 10 seconds
                except Exception as e:
                    logger.error(f"Error in resource monitoring: {str(e)}")
                    time.sleep(30)  # Wait longer on error
        
        # Start monitoring in a separate thread
        monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
        monitor_thread.start()
        logger.info("Resource monitoring started")
    
    def _can_scale(self) -> bool:
        """Check if scaling is allowed based on cooldown period."""
        return time.time() - self.last_scale_time > self.cooldown_period
    
    def _scale_up(self):
        """Increase the number of workers."""
        if self.current_workers < self.max_workers and self._can_scale():
            self.current_workers = min(self.current_workers + 1, self.max_workers)
            self.last_scale_time = time.time()
            WORKER_COUNT.set(self.current_workers)
            logger.info(f"Scaled up to {self.current_workers} workers")
            
            # If Kubernetes is enabled, we would also scale the Kubernetes deployment
            if self.kubernetes_enabled:
                self._scale_kubernetes_deployment(self.current_workers)
    
    def _scale_down(self):
        """Decrease the number of workers."""
        if self.current_workers > self.min_workers and self._can_scale():
            self.current_workers = max(self.current_workers - 1, self.min_workers)
            self.last_scale_time = time.time()
            WORKER_COUNT.set(self.current_workers)
            logger.info(f"Scaled down to {self.current_workers} workers")
            
            # If Kubernetes is enabled, we would also scale the Kubernetes deployment
            if self.kubernetes_enabled:
                self._scale_kubernetes_deployment(self.current_workers)
    
    def _scale_kubernetes_deployment(self, target_replicas: int):
        """Scale a Kubernetes deployment."""
        # This is a placeholder for the actual implementation
        # In a real scenario, this would use the Kubernetes API to scale the deployment
        logger.info(f"Scaling Kubernetes deployment to {target_replicas} replicas")
    
    def register_task_start(self):
        """Register a new task starting."""
        with self.lock:
            self.active_tasks += 1
            self.request_queue_length = max(0, self.request_queue_length - 1)
            utilization = self.active_tasks / self.current_workers
            
            if utilization > self.scale_up_threshold:
                self._scale_up()
    
    def register_task_completion(self):
        """Register a task completion."""
        with self.lock:
            self.active_tasks = max(0, self.active_tasks - 1)
            utilization = self.active_tasks / self.current_workers
            
            if utilization < self.scale_down_threshold:
                self._scale_down()
    
    def submit_task(self, task: Callable, *args, **kwargs):
        """Submit a task to the worker pool."""
        if not self.enabled or not self.worker_pool:
            return task(*args, **kwargs)
        
        # If load balancing is enabled, we might distribute tasks differently
        if self.load_balancing:
            # This is a placeholder for load balancing logic
            # In a real scenario, this might consider task complexity, resource requirements, etc.
            pass
        
        self.register_task_start()
        
        def wrapper():
            try:
                return task(*args, **kwargs)
            finally:
                self.register_task_completion()
        
        future = self.worker_pool.submit(wrapper)
        return future.result()
    
    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource utilization status."""
        return {
            "current_workers": self.current_workers,
            "active_tasks": self.active_tasks,
            "request_queue_length": self.request_queue_length,
            "cpu_utilization": self.cpu_utilization,
            "memory_utilization": self.memory_utilization,
            "scale_up_threshold": self.scale_up_threshold,
            "scale_down_threshold": self.scale_down_threshold
        }
    
    def shutdown(self):
        """Shutdown the worker pool."""
        if self.worker_pool:
            self.worker_pool.shutdown(wait=True)
            logger.info("AutoScaler worker pool shutdown complete")

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
    """Redis-backed cache manager with in-memory fallback and data lineage tracking."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", enable_lineage: bool = False):
        self.redis_client = None
        self.in_memory_cache: Dict[str, Any] = {}
        self.enable_lineage = enable_lineage
        self.data_lineage: Dict[str, List[Dict[str, Any]]] = {}
        
        try:
            import redis
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
        
        # Track data lineage if enabled
        if self.enable_lineage:
            self._track_data_lineage(data_source_name, "schema_cache", schema)
    
    def _track_data_lineage(self, data_source: str, operation: str, data: Any) -> None:
        """Track data lineage for operations."""
        if not self.enable_lineage:
            return
        
        lineage_entry = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "data_source": data_source,
            "data_size": len(json.dumps(data)) if isinstance(data, (dict, list)) else 0
        }
        
        if data_source not in self.data_lineage:
            self.data_lineage[data_source] = []
        
        self.data_lineage[data_source].append(lineage_entry)
        
        # Keep only the last 100 entries per data source
        if len(self.data_lineage[data_source]) > 100:
            self.data_lineage[data_source] = self.data_lineage[data_source][-100:]
    
    def get_data_lineage(self, data_source: str) -> List[Dict[str, Any]]:
        """Get data lineage for a data source."""
        return self.data_lineage.get(data_source, [])
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        if self.redis_client:
            try:
                self.redis_client.flushall()
            except Exception as e:
                logger.error(f"Redis clear error: {str(e)}")
        
        self.in_memory_cache.clear()
        self.data_lineage.clear()

# === Data Quality Profiler ===
class DataProfiler:
    """Comprehensive data quality profiler with enhanced monitoring."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_quality_config = config.get('data_quality', {})
        self.monitoring_config = config.get('monitoring', {})
        self.enable_advanced_monitoring = self.monitoring_config.get('enable_advanced_monitoring', True)
        self.performance_benchmarking = self.monitoring_config.get('performance_benchmarking', True)
    
    def profile_data(self, data: pd.DataFrame, data_source_name: str) -> Dict[str, Any]:
        """Generate comprehensive data profile."""
        start_time = time.time()
        
        if data.empty:
            profile = self._empty_profile(data_source_name)
            if self.enable_advanced_monitoring:
                self._log_performance_metrics(data_source_name, "empty_profile", start_time)
            return profile
        
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
                col_start_time = time.time()
                col_profile = self._profile_column(data[col], col)
                profile["columns"][col] = col_profile
                
                if self.enable_advanced_monitoring:
                    self._log_performance_metrics(f"{data_source_name}.{col}", "column_profile", col_start_time)
            except Exception as e:
                logger.warning(f"Error profiling column {col}: {str(e)}")
                profile["columns"][col] = {"error": str(e)}
        
        # Apply quality checks
        profile["issues"] = self._apply_quality_checks(data, profile)
        profile["overall_score"] = self._calculate_overall_score(profile)
        
        # Update metrics
        DATA_QUALITY_SCORE.labels(data_source=data_source_name).set(profile["overall_score"])
        
        if self.enable_advanced_monitoring:
            self._log_performance_metrics(data_source_name, "data_profile", start_time)
        
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
    
    def _log_performance_metrics(self, data_source: str, operation: str, start_time: float) -> None:
        """Log performance metrics for monitoring."""
        if not self.performance_benchmarking:
            return
        
        duration = time.time() - start_time
        
        # In a real implementation, this would send metrics to a monitoring system
        logger.debug(f"Performance metric - {data_source}.{operation}: {duration:.3f}s")

# === Schema Drift Detection ===
class SchemaDriftDetector:
    """Detect schema changes over time with enhanced monitoring."""
    
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
    summarized = {}
    
    # Limit the number of tables to avoid token limits
    tables = list(schema.keys())
    if len(tables) > max_tables:
        # Prioritize tables with more columns
        tables = sorted(tables, key=lambda t: len(schema[t].get("columns", [])), reverse=True)[:max_tables]
    
    for table in tables:
        table_info = schema[table]
        summarized[table] = {
            "columns": table_info.get("columns", [])[:20]  # Limit columns per table
        }
    
    return summarized

# === Data Source Connectors ===
class DataSourceConnector:
    """Base class for data source connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.connection_pool = None
    
    def connect(self) -> bool:
        """Connect to the data source."""
        raise NotImplementedError("Subclasses must implement connect()")
    
    def disconnect(self) -> None:
        """Disconnect from the data source."""
        raise NotImplementedError("Subclasses must implement disconnect()")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the data source."""
        raise NotImplementedError("Subclasses must implement get_schema()")
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from the data source."""
        raise NotImplementedError("Subclasses must implement get_data()")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query."""
        raise NotImplementedError("Subclasses must implement execute_query()")

class SQLServerConnector(DataSourceConnector):
    """Connector for SQL Server databases."""
    
    def connect(self) -> bool:
        """Connect to SQL Server."""
        try:
            connection_string = (
                f"DRIVER={self.config.get('driver', 'ODBC Driver 17 for SQL Server')};"
                f"SERVER={self.config.get('server')};"
                f"DATABASE={self.config.get('database')};"
                f"UID={self.config.get('username')};"
                f"PWD={self.config.get('password')}"
            )
            
            self.connection_pool = create_engine(
                connection_string,
                pool_size=self.config.get('connection_pool_size', 5),
                pool_timeout=self.config.get('connection_timeout', 30)
            )
            
            # Test connection
            with self.connection_pool.connect() as conn:
                conn.execute("SELECT 1")
            
            logger.info("Connected to SQL Server")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from SQL Server."""
        if self.connection_pool:
            self.connection_pool.dispose()
            self.connection_pool = None
            logger.info("Disconnected from SQL Server")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the SQL Server database."""
        schema = {}
        
        try:
            with self.connection_pool.connect() as conn:
                # Get all tables
                tables_query = """
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                """
                tables_result = conn.execute(tables_query)
                tables = tables_result.fetchall()
                
                # Get columns for each table
                for table_schema, table_name in tables:
                    columns_query = """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                    """
                    columns_result = conn.execute(columns_query, (table_schema, table_name))
                    columns = columns_result.fetchall()
                    
                    table_key = f"{table_schema}.{table_name}" if table_schema else table_name
                    schema[table_key] = {
                        "columns": [
                            {
                                "name": column_name,
                                "type": data_type,
                                "nullable": is_nullable == "YES"
                            }
                            for column_name, data_type, is_nullable in columns
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from SQL Server: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from SQL Server."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                with self.connection_pool.connect() as conn:
                    result = conn.execute(query)
                    data = result.fetchall()
                    
                    # Get column names
                    column_names = [desc[0] for desc in result.cursor.description]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from SQL Server: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on SQL Server."""
        try:
            with self.connection_pool.connect() as conn:
                result = conn.execute(query)
                data = result.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in result.cursor.description]
                
                # Convert to DataFrame
                return pd.DataFrame(data, columns=column_names)
                
        except Exception as e:
            logger.error(f"Failed to execute query on SQL Server: {str(e)}")
            return pd.DataFrame()

class PostgreSQLConnector(DataSourceConnector):
    """Connector for PostgreSQL databases."""
    
    def connect(self) -> bool:
        """Connect to PostgreSQL."""
        try:
            connection_string = (
                f"postgresql://{self.config.get('username')}:{self.config.get('password')}"
                f"@{self.config.get('host')}:{self.config.get('port', 5432)}/{self.config.get('database')}"
            )
            
            self.connection_pool = create_engine(
                connection_string,
                pool_size=self.config.get('connection_pool_size', 5),
                pool_timeout=self.config.get('connection_timeout', 30)
            )
            
            # Test connection
            with self.connection_pool.connect() as conn:
                conn.execute("SELECT 1")
            
            logger.info("Connected to PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        if self.connection_pool:
            self.connection_pool.dispose()
            self.connection_pool = None
            logger.info("Disconnected from PostgreSQL")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the PostgreSQL database."""
        schema = {}
        
        try:
            with self.connection_pool.connect() as conn:
                # Get all tables
                tables_query = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                """
                tables_result = conn.execute(tables_query)
                tables = tables_result.fetchall()
                
                # Get columns for each table
                for table_schema, table_name in tables:
                    columns_query = """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """
                    columns_result = conn.execute(columns_query, (table_schema, table_name))
                    columns = columns_result.fetchall()
                    
                    table_key = f"{table_schema}.{table_name}" if table_schema else table_name
                    schema[table_key] = {
                        "columns": [
                            {
                                "name": column_name,
                                "type": data_type,
                                "nullable": is_nullable == "YES"
                            }
                            for column_name, data_type, is_nullable in columns
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from PostgreSQL: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from PostgreSQL."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                with self.connection_pool.connect() as conn:
                    result = conn.execute(query)
                    data = result.fetchall()
                    
                    # Get column names
                    column_names = [desc[0] for desc in result.cursor.description]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from PostgreSQL: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on PostgreSQL."""
        try:
            with self.connection_pool.connect() as conn:
                result = conn.execute(query)
                data = result.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in result.cursor.description]
                
                # Convert to DataFrame
                return pd.DataFrame(data, columns=column_names)
                
        except Exception as e:
            logger.error(f"Failed to execute query on PostgreSQL: {str(e)}")
            return pd.DataFrame()

class MySQLConnector(DataSourceConnector):
    """Connector for MySQL databases."""
    
    def connect(self) -> bool:
        """Connect to MySQL."""
        try:
            connection_string = (
                f"mysql+pymysql://{self.config.get('username')}:{self.config.get('password')}"
                f"@{self.config.get('host')}:{self.config.get('port', 3306)}/{self.config.get('database')}"
            )
            
            self.connection_pool = create_engine(
                connection_string,
                pool_size=self.config.get('connection_pool_size', 5),
                pool_timeout=self.config.get('connection_timeout', 30)
            )
            
            # Test connection
            with self.connection_pool.connect() as conn:
                conn.execute("SELECT 1")
            
            logger.info("Connected to MySQL")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from MySQL."""
        if self.connection_pool:
            self.connection_pool.dispose()
            self.connection_pool = None
            logger.info("Disconnected from MySQL")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the MySQL database."""
        schema = {}
        
        try:
            with self.connection_pool.connect() as conn:
                # Get all tables
                tables_query = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                """
                tables_result = conn.execute(tables_query)
                tables = tables_result.fetchall()
                
                # Get columns for each table
                for table_schema, table_name in tables:
                    columns_query = """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """
                    columns_result = conn.execute(columns_query, (table_schema, table_name))
                    columns = columns_result.fetchall()
                    
                    table_key = f"{table_schema}.{table_name}" if table_schema else table_name
                    schema[table_key] = {
                        "columns": [
                            {
                                "name": column_name,
                                "type": data_type,
                                "nullable": is_nullable == "YES"
                            }
                            for column_name, data_type, is_nullable in columns
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from MySQL: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from MySQL."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                with self.connection_pool.connect() as conn:
                    result = conn.execute(query)
                    data = result.fetchall()
                    
                    # Get column names
                    column_names = [desc[0] for desc in result.cursor.description]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from MySQL: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on MySQL."""
        try:
            with self.connection_pool.connect() as conn:
                result = conn.execute(query)
                data = result.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in result.cursor.description]
                
                # Convert to DataFrame
                return pd.DataFrame(data, columns=column_names)
                
        except Exception as e:
            logger.error(f"Failed to execute query on MySQL: {str(e)}")
            return pd.DataFrame()

class CSVConnector(DataSourceConnector):
    """Connector for CSV files."""
    
    def connect(self) -> bool:
        """Connect to CSV file."""
        try:
            file_path = self.config.get('file_path')
            if not file_path or not os.path.exists(file_path):
                logger.error(f"CSV file not found: {file_path}")
                return False
            
            logger.info(f"Connected to CSV file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to CSV file: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from CSV file."""
        logger.info("Disconnected from CSV file")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the CSV file."""
        try:
            file_path = self.config.get('file_path')
            
            # Read a small sample to get schema
            sample_df = pd.read_csv(file_path, nrows=10)
            
            schema = {
                os.path.basename(file_path): {
                    "columns": [
                        {
                            "name": col,
                            "type": str(dtype),
                            "nullable": sample_df[col].isnull().any()
                        }
                        for col, dtype in sample_df.dtypes.items()
                    ]
                }
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from CSV file: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from CSV file."""
        try:
            file_path = self.config.get('file_path')
            
            # Read CSV file
            usecols = columns if columns else None
            nrows = limit if limit else None
            
            df = pd.read_csv(file_path, usecols=usecols, nrows=nrows)
            
            # Apply where clause if provided
            if where_clause:
                # This is a simple implementation for demonstration
                # In a real scenario, you would need a more sophisticated parser
                df = df.query(where_clause)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get data from CSV file: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on CSV file."""
        # For CSV files, we'll just return the entire DataFrame
        # In a real scenario, you might implement a simple query parser
        try:
            file_path = self.config.get('file_path')
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            logger.error(f"Failed to execute query on CSV file: {str(e)}")
            return pd.DataFrame()

class ExcelConnector(DataSourceConnector):
    """Connector for Excel files."""
    
    def connect(self) -> bool:
        """Connect to Excel file."""
        try:
            file_path = self.config.get('file_path')
            if not file_path or not os.path.exists(file_path):
                logger.error(f"Excel file not found: {file_path}")
                return False
            
            logger.info(f"Connected to Excel file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Excel file: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Excel file."""
        logger.info("Disconnected from Excel file")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the Excel file."""
        try:
            file_path = self.config.get('file_path')
            
            # Get all sheet names
            xl_file = pd.ExcelFile(file_path)
            sheet_names = xl_file.sheet_names
            
            schema = {}
            
            for sheet_name in sheet_names:
                # Read a small sample to get schema
                sample_df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10)
                
                schema[sheet_name] = {
                    "columns": [
                        {
                            "name": col,
                            "type": str(dtype),
                            "nullable": sample_df[col].isnull().any()
                        }
                        for col, dtype in sample_df.dtypes.items()
                    ]
                }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Excel file: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Excel file."""
        try:
            file_path = self.config.get('file_path')
            
            all_data = []
            
            for table in tables:
                # Read Excel sheet
                usecols = columns if columns else None
                nrows = limit if limit else None
                
                df = pd.read_excel(file_path, sheet_name=table, usecols=usecols, nrows=nrows)
                
                # Apply where clause if provided
                if where_clause:
                    # This is a simple implementation for demonstration
                    # In a real scenario, you would need a more sophisticated parser
                    df = df.query(where_clause)
                
                all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from Excel file: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Excel file."""
        # For Excel files, we'll just return the first sheet
        # In a real scenario, you might implement a simple query parser
        try:
            file_path = self.config.get('file_path')
            xl_file = pd.ExcelFile(file_path)
            df = pd.read_excel(file_path, sheet_name=xl_file.sheet_names[0])
            return df
        except Exception as e:
            logger.error(f"Failed to execute query on Excel file: {str(e)}")
            return pd.DataFrame()

class APIConnector(DataSourceConnector):
    """Connector for REST APIs."""
    
    def connect(self) -> bool:
        """Connect to REST API."""
        try:
            url = self.config.get('url')
            if not url:
                logger.error("API URL not provided")
                return False
            
            # Test connection with a simple request
            headers = self.config.get('headers', {})
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                logger.info(f"Connected to API: {url}")
                return True
            else:
                logger.error(f"Failed to connect to API: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to API: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from REST API."""
        logger.info("Disconnected from API")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the API."""
        try:
            url = self.config.get('url')
            headers = self.config.get('headers', {})
            params = self.config.get('params', {})
            
            # Make a request to get sample data
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame to infer schema
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data[:10])  # Use first 10 records
                    
                    schema = {
                        "api_data": {
                            "columns": [
                                {
                                    "name": col,
                                    "type": str(dtype),
                                    "nullable": df[col].isnull().any()
                                }
                                for col, dtype in df.dtypes.items()
                            ]
                        }
                    }
                    
                    return schema
                else:
                    logger.error("API did not return a list of records")
                    return {}
            else:
                logger.error(f"Failed to get schema from API: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Failed to get schema from API: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from REST API."""
        try:
            url = self.config.get('url')
            headers = self.config.get('headers', {})
            params = self.config.get('params', {})
            
            # Make request
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                    
                    # Select columns if specified
                    if columns:
                        df = df[columns]
                    
                    # Apply limit if specified
                    if limit:
                        df = df.head(limit)
                    
                    # Apply where clause if provided
                    if where_clause:
                        # This is a simple implementation for demonstration
                        # In a real scenario, you would need a more sophisticated parser
                        df = df.query(where_clause)
                    
                    return df
                else:
                    logger.error("API did not return a list of records")
                    return pd.DataFrame()
            else:
                logger.error(f"Failed to get data from API: {response.status_code} - {response.text}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from API: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on REST API."""
        # For APIs, we'll just return all data
        # In a real scenario, you might implement a simple query parser
        return self.get_data(["api_data"])

class BigQueryConnector(DataSourceConnector):
    """Connector for Google BigQuery."""
    
    def connect(self) -> bool:
        """Connect to Google BigQuery."""
        try:
            credentials_path = self.config.get('credentials_path')
            project_id = self.config.get('project_id')
            
            if not credentials_path or not project_id:
                logger.error("BigQuery credentials path or project ID not provided")
                return False
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            
            # Create client
            self.connection = bigquery.Client(credentials=credentials, project=project_id)
            
            # Test connection
            self.connection.query("SELECT 1").result()
            
            logger.info(f"Connected to BigQuery project: {project_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Google BigQuery."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from BigQuery")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the BigQuery dataset."""
        try:
            project_id = self.config.get('project_id')
            dataset_id = self.config.get('dataset_id')
            
            if not dataset_id:
                logger.error("BigQuery dataset ID not provided")
                return {}
            
            # Get all tables in the dataset
            tables = list(self.connection.list_tables(f"{project_id}.{dataset_id}"))
            
            schema = {}
            
            for table in tables:
                table_ref = self.connection.get_table(table)
                
                schema[table.table_id] = {
                    "columns": [
                        {
                            "name": field.name,
                            "type": field.field_type,
                            "nullable": field.is_nullable
                        }
                        for field in table_ref.schema
                    ]
                }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from BigQuery: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Google BigQuery."""
        try:
            project_id = self.config.get('project_id')
            dataset_id = self.config.get('dataset_id')
            
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM `{project_id}.{dataset_id}.{table}`"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                query_job = self.connection.query(query)
                results = query_job.result()
                
                # Convert to DataFrame
                df = results.to_dataframe()
                all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from BigQuery: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Google BigQuery."""
        try:
            query_job = self.connection.query(query)
            results = query_job.result()
            
            # Convert to DataFrame
            return results.to_dataframe()
                
        except Exception as e:
            logger.error(f"Failed to execute query on BigQuery: {str(e)}")
            return pd.DataFrame()

class SalesforceConnector(DataSourceConnector):
    """Connector for Salesforce."""
    
    def connect(self) -> bool:
        """Connect to Salesforce."""
        try:
            username = self.config.get('username')
            password = self.config.get('password')
            security_token = self.config.get('security_token')
            sandbox = self.config.get('sandbox', False)
            
            if not username or not password or not security_token:
                logger.error("Salesforce username, password, or security token not provided")
                return False
            
            # Create connection
            self.connection = Salesforce(
                username=username,
                password=password,
                security_token=security_token,
                sandbox=sandbox
            )
            
            # Test connection
            self.connection.query("SELECT COUNT() FROM Account")
            
            logger.info("Connected to Salesforce")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Salesforce."""
        if self.connection:
            self.connection = None
            logger.info("Disconnected from Salesforce")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the Salesforce org."""
        try:
            # Get all objects
            objects = self.connection.describe()["sobjects"]
            
            schema = {}
            
            for obj in objects:
                if obj["queryable"]:
                    # Get fields for the object
                    fields = self.connection.query(f"SELECT {', '.join([f['name'] for f in obj['fields'][:10]])} FROM {obj['name']} LIMIT 1")
                    
                    schema[obj["name"]] = {
                        "columns": [
                            {
                                "name": field["name"],
                                "type": field["type"],
                                "nullable": field["nillable"]
                            }
                            for field in obj["fields"]
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Salesforce: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Salesforce."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "FIELDS(ALL)"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                result = self.connection.query_all(query)
                
                # Convert to DataFrame
                records = result['records']
                df = pd.DataFrame(records)
                
                # Remove attributes column
                if 'attributes' in df.columns:
                    df = df.drop(columns=['attributes'])
                
                all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from Salesforce: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Salesforce."""
        try:
            result = self.connection.query_all(query)
            
            # Convert to DataFrame
            records = result['records']
            df = pd.DataFrame(records)
            
            # Remove attributes column
            if 'attributes' in df.columns:
                df = df.drop(columns=['attributes'])
            
            return df
                
        except Exception as e:
            logger.error(f"Failed to execute query on Salesforce: {str(e)}")
            return pd.DataFrame()
# Add these new configuration classes to the existing code

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

class AutoScalingConfig(BaseModel):
    enabled: bool = True
    min_capacity: str = "F2"
    max_capacity: str = "F64"
    scale_triggers: Dict[str, Any] = {
        "cpu_threshold": 80,
        "memory_threshold": 85,
        "concurrent_users": 1000
    }

class LoadBalancingConfig(BaseModel):
    enabled: bool = True
    strategy: str = "round_robin"
    health_check_interval: int = 30

class CachingConfig(BaseModel):
    redis_cluster: Dict[str, Any] = {
        "enabled": True,
        "nodes": 3,
        "memory_per_node": "8GB"
    }

class PerformanceOptimizationConfig(BaseModel):
    query_timeout: int = 300
    max_concurrent_queries: int = 50
    incremental_refresh: bool = True

class ScalingConfig(BaseModel):
    enabled: bool = False
    min_workers: int = 2
    max_workers: int = 10
    scale_up_threshold: float = 0.7
    scale_down_threshold: float = 0.3
    cooldown_period: int = 300
    distributed_processing: bool = False
    kubernetes_enabled: bool = False
    load_balancing: bool = False
    resource_optimization: bool = False
    auto_scaling: AutoScalingConfig = AutoScalingConfig()
    load_balancing_config: LoadBalancingConfig = LoadBalancingConfig()
    caching: CachingConfig = CachingConfig()
    performance_optimization: PerformanceOptimizationConfig = PerformanceOptimizationConfig()

class MultiGeoConfig(BaseModel):
    home_region: str = "West Europe"
    multi_geo_capacities: List[str] = []
    data_residency_compliance: str = "GDPR"

class IntegrationsConfig(BaseModel):
    azure_devops_project: str = ""
    teams_webhook: str = ""
    key_vault_url: str = ""

# Update MainConfig to include new sections
class MainConfig(BaseModel):
    openai: AIConfig
    fabric: FabricConfig
    data_sources: Dict[str, DataSourceConfig]
    data_quality: DataQualityConfig = DataQualityConfig()
    security: SecurityConfig = SecurityConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    visualization: VisualizationConfig = VisualizationConfig()
    scaling: ScalingConfig = ScalingConfig()
    api: ApiConfig = ApiConfig()
    streaming: StreamingConfig = StreamingConfig()
    copilot: CopilotConfig = CopilotConfig()
    ui: UIConfig = UIConfig()
    advanced_analytics: AdvancedAnalyticsConfig = AdvancedAnalyticsConfig()
    compliance: ComplianceConfig = ComplianceConfig()
    deployment: DeploymentConfig = DeploymentConfig()
    multi_geo_config: MultiGeoConfig = MultiGeoConfig()
    integrations: IntegrationsConfig = IntegrationsConfig()
    max_workers: int = 4
    data_chunk_size: int = 10000
    max_sql_generation_try: int = 3
    max_python_script_check: int = 3
    sandbox_path: str = "/tmp/sandbox"
    enable_fabric_deployment: bool = True
    enable_langsmith: bool = True
    enable_real_time_refresh: bool = False

# Add new data source connectors
class S3Connector(DataSourceConnector):
    """Connector for Amazon S3."""
    
    def connect(self) -> bool:
        """Connect to Amazon S3."""
        try:
            import boto3
            self.access_key_id = self.config.get('access_key_id')
            self.secret_access_key = self.config.get('secret_access_key')
            self.region = self.config.get('region', 'us-east-1')
            self.bucket_name = self.config.get('bucket_name')
            self.prefix = self.config.get('prefix', '')
            
            # Create S3 client
            self.client = boto3.client(
                's3',
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region
            )
            
            # Test connection
            self.client.head_bucket(Bucket=self.bucket_name)
            
            logger.info(f"Connected to S3 bucket: {self.bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to S3: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Amazon S3."""
        self.client = None
        logger.info("Disconnected from S3")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the S3 data."""
        try:
            # List objects in the bucket
            objects = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            # For S3, we'll return a simple schema based on the first file
            if 'Contents' in objects:
                first_object = objects['Contents'][0]['Key']
                
                # Try to infer schema from file extension
                if first_object.endswith('.csv'):
                    return {
                        "s3_data": {
                            "columns": [
                                {"name": "column1", "type": "string", "nullable": True},
                                {"name": "column2", "type": "string", "nullable": True}
                            ]
                        }
                    }
                elif first_object.endswith('.json'):
                    return {
                        "s3_data": {
                            "columns": [
                                {"name": "key", "type": "string", "nullable": False},
                                {"name": "value", "type": "string", "nullable": True}
                            ]
                        }
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get schema from S3: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Amazon S3."""
        try:
            all_data = []
            
            for table in tables:
                # List objects in the bucket
                objects = self.client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=f"{self.prefix}{table}"
                )
                
                if 'Contents' not in objects:
                    continue
                
                # Process each file
                for obj in objects['Contents']:
                    # Get the object
                    response = self.client.get_object(Bucket=self.bucket_name, Key=obj['Key'])
                    
                    # Read the content
                    content = response['Body'].read().decode('utf-8')
                    
                    # Parse based on file extension
                    if obj['Key'].endswith('.csv'):
                        df = pd.read_csv(io.StringIO(content))
                    elif obj['Key'].endswith('.json'):
                        df = pd.read_json(io.StringIO(content))
                    else:
                        continue
                    
                    # Apply filters
                    if columns:
                        df = df[columns]
                    
                    if where_clause:
                        df = df.query(where_clause)
                    
                    if limit:
                        df = df.head(limit)
                    
                    all_data.append(df)
            
            # Combine all data
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from S3: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on S3 data."""
        # For S3, we'll just return all data
        return self.get_data(["s3_data"])

class OneLakeConnector(DataSourceConnector):
    """Connector for Microsoft OneLake."""
    
    def connect(self) -> bool:
        """Connect to Microsoft OneLake."""
        try:
            self.workspace_id = self.config.get('workspace_id')
            
            if not self.workspace_id:
                logger.error("OneLake workspace ID not provided")
                return False
            
            # Create Fabric client
            fabric_config = self.config.get('fabric_config', {})
            credential = DefaultAzureCredential()
            client = FabricClient(
                tenant_id=fabric_config.get('tenant_id'),
                client_id=fabric_config.get('client_id'),
                client_secret=fabric_config.get('client_secret')
            )
            
            # Create workspace client
            self.workspace_client = WorkspaceClient(
                client=client,
                workspace_id=self.workspace_id
            )
            
            # Test connection
            self.workspace_client.get()
            
            logger.info(f"Connected to OneLake workspace: {self.workspace_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to OneLake: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Microsoft OneLake."""
        self.workspace_client = None
        logger.info("Disconnected from OneLake")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the OneLake data."""
        try:
            # Get all items in the workspace
            items = self.workspace_client.list_items()
            
            schema = {}
            
            for item in items:
                if item.get('type') == 'Lakehouse':
                    # Get tables in the lakehouse
                    lakehouse_id = item.get('id')
                    lakehouse_client = ItemClient(
                        client=self.workspace_client.client,
                        workspace_id=self.workspace_id,
                        item_id=lakehouse_id
                    )
                    
                    tables = lakehouse_client.get_tables()
                    
                    schema[item.get('display_name')] = {
                        "columns": [
                            {
                                "name": table.get('name'),
                                "type": table.get('type'),
                                "nullable": True
                            }
                            for table in tables
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from OneLake: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Microsoft OneLake."""
        try:
            all_data = []
            
            # Get all items in the workspace
            items = self.workspace_client.list_items()
            
            for item in items:
                if item.get('type') == 'Lakehouse' and item.get('display_name') in tables:
                    # Get tables in the lakehouse
                    lakehouse_id = item.get('id')
                    lakehouse_client = ItemClient(
                        client=self.workspace_client.client,
                        workspace_id=self.workspace_id,
                        item_id=lakehouse_id
                    )
                    
                    # Get data
                    query = "SELECT * FROM " + item.get('display_name')
                    
                    if where_clause:
                        query += f" WHERE {where_clause}"
                    
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    result = lakehouse_client.execute_query(query)
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(result)
                    
                    # Select columns if specified
                    if columns:
                        df = df[columns]
                    
                    all_data.append(df)
            
            # Combine all data
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from OneLake: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on OneLake data."""
        try:
            # Get all items in the workspace
            items = self.workspace_client.list_items()
            
            for item in items:
                if item.get('type') == 'Lakehouse':
                    # Get tables in the lakehouse
                    lakehouse_id = item.get('id')
                    lakehouse_client = ItemClient(
                        client=self.workspace_client.client,
                        workspace_id=self.workspace_id,
                        item_id=lakehouse_id
                    )
                    
                    # Execute query
                    result = lakehouse_client.execute_query(query)
                    
                    # Convert to DataFrame
                    return pd.DataFrame(result)
            
            return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to execute query on OneLake: {str(e)}")
            return pd.DataFrame()


class OracleConnector(DataSourceConnector):
    """Connector for Oracle databases."""
    
    def connect(self) -> bool:
        """Connect to Oracle."""
        try:
            import cx_Oracle
            
            username = self.config.get('username')
            password = self.config.get('password')
            host = self.config.get('host')
            port = self.config.get('port', 1521)
            service_name = self.config.get('service_name')
            
            # Create DSN
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
            
            # Create connection pool
            self.connection_pool = cx_Oracle.SessionPool(
                user=username,
                password=password,
                dsn=dsn,
                min=self.config.get('connection_pool_size', 5),
                max=self.config.get('connection_pool_size', 5),
                increment=1,
                threaded=True
            )
            
            # Test connection
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.close()
            
            logger.info("Connected to Oracle")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Oracle."""
        if self.connection_pool:
            self.connection_pool.close()
            self.connection_pool = None
            logger.info("Disconnected from Oracle")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the Oracle database."""
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("""
                SELECT table_name 
                FROM user_tables
                """)
                tables = cursor.fetchall()
                
                schema = {}
                
                for (table_name,) in tables:
                    cursor.execute("""
                    SELECT column_name, data_type, nullable
                    FROM user_tab_columns
                    WHERE table_name = :table_name
                    ORDER BY column_id
                    """, table_name=table_name)
                    columns = cursor.fetchall()
                    
                    schema[table_name] = {
                        "columns": [
                            {
                                "name": column_name,
                                "type": data_type,
                                "nullable": nullable == 'Y'
                            }
                            for column_name, data_type, nullable in columns
                        ]
                    }
                
                cursor.close()
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Oracle: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Oracle."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" FETCH FIRST {limit} ROWS ONLY"
                
                # Execute query
                with self.connection_pool.acquire() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    data = cursor.fetchall()
                    
                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    all_data.append(df)
                    cursor.close()
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from Oracle: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Oracle."""
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                data = cursor.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                
                # Convert to DataFrame
                df = pd.DataFrame(data, columns=column_names)
                cursor.close()
                
                return df
                
        except Exception as e:
            logger.error(f"Failed to execute query on Oracle: {str(e)}")
            return pd.DataFrame()

class SnowflakeConnector(DataSourceConnector):
    """Connector for Snowflake."""
    
    def connect(self) -> bool:
        """Connect to Snowflake."""
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                user=self.config.get('username'),
                password=self.config.get('password'),
                account=self.config.get('account'),
                warehouse=self.config.get('warehouse'),
                database=self.config.get('database'),
                schema=self.config.get('schema')
            )
            
            # Test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            
            logger.info("Connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Snowflake."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from Snowflake")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the Snowflake database."""
        try:
            cursor = self.connection.cursor()
            
            # Get all tables
            cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            """)
            tables = cursor.fetchall()
            
            schema = {}
            
            for (table_name,) in tables:
                cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = :table_name
                ORDER BY ordinal_position
                """, table_name=table_name)
                columns = cursor.fetchall()
                
                schema[table_name] = {
                    "columns": [
                        {
                            "name": column_name,
                            "type": data_type,
                            "nullable": is_nullable == 'YES'
                        }
                        for column_name, data_type, is_nullable in columns
                    ]
                }
            
            cursor.close()
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Snowflake: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Snowflake."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                cursor = self.connection.cursor()
                cursor.execute(query)
                data = cursor.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                
                # Convert to DataFrame
                df = pd.DataFrame(data, columns=column_names)
                all_data.append(df)
                cursor.close()
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from Snowflake: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Snowflake."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=column_names)
            cursor.close()
            
            return df
                
        except Exception as e:
            logger.error(f"Failed to execute query on Snowflake: {str(e)}")
            return pd.DataFrame()

class SynapseConnector(DataSourceConnector):
    """Connector for Azure Synapse."""
    
    def connect(self) -> bool:
        """Connect to Azure Synapse."""
        try:
            connection_string = (
                f"mssql+pyodbc://{self.config.get('username')}:{self.config.get('password')}"
                f"@{self.config.get('server')}?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            self.connection_pool = create_engine(
                connection_string,
                pool_size=self.config.get('connection_pool_size', 5),
                pool_timeout=self.config.get('connection_timeout', 30)
            )
            
            # Test connection
            with self.connection_pool.connect() as conn:
                conn.execute("SELECT 1")
            
            logger.info("Connected to Azure Synapse")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Azure Synapse: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Azure Synapse."""
        if self.connection_pool:
            self.connection_pool.dispose()
            self.connection_pool = None
            logger.info("Disconnected from Azure Synapse")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the schema of the Azure Synapse database."""
        try:
            with self.connection_pool.connect() as conn:
                # Get all tables
                tables_query = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                """
                tables_result = conn.execute(tables_query)
                tables = tables_result.fetchall()
                
                schema = {}
                
                for table_schema, table_name in tables:
                    columns_query = """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = ? AND table_name = ?
                    ORDER BY ordinal_position
                    """
                    columns_result = conn.execute(columns_query, (table_schema, table_name))
                    columns = columns_result.fetchall()
                    
                    table_key = f"{table_schema}.{table_name}" if table_schema else table_name
                    schema[table_key] = {
                        "columns": [
                            {
                                "name": column_name,
                                "type": data_type,
                                "nullable": is_nullable == "YES"
                            }
                            for column_name, data_type, is_nullable in columns
                        ]
                    }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Azure Synapse: {str(e)}")
            return {}
    
    def get_data(self, tables: List[str], columns: Optional[List[str]] = None, 
                where_clause: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Get data from Azure Synapse."""
        try:
            all_data = []
            
            for table in tables:
                # Build query
                column_list = "*"
                if columns:
                    column_list = ", ".join(columns)
                
                query = f"SELECT {column_list} FROM {table}"
                
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                with self.connection_pool.connect() as conn:
                    result = conn.execute(query)
                    data = result.fetchall()
                    
                    # Get column names
                    column_names = [desc[0] for desc in result.cursor.description]
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data, columns=column_names)
                    all_data.append(df)
            
            # Combine all data
            if len(all_data) == 1:
                return all_data[0]
            elif len(all_data) > 1:
                # Merge dataframes on common columns
                result_df = all_data[0]
                for df in all_data[1:]:
                    # Find common columns
                    common_cols = list(set(result_df.columns) & set(df.columns))
                    if common_cols:
                        result_df = pd.merge(result_df, df, on=common_cols, how="inner")
                return result_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data from Azure Synapse: {str(e)}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a custom query on Azure Synapse."""
        try:
            with self.connection_pool.connect() as conn:
                result = conn.execute(query)
                data = result.fetchall()
                
                # Get column names
                column_names = [desc[0] for desc in result.cursor.description]
                
                # Convert to DataFrame
                return pd.DataFrame(data, columns=column_names)
                
        except Exception as e:
            logger.error(f"Failed to execute query on Azure Synapse: {str(e)}")
            return pd.DataFrame()

# Update the _initialize_data_source_connectors method in PowerBIReportGenerator
def _initialize_data_source_connectors(self):
    """Initialize data source connectors based on configuration."""
    for name, config in self.config.get('data_sources', {}).items():
        source_type = config.get('type')
        
        if source_type == 'sql_server':
            self.data_source_connectors[name] = SQLServerConnector(config)
        elif source_type == 'postgresql':
            self.data_source_connectors[name] = PostgreSQLConnector(config)
        elif source_type == 'mysql':
            self.data_source_connectors[name] = MySQLConnector(config)
        elif source_type == 'csv':
            self.data_source_connectors[name] = CSVConnector(config)
        elif source_type == 'excel':
            self.data_source_connectors[name] = ExcelConnector(config)
        elif source_type == 'api':
            self.data_source_connectors[name] = APIConnector(config)
        elif source_type == 'bigquery':
            self.data_source_connectors[name] = BigQueryConnector(config)
        elif source_type == 'salesforce':
            self.data_source_connectors[name] = SalesforceConnector(config)
        elif source_type == 's3':
            self.data_source_connectors[name] = S3Connector(config)
        elif source_type == 'onelake':
            # Pass fabric config to OneLake connector
            config['fabric_config'] = self.config.get('fabric', {})
            self.data_source_connectors[name] = OneLakeConnector(config)
        elif source_type == 'oracle':
            self.data_source_connectors[name] = OracleConnector(config)
        elif source_type == 'snowflake':
            self.data_source_connectors[name] = SnowflakeConnector(config)
        elif source_type == 'synapse':
            self.data_source_connectors[name] = SynapseConnector(config)
        else:
            logger.warning(f"Unsupported data source type: {source_type}")

# Update the deploy_to_fabric method in PowerBIReportGenerator to use deployment configuration
def deploy_to_fabric(self, report_config: Dict[str, Any]) -> Dict[str, Any]:
    """Deploy a report to Microsoft Fabric."""
    if not self.config.get('enable_fabric_deployment', False):
        return {"status": "error", "message": "Fabric deployment is not enabled"}
    
    try:
        fabric_config = self.config.get('fabric', {})
        deployment_config = self.config.get('deployment', {})
        
        # Check if approval is required
        if deployment_config.get('approval_required', False):
            # Create approval workflow
            draft_id = self.interactive_builder.create_report_draft({
                "title": report_config.get('title', 'Untitled Report'),
                "description": report_config.get('description', ''),
                "data_sources": report_config.get('data_sources', []),
                "created_by": "system"
            })
            
            # In a real scenario, this would wait for human approval
            # For now, we'll auto-approve
            self.interactive_builder.approve_report(draft_id, "system", "Auto-approved for deployment")
        
        # Create Fabric client
        credential = DefaultAzureCredential()
        client = FabricClient(
            tenant_id=fabric_config.get('tenant_id'),
            client_id=fabric_config.get('client_id'),
            client_secret=fabric_config.get('client_secret')
        )
        
        # Create workspace client
        workspace_client = WorkspaceClient(
            client=client,
            workspace_id=fabric_config.get('workspace_id')
        )
        
        # Create item client
        item_client = ItemClient(
            client=client,
            workspace_id=fabric_config.get('workspace_id')
        )
        
        # Create report definition
        report_definition = {
            "displayName": report_config.get('title', 'Untitled Report'),
            "description": report_config.get('description', ''),
            "type": "Report",
            "definition": {
                "parts": [
                    {
                        "path": "report.json",
                        "payload": json.dumps(report_config),
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        # Create report
        report = item_client.create_item(report_definition)
        
        # Update metrics
        FABRIC_DEPLOYMENT_COUNT.labels(status='success').inc()
        
        # Send notification to Teams if webhook is configured
        teams_webhook = self.config.get('integrations', {}).get('teams_webhook', '')
        if teams_webhook:
            try:
                message = {
                    "text": f"Report '{report_config.get('title', 'Untitled Report')}' has been deployed to Fabric successfully."
                }
                requests.post(teams_webhook, json=message)
            except Exception as e:
                logger.error(f"Failed to send Teams notification: {str(e)}")
        
        return {
            "status": "success",
            "report_id": report.get('id'),
            "report_name": report.get('displayName'),
            "workspace_id": fabric_config.get('workspace_id'),
            "message": "Report deployed to Fabric successfully"
        }
        
    except Exception as e:
        FABRIC_DEPLOYMENT_COUNT.labels(status='error').inc()
        logger.error(f"Error deploying report to Fabric: {str(e)}")
        return {"status": "error", "message": str(e)}

# === Main Power BI Report Generator Class ===
class PowerBIReportGenerator:
    """Main class for generating Power BI reports with enhanced features."""
    
    def __init__(self, config_path: str = "config.yaml"):
        # Load configuration
        self.config = load_config(config_path)
        
        # Initialize components
        self.cache_manager = CacheManager(
            redis_url=self.config.get('monitoring', {}).get('redis_url', 'redis://localhost:6379'),
            enable_lineage=self.config.get('monitoring', {}).get('data_lineage_tracking', False)
        )
        
        self.data_profiler = DataProfiler(self.config)
        self.schema_drift_detector = SchemaDriftDetector(self.cache_manager)
        self.python_sanitizer = PythonScriptSanitizer(self.config.get('sandbox_path', '/tmp/sandbox'))
        self.sql_validator = SQLQueryValidator()
        
        # Initialize enhanced features
        self.real_time_processor = RealTimeDataProcessor(self.config)
        self.interactive_builder = InteractiveReportBuilder(self.config)
        self.copilot = PowerBICopilot(self.config)
        self.advanced_analytics = AdvancedAnalyticsEngine(self.config)
        
        # Initialize auto-scaler
        self.autoscaler = AutoScaler(self.config)
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests=self.config.get('monitoring', {}).get('rate_limit_requests', 100),
            period=self.config.get('monitoring', {}).get('rate_limit_period', 60)
        )
        
        # Initialize data source connectors
        self.data_source_connectors = {}
        self._initialize_data_source_connectors()
        
        logger.info("Power BI Report Generator initialized with enhanced features")
    
    def _initialize_data_source_connectors(self):
        """Initialize data source connectors based on configuration."""
        for name, config in self.config.get('data_sources', {}).items():
            source_type = config.get('type')
            
            if source_type == 'sql_server':
                self.data_source_connectors[name] = SQLServerConnector(config)
            elif source_type == 'postgresql':
                self.data_source_connectors[name] = PostgreSQLConnector(config)
            elif source_type == 'mysql':
                self.data_source_connectors[name] = MySQLConnector(config)
            elif source_type == 'csv':
                self.data_source_connectors[name] = CSVConnector(config)
            elif source_type == 'excel':
                self.data_source_connectors[name] = ExcelConnector(config)
            elif source_type == 'api':
                self.data_source_connectors[name] = APIConnector(config)
            elif source_type == 'bigquery':
                self.data_source_connectors[name] = BigQueryConnector(config)
            elif source_type == 'salesforce':
                self.data_source_connectors[name] = SalesforceConnector(config)
            else:
                logger.warning(f"Unsupported data source type: {source_type}")
    
    def generate_report(self, requirements: str, data_source_name: str) -> Dict[str, Any]:
        """Generate a Power BI report based on requirements."""
        # Apply rate limiting
        if not self.rate_limiter.consume():
            return {"status": "error", "message": "Rate limit exceeded"}
        
        start_time = time.time()
        REQUEST_COUNT.labels(endpoint='generate_report', status='started').inc()
        
        try:
            # Get data source connector
            if data_source_name not in self.data_source_connectors:
                return {"status": "error", "message": f"Data source not found: {data_source_name}"}
            
            connector = self.data_source_connectors[data_source_name]
            
            # Connect to data source
            if not connector.connect():
                return {"status": "error", "message": f"Failed to connect to data source: {data_source_name}"}
            
            try:
                # Get schema
                schema = connector.get_schema()
                if not schema:
                    return {"status": "error", "message": f"Failed to get schema for data source: {data_source_name}"}
                
                # Detect schema drift
                drift_result = self.schema_drift_detector.detect_schema_drift(data_source_name, schema)
                if drift_result["drift_detected"]:
                    logger.warning(f"Schema drift detected for {data_source_name}: {drift_result['message']}")
                
                # Select tables
                tables = select_table_list(schema, requirements, True, self.config)
                if not tables:
                    return {"status": "error", "message": "No relevant tables found for the requirements"}
                
                # Get data
                data = connector.get_data(tables)
                if data is None or data.empty:
                    return {"status": "error", "message": "Failed to retrieve data"}
                
                # Profile data
                profile = self.data_profiler.profile_data(data, data_source_name)
                
                # Generate automated insights if enabled
                insights = []
                if self.config.get('advanced_analytics', {}).get('automated_insights', False):
                    insights_result = self.advanced_analytics.generate_automated_insights(data)
                    if insights_result["status"] == "success":
                        insights = insights_result["insights"]
                
                # Generate report using LangChain
                report_result = self._generate_report_with_langchain(
                    requirements, data, schema, tables, profile, insights
                )
                
                # Create interactive report if UI features are enabled
                interactive_report = None
                if self.config.get('ui', {}).get('interactive_dashboards', False):
                    # Create report draft
                    draft_id = self.interactive_builder.create_report_draft({
                        "title": f"Report for {data_source_name}",
                        "description": requirements,
                        "data_sources": [data_source_name],
                        "created_by": "system"
                    })
                    
                    # Create approval workflow
                    workflow = self.interactive_builder.create_approval_workflow(draft_id)
                    
                    # Approve the report (in a real scenario, this would be done by a human)
                    self.interactive_builder.approve_report(draft_id, "system", "Auto-approved")
                    
                    # Create interactive dashboard
                    interactive_report = self.interactive_builder.create_interactive_dashboard(draft_id)
                
                # Calculate duration
                duration = time.time() - start_time
                REQUEST_DURATION.labels(endpoint='generate_report').observe(duration)
                REQUEST_COUNT.labels(endpoint='generate_report', status='success').inc()
                
                return {
                    "status": "success",
                    "report": report_result,
                    "profile": profile,
                    "schema_drift": drift_result,
                    "insights": insights,
                    "interactive_report": interactive_report,
                    "duration": duration,
                    "timestamp": datetime.now().isoformat()
                }
                
            finally:
                # Disconnect from data source
                connector.disconnect()
                
        except Exception as e:
            duration = time.time() - start_time
            REQUEST_DURATION.labels(endpoint='generate_report').observe(duration)
            REQUEST_COUNT.labels(endpoint='generate_report', status='error').inc()
            ERROR_COUNT.labels(type='report_generation').inc()
            
            logger.error(f"Error generating report: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _generate_report_with_langchain(self, requirements: str, data: pd.DataFrame, 
                                      schema: Dict[str, Any], tables: List[str], 
                                      profile: Dict[str, Any], insights: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a Power BI report using LangChain."""
        try:
            openai_config = self.config.get('openai', {})
            api_key = openai_config.get('api_key')
            
            if not api_key:
                return {"status": "error", "message": "OpenAI API key not found"}
            
            # Initialize OpenAI client
            if openai_config.get('use_azure', False):
                llm = AzureChatOpenAI(
                    azure_deployment=openai_config.get('model', 'gpt-4'),
                    azure_endpoint=openai_config.get('azure_endpoint'),
                    api_version=openai_config.get('azure_version', '2023-12-01-preview'),
                    api_key=api_key,
                    temperature=0.3
                )
            else:
                llm = ChatOpenAI(
                    api_key=api_key,
                    model=openai_config.get('model', 'gpt-4'),
                    temperature=0.3
                )
            
            # Prepare data summary
            data_summary = {
                "shape": data.shape,
                "columns": list(data.columns),
                "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()},
                "sample_data": data.head(5).to_dict(),
                "summary_stats": data.describe().to_dict() if not data.empty else {}
            }
            
            # Create prompt
            system_prompt = SystemMessage(
                content="You are an expert Power BI report developer. Generate a Power BI report based on the provided requirements, data, and insights."
            )
            
            human_prompt = HumanMessage(
                content=f"""
Requirements:
{requirements}

Data Summary:
{json.dumps(data_summary, indent=2)}

Schema:
{json.dumps(schema, indent=2)}

Selected Tables:
{', '.join(tables)}

Data Profile:
{json.dumps(profile, indent=2)}

Insights:
{json.dumps(insights, indent=2)}

Generate a Power BI report with the following structure:
1. Report title and description
2. Pages with visuals
3. DAX measures if needed
4. Data relationships if needed

Return the report as a JSON object with the following structure:
{
  "title": "Report Title",
  "description": "Report Description",
  "pages": [
    {
      "name": "Page Name",
      "visuals": [
        {
          "type": "visual_type",
          "title": "Visual Title",
          "dataFields": ["field1", "field2"],
          "aggregation": "sum|avg|count|min|max"
        }
      ]
    }
  ],
  "measures": {
    "measure_name": "DAX expression"
  },
  "relationships": [
    {
      "from": "table1.field1",
      "to": "table2.field2",
      "cardinality": "one-to-many|many-to-one|one-to-one|many-to-many"
    }
  ]
}
"""
            )
            
            # Generate response
            response = llm.invoke([system_prompt, human_prompt])
            response_text = response.content.strip()
            
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                report = json.loads(json_match.group())
                
                # Validate report structure
                if "title" in report and "pages" in report:
                    return report
            
            # Fallback: create a basic report
            return {
                "title": f"Report for {', '.join(tables)}",
                "description": requirements,
                "pages": [
                    {
                        "name": "Summary",
                        "visuals": [
                            {
                                "type": "table",
                                "title": "Data Summary",
                                "dataFields": list(data.columns)
                            }
                        ]
                    }
                ],
                "measures": {},
                "relationships": []
            }
            
        except Exception as e:
            logger.error(f"Error generating report with LangChain: {str(e)}")
            return {
                "title": f"Report for {', '.join(tables)}",
                "description": requirements,
                "pages": [
                    {
                        "name": "Summary",
                        "visuals": [
                            {
                                "type": "table",
                                "title": "Data Summary",
                                "dataFields": list(data.columns)
                            }
                        ]
                    }
                ],
                "measures": {},
                "relationships": []
            }
    
    def deploy_to_fabric(self, report_config: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy a report to Microsoft Fabric."""
        if not self.config.get('enable_fabric_deployment', False):
            return {"status": "error", "message": "Fabric deployment is not enabled"}
        
        try:
            fabric_config = self.config.get('fabric', {})
            
            # Create Fabric client
            credential = DefaultAzureCredential()
            client = FabricClient(
                tenant_id=fabric_config.get('tenant_id'),
                client_id=fabric_config.get('client_id'),
                client_secret=fabric_config.get('client_secret')
            )
            
            # Create workspace client
            workspace_client = WorkspaceClient(
                client=client,
                workspace_id=fabric_config.get('workspace_id')
            )
            
            # Create item client
            item_client = ItemClient(
                client=client,
                workspace_id=fabric_config.get('workspace_id')
            )
            
            # Create report definition
            report_definition = {
                "displayName": report_config.get('title', 'Untitled Report'),
                "description": report_config.get('description', ''),
                "type": "Report",
                "definition": {
                    "parts": [
                        {
                            "path": "report.json",
                            "payload": json.dumps(report_config),
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            # Create report
            report = item_client.create_item(report_definition)
            
            # Update metrics
            FABRIC_DEPLOYMENT_COUNT.labels(status='success').inc()
            
            return {
                "status": "success",
                "report_id": report.get('id'),
                "report_name": report.get('displayName'),
                "workspace_id": fabric_config.get('workspace_id'),
                "message": "Report deployed to Fabric successfully"
            }
            
        except Exception as e:
            FABRIC_DEPLOYMENT_COUNT.labels(status='error').inc()
            logger.error(f"Error deploying report to Fabric: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def train_predictive_model(self, data_source_name: str, target_column: str, model_type: str = 'regression') -> Dict[str, Any]:
        """Train a predictive model using data from a data source."""
        try:
            # Get data source connector
            if data_source_name not in self.data_source_connectors:
                return {"status": "error", "message": f"Data source not found: {data_source_name}"}
            
            connector = self.data_source_connectors[data_source_name]
            
            # Connect to data source
            if not connector.connect():
                return {"status": "error", "message": f"Failed to connect to data source: {data_source_name}"}
            
            try:
                # Get schema
                schema = connector.get_schema()
                if not schema:
                    return {"status": "error", "message": f"Failed to get schema for data source: {data_source_name}"}
                
                # Select tables
                tables = list(schema.keys())
                
                # Get data
                data = connector.get_data(tables)
                if data is None or data.empty:
                    return {"status": "error", "message": "Failed to retrieve data"}
                
                # Train model
                return self.advanced_analytics.train_predictive_model(data, target_column, model_type)
                
            finally:
                # Disconnect from data source
                connector.disconnect()
                
        except Exception as e:
            logger.error(f"Error training predictive model: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def predict_with_model(self, model_id: str, data_source_name: str, input_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Make predictions using a trained model."""
        try:
            # Get data source connector
            if data_source_name not in self.data_source_connectors:
                return {"status": "error", "message": f"Data source not found: {data_source_name}"}
            
            connector = self.data_source_connectors[data_source_name]
            
            # Connect to data source
            if not connector.connect():
                return {"status": "error", "message": f"Failed to connect to data source: {data_source_name}"}
            
            try:
                # Get schema
                schema = connector.get_schema()
                if not schema:
                    return {"status": "error", "message": f"Failed to get schema for data source: {data_source_name}"}
                
                # Select tables
                tables = list(schema.keys())
                
                # Get data if not provided
                if input_data is None:
                    input_data = connector.get_data(tables)
                    if input_data is None or input_data.empty:
                        return {"status": "error", "message": "Failed to retrieve data"}
                
                # Make predictions
                return self.advanced_analytics.predict_with_model(model_id, input_data)
                
            finally:
                # Disconnect from data source
                connector.disconnect()
                
        except Exception as e:
            logger.error(f"Error making predictions: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def natural_language_to_sql(self, question: str, data_source_name: str) -> Dict[str, Any]:
        """Convert natural language question to SQL query."""
        try:
            # Get data source connector
            if data_source_name not in self.data_source_connectors:
                return {"status": "error", "message": f"Data source not found: {data_source_name}"}
            
            connector = self.data_source_connectors[data_source_name]
            
            # Connect to data source
            if not connector.connect():
                return {"status": "error", "message": f"Failed to connect to data source: {data_source_name}"}
            
            try:
                # Get schema
                schema = connector.get_schema()
                if not schema:
                    return {"status": "error", "message": f"Failed to get schema for data source: {data_source_name}"}
                
                # Convert natural language to SQL
                return self.advanced_analytics.natural_language_to_sql(question, schema)
                
            finally:
                # Disconnect from data source
                connector.disconnect()
                
        except Exception as e:
            logger.error(f"Error converting natural language to SQL: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def generate_insights(self, data_source_name: str, question: str) -> Dict[str, Any]:
        """Generate insights from data using Copilot."""
        try:
            # Get data source connector
            if data_source_name not in self.data_source_connectors:
                return {"status": "error", "message": f"Data source not found: {data_source_name}"}
            
            connector = self.data_source_connectors[data_source_name]
            
            # Connect to data source
            if not connector.connect():
                return {"status": "error", "message": f"Failed to connect to data source: {data_source_name}"}
            
            try:
                # Get schema
                schema = connector.get_schema()
                if not schema:
                    return {"status": "error", "message": f"Failed to get schema for data source: {data_source_name}"}
                
                # Select tables
                tables = list(schema.keys())
                
                # Get data
                data = connector.get_data(tables)
                if data is None or data.empty:
                    return {"status": "error", "message": "Failed to retrieve data"}
                
                # Generate insights
                return self.copilot.generate_insights(data, question)
                
            finally:
                # Disconnect from data source
                connector.disconnect()
                
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def process_streaming_data(self, stream_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process streaming data."""
        return self.real_time_processor.process_streaming_data(stream_config)
    
    def publish_to_stream(self, data: Dict[str, Any], source: str = 'kafka') -> Dict[str, Any]:
        """Publish data to a streaming source."""
        return self.real_time_processor.publish_to_stream(data, source)
    
    def create_report_draft(self, report_config: Dict[str, Any]) -> str:
        """Create a new report draft for review."""
        return self.interactive_builder.create_report_draft(report_config)
    
    def submit_feedback(self, report_draft_id: str, feedback: Dict[str, Any]) -> Dict[str, Any]:
        """Submit feedback for a report draft."""
        return self.interactive_builder.submit_feedback(report_draft_id, feedback)
    
    def approve_report(self, report_draft_id: str, approver: str, comments: str = "") -> Dict[str, Any]:
        """Approve a report draft."""
        return self.interactive_builder.approve_report(report_draft_id, approver, comments)
    
    def reject_report(self, report_draft_id: str, rejector: str, reason: str = "") -> Dict[str, Any]:
        """Reject a report draft."""
        return self.interactive_builder.reject_report(report_draft_id, rejector, reason)
    
    def create_interactive_dashboard(self, report_id: str) -> Dict[str, Any]:
        """Create an interactive dashboard from a report."""
        return self.interactive_builder.create_interactive_dashboard(report_id)
    
    def write_back_data(self, dataset_id: str, updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Write data back to a dataset."""
        return self.copilot.write_back_data(dataset_id, updates)
    
    def create_data_agent(self, agent_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a data agent for automated analysis."""
        return self.copilot.create_data_agent(agent_config)
    
    def generate_dax_measures(self, table_name: str, columns: List[str], requirements: str) -> Dict[str, Any]:
        """Generate DAX measures based on requirements."""
        return self.copilot.generate_dax_measures(table_name, columns, requirements)
    
    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource utilization status."""
        return self.autoscaler.get_resource_status()
    
    def shutdown(self):
        """Shutdown the report generator and release resources."""
        self.autoscaler.shutdown()
        self.real_time_processor.stop_processing()
        
        # Disconnect all data source connectors
        for connector in self.data_source_connectors.values():
            try:
                connector.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting data source: {str(e)}")
        
        logger.info("Power BI Report Generator shutdown complete")
# In generate_report_v3.py, enhance logging configuration
def setup_enhanced_logging():
    """Set up enhanced logging with structured output."""
    import structlog
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Create loggers
    logger = structlog.get_logger("powerbi_generator")
    
    # Add file handler with rotation
    from logging.handlers import RotatingFileHandler
    
    file_handler = RotatingFileHandler(
        'powerbi_generator.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(logging.Formatter('%(message)s'))
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )
    
    return logger

# Replace all logger instances with structured logger
logger = setup_enhanced_logging()
# === FastAPI App (if enabled) ===
def create_app(config: Dict[str, Any]) -> FastAPI:
    """Create FastAPI app if API is enabled."""
    if not config.get('api', {}).get('enabled', False):
        return None
    
    app = FastAPI(
        title="Power BI Report Generator API",
        description="API for generating Power BI reports with enhanced features",
        version="1.0.0"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.get('api', {}).get('cors_origins', ["*"]),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Initialize report generator
    report_generator = PowerBIReportGenerator()
    
    @app.get("/")
    async def root():
        return {"message": "Power BI Report Generator API"}
    
    @app.post("/generate-report")
    async def generate_report_endpoint(
        requirements: str,
        data_source_name: str
    ):
        """Generate a Power BI report."""
        return report_generator.generate_report(requirements, data_source_name)
    
    @app.post("/deploy-to-fabric")
    async def deploy_to_fabric_endpoint(
        report_config: Dict[str, Any]
    ):
        """Deploy a report to Microsoft Fabric."""
        return report_generator.deploy_to_fabric(report_config)
    
    @app.post("/train-model")
    async def train_model_endpoint(
        data_source_name: str,
        target_column: str,
        model_type: str = "regression"
    ):
        """Train a predictive model."""
        return report_generator.train_predictive_model(data_source_name, target_column, model_type)
    
    @app.post("/predict")
    async def predict_endpoint(
        model_id: str,
        data_source_name: str,
        input_data: Optional[pd.DataFrame] = None
    ):
        """Make predictions using a trained model."""
        return report_generator.predict_with_model(model_id, data_source_name, input_data)
    
    @app.post("/natural-language-to-sql")
    async def natural_language_to_sql_endpoint(
        question: str,
        data_source_name: str
    ):
        """Convert natural language question to SQL query."""
        return report_generator.natural_language_to_sql(question, data_source_name)
    
    @app.post("/generate-insights")
    async def generate_insights_endpoint(
        data_source_name: str,
        question: str
    ):
        """Generate insights from data using Copilot."""
        return report_generator.generate_insights(data_source_name, question)
    
    @app.post("/process-streaming-data")
    async def process_streaming_data_endpoint(
        stream_config: Dict[str, Any]
    ):
        """Process streaming data."""
        return report_generator.process_streaming_data(stream_config)
    
    @app.post("/publish-to-stream")
    async def publish_to_stream_endpoint(
        data: Dict[str, Any],
        source: str = "kafka"
    ):
        """Publish data to a streaming source."""
        return report_generator.publish_to_stream(data, source)
    
    @app.post("/create-report-draft")
    async def create_report_draft_endpoint(
        report_config: Dict[str, Any]
    ):
        """Create a new report draft for review."""
        draft_id = report_generator.create_report_draft(report_config)
        return {"draft_id": draft_id}
    
    @app.post("/submit-feedback")
    async def submit_feedback_endpoint(
        report_draft_id: str,
        feedback: Dict[str, Any]
    ):
        """Submit feedback for a report draft."""
        return report_generator.submit_feedback(report_draft_id, feedback)
    
    @app.post("/approve-report")
    async def approve_report_endpoint(
        report_draft_id: str,
        approver: str,
        comments: str = ""
    ):
        """Approve a report draft."""
        return report_generator.approve_report(report_draft_id, approver, comments)
    
    @app.post("/reject-report")
    async def reject_report_endpoint(
        report_draft_id: str,
        rejector: str,
        reason: str = ""
    ):
        """Reject a report draft."""
        return report_generator.reject_report(report_draft_id, rejector, reason)
    
    @app.post("/create-interactive-dashboard")
    async def create_interactive_dashboard_endpoint(
        report_id: str
    ):
        """Create an interactive dashboard from a report."""
        return report_generator.create_interactive_dashboard(report_id)
    
    @app.post("/write-back-data")
    async def write_back_data_endpoint(
        dataset_id: str,
        updates: List[Dict[str, Any]]
    ):
        """Write data back to a dataset."""
        return report_generator.write_back_data(dataset_id, updates)
    
    @app.post("/create-data-agent")
    async def create_data_agent_endpoint(
        agent_config: Dict[str, Any]
    ):
        """Create a data agent for automated analysis."""
        return report_generator.create_data_agent(agent_config)
    
    @app.post("/generate-dax-measures")
    async def generate_dax_measures_endpoint(
        table_name: str,
        columns: List[str],
        requirements: str
    ):
        """Generate DAX measures based on requirements."""
        return report_generator.generate_dax_measures(table_name, columns, requirements)
    
    @app.get("/resource-status")
    async def resource_status_endpoint():
        """Get current resource utilization status."""
        return report_generator.get_resource_status()
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """Shutdown the report generator when the app shuts down."""
        report_generator.shutdown()
    
    return app

# === Main Execution ===
if __name__ == "__main__":
    # Load configuration
    config = load_config()
    
    # Start metrics server
    start_metrics_server()
    
    # Create FastAPI app if enabled
    app = create_app(config)
    
    if app:
        # Run the API server
        import uvicorn
        uvicorn.run(
            app,
            host=config.get('api', {}).get('host', '0.0.0.0'),
            port=config.get('api', {}).get('port', 8000)
        )
    else:
        # Initialize the report generator
        report_generator = PowerBIReportGenerator()
        
        # Example usage
        requirements = "Generate a sales report showing total sales by product category and customer country"
        data_source_name = "SalesDB"
        
        # Generate report
        result = report_generator.generate_report(requirements, data_source_name)
        
        # Print result
        print(json.dumps(result, indent=2))
        
        # Shutdown
        report_generator.shutdown()


from workflow_orchestrator import PowerBIWorkflowOrchestrator

class EnhancedPowerBIReportGenerator(PowerBIReportGenerator):
    def __init__(self, config_path: str = "config.yaml"):
        super().__init__(config_path)
        # Add agentic workflow capabilities
        self.workflow_orchestrator = PowerBIWorkflowOrchestrator(self.config)
    
    async def generate_report_with_agents(self, requirements: str, data_source_name: str):
        """Enhanced report generation using AI agents."""
        data_source_configs = [self.config['data_sources'][data_source_name]]
        
        return await self.workflow_orchestrator.execute_powerbi_development(
            requirements=requirements,
            data_source_configs=data_source_configs,
            workflow_type="main_powerbi_development",
            output_path="./output"
        )
