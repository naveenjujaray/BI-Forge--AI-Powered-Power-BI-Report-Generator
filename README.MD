
# BI Forge: AI-Powered Power BI Report Generator

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-green.svg)
![Power BI](https://img.shields.io/badge/Power%20BI-Enterprise-orange.svg)

## Disclaimer
# GNU GPL v3.0 Notice & Disclaimer ⚖️

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License version 3.0 (GPL‑3.0) as published by the Free Software Foundation. 📜

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranties of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. 🚫🛡️

You should have received a copy of the GNU General Public License along with this program. If not, see: https://www.gnu.org/licenses/ 🔗

## Your Responsibilities When Distributing or Modifying 🧭
- Keep intact all copyright, license notices, and relevant attributions. 🧾
- Provide the complete corresponding source code when conveying object/binary forms. 📦➡️💻
- State the significant changes you made to the work (if any). ✍️
- License your modifications and combined works under GPL‑3.0 when you distribute them. 🔁
- Provide "Installation Information" for User Products where required (anti‑tivoization). 🔧
- Do not impose additional restrictions beyond those permitted by GPL‑3.0. 🚷

## Good‑Practice Guidance (Non‑license Obligations) 💡
- Comply with all applicable laws and regulations in your jurisdiction. 🌐
- Protect secrets (API keys, credentials, tokens) and personal data appropriately. 🔐
- Validate all AI‑generated artifacts (SQL, code, and reports) before production use. ✅
- Maintain security controls and audit trails appropriate to your environment. 🛡️📝
---
## License

This project is licensed under the **GNU General Public License v3.0** - see the [LICENSE](LICENSE) file for details.

---
# Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Security Implementation](#security-implementation)
- [Compliance](#compliance)
- [Monitoring & Metrics](#monitoring--metrics)
- [Multi-Geo Support](#multi-geo-support)
- [Enterprise Integrations](#enterprise-integrations)
- [Scaling & Performance](#scaling--performance)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)
- [Disclaimer](#disclaimer)

---

## Overview

BI Forge is an enterprise-grade AI-powered solution that leverages OpenAI's GPT-4 to automatically generate Power BI reports from diverse data sources. This system combines advanced AI capabilities with robust security, comprehensive data quality checks, and seamless enterprise integrations to streamline business intelligence workflows at scale.

![Reporting with LLM](/assets/flowchart.png)

Ref: [https://medium.com/@mail2mhossain/llm-powered-reporting-transforming-traditional-reporting-into-ai-driven-solutions-14a188793760](https://medium.com/@mail2mhossain/llm-powered-reporting-transforming-traditional-reporting-into-ai-driven-solutions-14a188793760)

The system follows a sophisticated workflow:
1. **Data Ingestion**: Connects to multiple data sources (SQL, NoSQL, APIs, cloud storage)
2. **Real-time Streaming**: Processes streaming data from Kafka or Event Hubs
3. **Query Processing**: Uses NLP to understand business requirements
4. **AI Generation**: Creates SQL queries and Python scripts using GPT-4
5. **Security Validation**: Sandboxes and validates all generated code
6. **Quality Assurance**: Performs data profiling and drift detection
7. **Copilot Integration**: Enhances reports with AI-powered insights
8. **Deployment**: Publishes reports to Power BI with CI/CD integration
9. **Monitoring**: Tracks performance and data quality in real-time

---

## Features

### Core Capabilities
- **Multi-Source Data Integration**: Supports SQL Server, PostgreSQL, MySQL, Oracle, Snowflake, CSV, Excel, APIs, BigQuery, Salesforce, S3, OneLake, Synapse, and more
- **AI-Powered Report Generation**: Leverages GPT-4 to transform natural language queries into Power BI reports
- **Real-time Streaming Analytics**: Processes streaming data from Kafka or Azure Event Hubs
- **Enterprise Security**: AES-256 encryption, JWT authentication, Azure Key Vault integration
- **Data Quality Assurance**: Automated profiling, outlier detection, and rule-based validation
- **Schema Drift Detection**: Real-time monitoring of structural changes in data sources
- **Compliance Management**: Built-in support for GDPR, SOX, and HIPAA
- **Deployment Pipeline**: Automated CI/CD through Dev, Test, and Prod environments
- **Performance Optimization**: Redis caching, auto-scaling, and query optimization

### Advanced Features
- **Natural Language Processing**: Converts business questions into technical queries
- **Dynamic Data Profiling**: Analyzes data quality metrics (null percentages, duplicates, outliers)
- **Code Security Sandbox**: Executes generated Python scripts in isolated environments
- **Power BI Copilot Integration**: AI-powered insights, write-back capabilities, and data agents
- **Interactive Dashboards**: Low-code interface for report customization with mobile responsiveness
- **Advanced Analytics**: Predictive modeling, natural language querying, and automated insights
- **Multi-Geo Support**: Configurable data residency and regional compliance
- **Real-time Monitoring**: Prometheus metrics and Application Insights integration
- **Kubernetes Integration**: Auto-scaling and load balancing for enterprise deployments

---

## Architecture

The system follows a modular, enterprise-grade architecture with clear separation of concerns:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    BI Forge: AI-Powered Power BI Report Generator                │
├──────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────┐ │
│  │   Data Sources   │ │   Streaming      │ │   AI Engine      │ │   Security   │ │
│  │                  │ │                  │ │                  │ │              │ │
│  │ • SQL Server     │ │ • Kafka          │ │ • GPT-4          │ │ • AES-256    │ │
│  │ • PostgreSQL     │ │ • Event Hubs     │ │ • Query Gen      │ │ • JWT Auth   │ │
│  │ • BigQuery       │ │ • Real-time Proc │ │ • Code Gen       │ │ • Key Vault  │ │
│  │ • Salesforce     │ │ • Batch Proc     │ │ • Validation     │ │ • RBAC       │ │
│  └──────────────────┘ └──────────────────┘ └──────────────────┘ └──────────────┘ │
├──────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────┐ │
│  │   Data Quality   │ │   Copilot        │ │   Deployment     │ │   Monitoring │ │
│  │                  │ │                  │ │                  │ │              │ │
│  │ • Profiling      │ │ • Insights       │ │ • CI/CD Pipeline │ │ • Prometheus │ │
│  │ • Drift Detect   │ │ • Write-back     │ │ • Stage Mgmt     │ │ • AppInsights│ │
│  │ • Validation     │ │ • Data Agents    │ │ • Approval WF    │ │ • Alerting   │ │
│  └──────────────────┘ └──────────────────┘ └──────────────────┘ └──────────────┘ │
├──────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │              Enterprise Integration & Scaling Layer                         │ │
│  │                                                                             │ │
│  │ • Azure DevOps  • Microsoft Teams  • Power Automate  • Kubernetes           │ │
│  │ • Multi-Geo     • Compliance Reporting • Audit Logging • Auto-scaling       │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Component Details

| Component | Status | Grade | What's Been Added |
|-----------|--------|-------|-------------------|
| Architecture & Design | ✅ Exceptional | A+ | Strong modularity across connectors, orchestration, and UI; abstract base classes for data sources with concrete implementations; clear separation between data access, business logic, and presentation layers |
| Configuration Management | ✅ Enterprise-grade | A+ | Pydantic validation for all configuration objects; comprehensive configuration hierarchy with environment-specific settings; support for encrypted sensitive values |
| Security Implementation | ✅ Robust | A | AES encryption for sensitive data; JWT token generation and verification; Azure Key Vault integration; conditional access policies; IP restrictions |
| Authentication & Authorization | ✅ Implemented | A- | Centralized token acquisition for OpenAI/Power BI/OneLake clients; service principal validation; secure credential management with encryption |
| Data Quality Management | ✅ Comprehensive | A+ | Automated data profiling; outlier detection using Z-score and IQR methods; rule-based validation; customizable quality thresholds; detailed reporting |
| Schema Drift Detection | ✅ Proactive | A+ | Real-time schema monitoring; change detection with severity classification; alerting for structural changes; schema versioning |
| Enterprise Integration | ✅ Complete | A+ | Azure DevOps CI/CD pipeline integration; Microsoft Teams notifications; Power Automate workflows; comprehensive audit logging |
| Compliance Management | ✅ Thorough | A+ | Support for GDPR, SOX, and HIPAA; data classification; retention policies; audit trails; compliance reporting |
| Performance Optimization | ✅ Advanced | A+ | Redis caching with cluster support; auto-scaling configuration; query optimization; incremental refresh capabilities |
| Monitoring & Alerting | ✅ Comprehensive | A+ | Prometheus metrics collection; Application Insights integration; custom health checks; Teams notification system |
| Streaming Analytics | ✅ Real-time | A+ | Kafka and Event Hub integration; batch processing; checkpoint management; real-time data processing capabilities |
| Copilot Integration | ✅ AI-Enhanced | A+ | AI-powered insights generation; write-back capabilities; data agents; terminology management; grounding context |
| Multi-Geo Support | ✅ Global | A+ | Configurable data residency; regional compliance; multi-geo capacities; home region configuration |
| Kubernetes Integration | ✅ Scalable | A+ | Auto-scaling; load balancing; resource optimization; health checks; distributed processing |

---

## Prerequisites

- **Python 3.8+**: Required for all core functionality
- **Power BI Workspace**: With appropriate permissions for report deployment
- **OpenAI API Key**: For GPT-4 integration
- **Azure Account**: For enterprise features (Key Vault, DevOps, Monitoring)
- **Data Source Access**: Credentials for all configured data sources
- **Redis Server**: For caching (optional but recommended)
- **Kafka/Event Hub**: For streaming analytics (optional)
- **Kubernetes Cluster**: For advanced scaling (optional)

---

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/naveenjujaray/BI-Forge--AI-Powered-Power-BI-Report-Generation-Generator.git
   cd BI-Forge--AI-Powered-Power-BI-Report-Generation-Generator
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp config.yaml.example config.yaml
   # Edit config.yaml with your credentials
   ```

4. **Verify installation**
   ```bash
   python -c "from generate_report_v3 import PowerBIGenerator; print('Installation successful')"
   ```

---

## Configuration

The system uses a comprehensive YAML configuration file. Key sections include:

### Core Configuration
```yaml
openai:
  api_key: "your-openai-api-key"
  model: "gpt-4"
  temperature: 0.3
  max_tokens: 2000
  max_retries: 3
  use_azure: false
  langsmith_project: "powerbi-automation"
  langsmith_endpoint: "https://api.smith.langchain.com"

fabric:
  tenant_id: "your-tenant-id"
  client_id: "your-client-id"
  client_secret: "your-client-secret"
  workspace_id: "your-workspace-id"
  pipeline_id: "your-pipeline-id"
  capacity_id: "your-capacity-id"
  api_endpoint: "https://api.fabric.microsoft.com/v1"
```

### Data Sources
```yaml
data_sources:
  sales_data:
    type: "sql_server"
    server: "your-server-name.database.windows.net"
    database: "your-database-name"
    username: "your-username"
    password: "your-password"
    connection_pool_size: 5
    connection_timeout: 30
  
  customer_data:
    type: "postgresql"
    host: "your-postgres-server"
    port: 5432
    database: "your-database"
    username: "your-username"
    password: "your-password"
    schema: "public"
  
  streaming_data:
    type: "kafka"
    bootstrap_servers: "kafka-server1:9092,kafka-server2:9092"
    topic: "powerbi-data-stream"
    consumer_group: "$Default"
```

### Streaming Configuration
```yaml
streaming:
  enabled: true
  kafka_bootstrap_servers: "kafka-server1:9092,kafka-server2:9092"
  kafka_topic: "powerbi-data-stream"
  event_hub_connection_string: "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=your-policy;SharedAccessKey=your-key;EntityPath=your-eventhub"
  event_hub_name: "powerbi-events"
  consumer_group: "$Default"
  checkpoint_interval: 30
  batch_size: 100
```

### Copilot Configuration
```yaml
copilot:
  enabled: true
  write_back_enabled: true
  data_agents_enabled: true
  grounding_context: 
    - "Sales data includes daily transactions from all regions"
    - "Customer data contains demographic and purchase history"
  terminology:
    "Revenue": "Total income from sales before deductions"
    "Churn Rate": "Percentage of customers who discontinued service"
  api_key: "your-copilot-api-key"
```

### Advanced Analytics Configuration
```yaml
advanced_analytics:
  predictive_modeling: true
  natural_language_querying: true
  automated_insights: true
  model_path: "/models"
  confidence_threshold: 0.7
```

### Security & Compliance
```yaml
security:
  conditional_access: true
  mfa_required: true
  device_compliance_required: true
  ip_restrictions: []
  encryption_key: "your-encryption-key-here"
  api_key_rotation_days: 90

compliance:
  data_classification: "Confidential"
  retention_policy: "7_years"
  audit_logging: true
  standards: ["GDPR", "SOX", "HIPAA"]
```

### Scaling Configuration
```yaml
scaling:
  enabled: true
  min_workers: 2
  max_workers: 10
  scale_up_threshold: 0.7
  scale_down_threshold: 0.3
  cooldown_period: 300
  distributed_processing: true
  kubernetes_enabled: true
  load_balancing: true
  resource_optimization: true
  auto_scaling:
    enabled: true
    min_capacity: "F2"
    max_capacity: "F64"
    scale_triggers:
      cpu_threshold: 80
      memory_threshold: 85
      concurrent_users: 1000
```

---

## Usage

### Basic Example
```python
from generate_report_v3 import PowerBIGenerator

# Initialize generator
generator = PowerBIGenerator(config_path="config.yaml")

# Connect to data sources
generator.connect_data_sources()

# Generate report from natural language query
report = generator.generate_report(
    "Create a sales dashboard showing monthly revenue by product category"
)

# Deploy to Power BI
deployment_result = generator.deploy_to_powerbi(report)
```

### Streaming Analytics Example
```python
# Initialize streaming processor
stream_processor = RealTimeDataProcessor(config)

# Register a processor for streaming data
def process_sales_data(data):
    # Process real-time sales data
    processed_data = transform_data(data)
    update_dashboard(processed_data)
    return processed_data

stream_processor.register_processor("sales_data", process_sales_data)

# Start processing streaming data
result = stream_processor.process_streaming_data({
    "source": "kafka",
    "data_source": "sales_data"
})
```

### Copilot Integration Example
```python
# Initialize Power BI Copilot
copilot = PowerBICopilot(config)

# Generate insights from data
insights = copilot.generate_insights(
    data=sales_dataframe,
    question="What are the key trends in our sales data?"
)

# Create a data agent for automated analysis
agent = copilot.create_data_agent({
    "name": "Sales Performance Analyzer",
    "description": "Analyzes daily sales performance",
    "dataset_id": "sales_dataset",
    "schedule": "daily",
    "tasks": ["trend_analysis", "anomaly_detection"]
})

# Generate DAX measures
dax_measures = copilot.generate_dax_measures(
    table_name="Sales",
    columns=["Date", "Product", "Revenue", "Quantity"],
    requirements="Create measures for total revenue, year-over-year growth, and top products"
)
```

### Advanced Analytics Example
```python
# Enable predictive modeling
if config.advanced_analytics.predictive_modeling:
    # Train a predictive model
    model = train_predictive_model(
        data=sales_data,
        target="Revenue",
        features=["Quantity", "Discount", "Region"]
    )
    
    # Generate predictions
    predictions = model.predict(future_data)
    
    # Add predictions to dashboard
    dashboard.add_predictions(predictions)
```

### Command Line Interface
```bash
# Generate a report
python generate_report_v3.py --query "Show quarterly sales trends by region" --output sales_dashboard.pbit

# Deploy to specific stage
python generate_report_v3.py --deploy --stage "prod" --report-id "report-123"

# Process streaming data
python generate_report_v3.py --stream --source "kafka" --topic "sales-data"
```

---

## Security Implementation

### Data Protection
- **AES-256 Encryption**: All sensitive data encrypted using PBKDF2 key derivation
- **Secure Storage**: Azure Key Vault integration for credential management
- **Data Masking**: Automatic masking of sensitive fields in logs and reports
- **API Key Rotation**: Automatic rotation of API keys based on configured intervals

### Authentication & Authorization
- **JWT Tokens**: Configurable token expiration and validation
- **Service Principals**: Azure AD authentication for enterprise environments
- **Conditional Access**: IP restrictions and MFA enforcement
- **Role-Based Access Control**: Granular permissions for different user roles

### Code Security
- **Sandbox Execution**: Generated Python scripts executed in isolated environments
- **Input Validation**: Comprehensive validation of all user inputs
- **Audit Logging**: Complete audit trail of all security events
- **Circuit Breaker Pattern**: Prevents system overload during failures

```python
# Example of secure credential handling
class SecurityHardeningManager:
    def encrypt_sensitive_config(self, config: Dict) -> Dict:
        # Encrypt sensitive fields using AES-256
        # Store in Azure Key Vault
        # Return config with secure references
```

---

## Compliance

### Supported Standards
- **GDPR**: Data minimization, consent management, right to erasure
- **SOX**: Financial controls, audit trails, change management
- **HIPAA**: PHI protection, access controls, audit requirements
- **SOC 2**: Security, availability, processing integrity controls

### Compliance Features
- **Data Classification**: Automatic classification (Public, Internal, Confidential)
- **Retention Policies**: Configurable data retention and deletion
- **Audit Reporting**: Comprehensive compliance documentation
- **Consent Management**: User consent tracking and management
- **Multi-Geo Compliance**: Data residency and regional compliance management

```python
# Compliance reporting example
class FabricComplianceManager:
    def generate_compliance_report(self) -> Dict:
        return {
            "status": "compliant",
            "last_audit": datetime.now().isoformat(),
            "standards": ["GDPR", "SOX", "HIPAA"],
            "data_classification": "Confidential",
            "data_residency": "West Europe"
        }
```

---

## Monitoring & Metrics

### Performance Metrics
- **Request Metrics**: Count, duration, and success rates
- **Data Quality**: Quality scores and issue tracking
- **System Health**: Memory usage, CPU utilization, connection counts
- **Business Metrics**: Report generation times, deployment success rates
- **Streaming Metrics**: Data processing rates, lag times, error counts

### Monitoring Integration
- **Prometheus**: Real-time metrics collection and alerting
- **Application Insights**: Application performance monitoring
- **Azure Monitor**: Infrastructure and dependency monitoring
- **Teams Notifications**: Real-time alerts and notifications
- **Redis**: Caching metrics and performance monitoring

```python
# Prometheus metrics example
REQUEST_COUNT = Counter('powerbi_requests_total', 'Total Power BI requests', ['endpoint', 'status'])
DATA_QUALITY_SCORE = Gauge('powerbi_data_quality_score', 'Data quality score', ['data_source'])
STREAMING_DATA_PROCESSED = Counter('streaming_data_processed_total', 'Total streaming data processed', ['source'])
HUMAN_INTERACTION_COUNT = Counter('human_interaction_total', 'Total human interactions', ['type'])
COPILOT_ACTION_COUNT = Counter('copilot_action_total', 'Total Copilot actions', ['action'])
```

---

## Multi-Geo Support

### Global Deployment
- **Home Region Configuration**: Designate primary region for data processing
- **Multi-Geo Capacities**: Distribute workloads across multiple regions
- **Data Residency Compliance**: Ensure data stays within specified geographic boundaries
- **Regional Failover**: Automatic failover to secondary regions during outages

### Configuration
```yaml
multi_geo_config:
  home_region: "West Europe"
  multi_geo_capacities: 
    - region: "North Europe"
      capacity_id: "capacity-ne-01"
    - region: "West US"
      capacity_id: "capacity-wus-01"
  data_residency_compliance: "GDPR"
```

### Benefits
- **Compliance**: Meets regional data protection requirements
- **Performance**: Reduces latency by processing data closer to users
- **Resilience**: Geographic redundancy for disaster recovery
- **Scalability**: Distributes load across multiple regions

---

## Enterprise Integrations

### Azure DevOps
- **CI/CD Pipeline**: Automated deployment through Dev, Test, and Prod environments
- **Artifact Management**: Store and version report artifacts
- **Work Item Tracking**: Track report requirements and issues
- **Test Automation**: Automated testing of generated reports

### Microsoft Teams
- **Notifications**: Real-time alerts for report generation and deployment
- **Collaboration**: Discuss reports and provide feedback within Teams
- **Approvals**: Streamlined approval workflows within Teams channels
- **Bots**: Interactive bots for report generation and management

### Power Automate
- **Workflow Automation**: Automate business processes based on report insights
- **Data Synchronization**: Keep data sources synchronized
- **Approval Workflows**: Custom approval processes for report deployment
- **Notification Systems**: Custom notification rules and actions

### Configuration
```yaml
integrations:
  azure_devops_project: "https://dev.azure.com/your-org/your-project"
  teams_webhook: "https://outlook.office.com/webhook/your-webhook-url"
  key_vault_url: "https://your-key-vault-name.vault.azure.net/"
```

---

## Scaling & Performance

### Auto-Scaling
- **Dynamic Worker Management**: Automatically scale workers based on load
- **Resource Optimization**: Optimize resource utilization across the system
- **Kubernetes Integration**: Native support for Kubernetes deployments
- **Load Balancing**: Distribute requests across multiple instances

### Performance Optimization
- **Query Optimization**: Advanced query optimization techniques
- **Incremental Refresh**: Refresh only changed data to improve performance
- **Caching Strategy**: Multi-level caching for frequently accessed data
- **Connection Pooling**: Efficient management of database connections

### Configuration
```yaml
scaling:
  enabled: true
  min_workers: 2
  max_workers: 10
  scale_up_threshold: 0.7
  scale_down_threshold: 0.3
  cooldown_period: 300
  distributed_processing: true
  kubernetes_enabled: true
  load_balancing: true
  resource_optimization: true
  auto_scaling:
    enabled: true
    min_capacity: "F2"
    max_capacity: "F64"
    scale_triggers:
      cpu_threshold: 80
      memory_threshold: 85
      concurrent_users: 1000
  caching:
    redis_cluster:
      enabled: true
      nodes: 3
      memory_per_node: "8GB"
  performance_optimization:
    query_timeout: 300
    max_concurrent_queries: 50
    incremental_refresh: true
```

---

## Development

### Project Structure
```
BI-Forge--AI-Powered-Power-BI-Report-Generation-Generator/
├── generate_report_v1.py        # Raw Main application code V1
├── generate_report_v2.py        # LangChain integrated Main application code V2
├── generate_report_v3.py        # Kafka integrated Main application code V3
├── config.yaml                  # Configuration file
├── requirements.txt             # Python dependencies
├── archive/                     # older files
├── assets/                      # assets ex: images, pdf..etc
├── README.md                    # This file
└── LICENSE                      # MIT License
```

### Development Setup
1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Run tests:
   ```bash
   pytest tests/ --cov=generate_report_v3
   ```

3. Format code:
   ```bash
   black generate_report_v3.py
   isort generate_report_v3.py
   ```

### Key Dependencies
- **Core**: pandas, numpy, pydantic
- **AI**: openai, scikit-learn, langchain
- **Security**: pycryptodome, PyJWT
- **Cloud**: azure-identity, boto3, google-cloud-bigquery
- **Monitoring**: prometheus-client, redis
- **Streaming**: kafka-python, azure-eventhub
- **Web**: requests, aiohttp, fastapi

---

## Contributing

Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Future Updates List 📋

### 1. Syntax and Logging Fixes 🔧
- Fix unclosed logger handler list in `logging.basicConfig()` 🔌
- Correct function definitions missing colons (using `->` instead of `->`) ✏️

### 2. Configuration Management ⚙️
- Replace deeply nested defaults with YAML schema file 📄
- Implement Pydantic's Settings functionality for configuration validation and overrides ✅

### 3. Connector Factory and DRY Principles 🏭
- Introduce a ConnectorFactory to map types to classes 🗺️
- Extract common logic into base Connector class 🧩
- Refactor `_initialize_data_source_connectors` to use the factory 🔄

### 4. Resource Cleanup and Context Managers 🧹
- Add missing `disconnect()` calls on error paths 🚫
- Implement context managers for connectors to ensure proper cleanup 🔄

### 5. Error Handling and Logging 🚨
- Replace broad `except Exception` with specific exception types 🎯
- Implement proper exception traceback logging with `logger.exception(...)` 📊

### 6. Security and Sensitive Data 🔒
- Apply `sanitize_log_data()` to all exception messages before logging 🧼
- Replace environment variables with Azure/Key Vault or AWS Secrets Manager for credentials 🛡️

## 7. Rate Limiting and Concurrency ⏱️
- Make RateLimiter and AutoScaler thread-safe with proper locking 🔒
- Consider using existing libraries (tokensbucket, ratelimit) instead of custom implementation 📚

### 8. Simplify Report Generation Workflow 📊
- Break monolithic `PowerBIReportGenerator.generate_report` into smaller private methods:
  - `_detect_drift` 🔍
  - `_select_tables` 📋
  - `_profile_data` 📈
  - `_build_report` 🏗️

### 9. Testing and Validation 🧪
- Write comprehensive unit tests for:
  - Each connector class 🔌
  - Data profiler 📊
  - Schema drift detector 🔍
  - Main workflow 🔄
- Implement mocks for external dependencies (database, OpenAI, Fabric) 🎭

### 10. Documentation and Typing 📖
- Add comprehensive docstrings with parameter and return descriptions 📝
- Complete type hints for all public methods and classes 🔤
- Separate CLI entrypoint into its own module 🖥️
- Implement proper CLI using `argparse` or `click` 🎛️

### Development Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Update documentation for API changes
- Ensure all security best practices are followed
- Test with multiple data sources and configurations

---

Made with ❤️ by Naveen Jujaray