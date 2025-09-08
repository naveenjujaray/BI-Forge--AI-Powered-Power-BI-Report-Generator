
# BI Forge: AI-Powered Power BI Report Generation Generator

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-green.svg)
![Power BI](https://img.shields.io/badge/Power%20BI-Enterprise-orange.svg)

## Disclaimer

**This software is provided "as is" without warranty of any kind, express or implied.** The authors and copyright holders shall not be liable for any claim, damages or liability arising from the use of this software. Users are solely responsible for:

1. Ensuring compliance with all applicable laws and regulations in their jurisdiction
2. Protecting sensitive data and adhering to data protection requirements (GDPR, HIPAA, etc.)
3. Securing API keys, credentials, and authentication tokens
4. Validating all AI-generated reports before deployment to production environments
5. Maintaining appropriate security controls for their specific use case

The software generates reports and code using AI models. While quality checks are implemented, users must verify all outputs before implementation.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```

MIT License

Copyright (c) 2023 Power BI AI Generator Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

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
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)
- [Disclaimer](#disclaimer)

---

## Overview

The Power BI AI Generator is an enterprise-grade solution that leverages OpenAI's GPT-4 to automatically generate Power BI reports from diverse data sources. This system combines advanced AI capabilities with robust security, comprehensive data quality checks, and seamless enterprise integrations to streamline business intelligence workflows.

![Reporting with LLM](/assets/flowchart.png)

The system follows a sophisticated workflow:
1. **Data Ingestion**: Connects to multiple data sources (SQL, NoSQL, APIs, cloud storage)
2. **Query Processing**: Uses NLP to understand business requirements
3. **AI Generation**: Creates SQL queries and Python scripts using GPT-4
4. **Security Validation**: Sandboxes and validates all generated code
5. **Quality Assurance**: Performs data profiling and drift detection
6. **Deployment**: Publishes reports to Power BI with CI/CD integration

---

## Features

### Core Capabilities
- **Multi-Source Data Integration**: Supports SQL Server, PostgreSQL, CSV, Excel, APIs, BigQuery, Salesforce, S3, and OneLake
- **AI-Powered Report Generation**: Leverages GPT-4 to transform natural language queries into Power BI reports
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
- **Enterprise Integrations**: Azure DevOps, Microsoft Teams, Power Automate
- **Multi-Geo Support**: Configurable data residency and regional compliance
- **Real-time Monitoring**: Prometheus metrics and Application Insights integration

---

## Architecture

The system follows a modular, enterprise-grade architecture with clear separation of concerns:

```
┌──────────────────────────────────────────────────────────────────┐
│    BI Forge: AI-Powered Power BI Report Generation Generator     │
├──────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐  │
│  │   Data Sources   │ │   AI Engine      │ │   Security       │  │
│  │                  │ │                  │ │                  │  │
│  │ • SQL Server     │ │ • GPT-4          │ │ • AES-256        │  │
│  │ • PostgreSQL     │ │ • Query Gen      │ │ • JWT Auth       │  │
│  │ • BigQuery       │ │ • Code Gen       │ │ • Key Vault      │  │
│  │ • Salesforce     │ │ • Validation     │ │ • RBAC           │  │
│  └──────────────────┘ └──────────────────┘ └──────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐  │
│  │   Data Quality   │ │   Deployment     │ │   Monitoring     │  │
│  │                  │ │                  │ │                  │  │
│  │ • Profiling      │ │ • CI/CD Pipeline │ │ • Prometheus     │  │
│  │ • Drift Detect   │ │ • Stage Mgmt     │ │ • App Insights   │  │
│  │ • Validation     │ │ • Approval WF    │ │ • Alerting       │  │
│  └──────────────────┘ └──────────────────┘ └──────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Enterprise Integration Layer                   │ │
│  │                                                             │ │
│  │ • Azure DevOps     • Microsoft Teams    • Power Automate    │ │
│  │ • Compliance Reporting • Audit Logging                      │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
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

---

## Prerequisites

- **Python 3.8+**: Required for all core functionality
- **Power BI Workspace**: With appropriate permissions for report deployment
- **OpenAI API Key**: For GPT-4 integration
- **Azure Account**: For enterprise features (Key Vault, DevOps, Monitoring)
- **Data Source Access**: Credentials for all configured data sources
- **Redis Server**: For caching (optional but recommended)

---

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/powerbi-ai-generator.git
   cd powerbi-ai-generator
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
   python -c "from generate_report import PowerBIGenerator; print('Installation successful')"
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

fabric:
  tenant_id: "your-tenant-id"
  client_id: "your-client-id"
  client_secret: "your-client-secret"
  workspace_id: "your-workspace-id"
```

### Data Sources
```yaml
data_sources:
  sales_data:
    type: "sql_server"
    server: "your-server.database.windows.net"
    database: "your-database"
    username: "your-username"
    password: "your-password"
  
  customer_data:
    type: "postgresql"
    host: "your-postgres-server"
    port: 5432
    database: "your-database"
    username: "your-username"
    password: "your-password"
```

### Security & Compliance
```yaml
enterprise_security:
  conditional_access: true
  mfa_required: true
  device_compliance_required: true
  ip_restrictions: []

compliance:
  data_classification: "Confidential"
  retention_policy: "7_years"
  audit_logging: true
  standards: ["GDPR", "SOX", "HIPAA"]
```

---

## Usage

### Basic Example
```python
from generate_report import PowerBIGenerator

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

### Command Line Interface
```bash
# Generate a report
python generate_report.py --query "Show quarterly sales trends by region" --output sales_dashboard.pbit

# Deploy to specific stage
python generate_report.py --deploy --stage "prod" --report-id "report-123"
```

### Advanced Features
```python
# Data quality profiling
profile = generator.data_profiler.profile_data(dataframe, "sales_data")

# Schema drift detection
drift_result = generator.drift_detector.detect_schema_drift("sales_data", current_schema)

# Compliance reporting
compliance_report = generator.compliance_manager.generate_compliance_report()
```

---

## Security Implementation

### Data Protection
- **AES-256 Encryption**: All sensitive data encrypted using PBKDF2 key derivation
- **Secure Storage**: Azure Key Vault integration for credential management
- **Data Masking**: Automatic masking of sensitive fields in logs and reports

### Authentication & Authorization
- **JWT Tokens**: Configurable token expiration and validation
- **Service Principals**: Azure AD authentication for enterprise environments
- **Conditional Access**: IP restrictions and MFA enforcement

### Code Security
- **Sandbox Execution**: Generated Python scripts executed in isolated environments
- **Input Validation**: Comprehensive validation of all user inputs
- **Audit Logging**: Complete audit trail of all security events

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

```python
# Compliance reporting example
class FabricComplianceManager:
    def generate_compliance_report(self) -> Dict:
        return {
            "status": "compliant",
            "last_audit": datetime.now().isoformat(),
            "standards": ["GDPR", "SOX", "HIPAA"],
            "data_classification": "Confidential"
        }
```

---

## Monitoring & Metrics

### Performance Metrics
- **Request Metrics**: Count, duration, and success rates
- **Data Quality**: Quality scores and issue tracking
- **System Health**: Memory usage, CPU utilization, connection counts
- **Business Metrics**: Report generation times, deployment success rates

### Monitoring Integration
- **Prometheus**: Real-time metrics collection and alerting
- **Application Insights**: Application performance monitoring
- **Azure Monitor**: Infrastructure and dependency monitoring
- **Teams Notifications**: Real-time alerts and notifications

```python
# Prometheus metrics example
REQUEST_COUNT = Counter('powerbi_requests_total', 'Total Power BI requests', ['endpoint', 'status'])
DATA_QUALITY_SCORE = Gauge('powerbi_data_quality_score', 'Data quality score', ['data_source'])
```

---

## Development

### Project Structure
```
powerbi-ai-generator/
├── generate_report.py         # Main application code
├── config.yaml                # Configuration file
├── requirements.txt           # Python dependencies
├── tests/                     # Test suite
├── docs/                      # Documentation
├── .github/                   # GitHub workflows
└── LICENSE                    # MIT License
```

### Development Setup
1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Run tests:
   ```bash
   pytest tests/ --cov=generate_report
   ```

3. Format code:
   ```bash
   black generate_report.py
   isort generate_report.py
   ```

### Key Dependencies
- **Core**: pandas, numpy, pydantic
- **AI**: openai, scikit-learn
- **Security**: pycryptodome, PyJWT
- **Cloud**: azure-identity, boto3, google-cloud-bigquery
- **Monitoring**: prometheus-client, redis
- **Web**: requests, aiohttp

---

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Update documentation for API changes
- Ensure all security best practices are followed

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

**This software is provided "as is" without warranty of any kind, express or implied.** The authors and copyright holders shall not be liable for any claim, damages or liability arising from the use of this software. Users are solely responsible for:

1. Ensuring compliance with all applicable laws and regulations in their jurisdiction
2. Protecting sensitive data and adhering to data protection requirements (GDPR, HIPAA, etc.)
3. Securing API keys, credentials, and authentication tokens
4. Validating all AI-generated reports before deployment to production environments
5. Maintaining appropriate security controls for their specific use case

The software generates reports and code using AI models. While quality checks are implemented, users must verify all outputs before implementation.

---

Made with ❤️ by Naveen Jujaray