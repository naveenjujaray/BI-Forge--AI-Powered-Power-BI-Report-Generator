"""
Power BI Generator Package

This package provides tools for generating Power BI project files (PBIP)
and related artifacts for automated Power BI development workflows.
"""

__version__ = "1.0.0"
__author__ = "AI Development Team"
__description__ = "Power BI project file generation and management"

# Core generator imports
from .pbip_generator import PBIPGenerator

# Utility imports and constants
import os
import json
from typing import Dict, List, Any, Optional
from pathlib import Path

# Supported Power BI artifact types
SUPPORTED_ARTIFACT_TYPES = [
    "report",
    "dataset", 
    "dataflow",
    "dashboard",
    "paginated_report"
]

# Supported data source types
SUPPORTED_DATA_SOURCES = [
    "sql_server",
    "postgresql", 
    "mysql",
    "oracle",
    "csv",
    "excel",
    "api",
    "bigquery",
    "salesforce",
    "s3",
    "azure_sql",
    "synapse",
    "snowflake",
    "onelake",
    "semantic_model"
]

# Power BI visual types mapping
POWER_BI_VISUALS = {
    "column_chart": "columnChart",
    "line_chart": "lineChart", 
    "pie_chart": "pieChart",
    "bar_chart": "barChart",
    "area_chart": "areaChart",
    "card": "card",
    "multi_row_card": "multiRowCard",
    "table": "table",
    "matrix": "matrix",
    "map": "map",
    "filled_map": "filledMap",
    "gauge": "gauge",
    "kpi": "kpi",
    "slicer": "slicer",
    "scatter_chart": "scatterChart",
    "bubble_chart": "bubbleChart",
    "waterfall": "waterfallChart",
    "funnel": "funnelChart",
    "tree_map": "treemapChart",
    "ribbon_chart": "ribbonChart",
    "decomposition_tree": "decompositionTree",
    "key_influencers": "keyInfluencers"
}

# Default themes
DEFAULT_THEMES = {
    "standard": {
        "name": "Standard",
        "dataColors": [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
            "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
        ],
        "background": "#FFFFFF",
        "foreground": "#000000"
    },
    "executive": {
        "name": "Executive", 
        "dataColors": [
            "#003f5c", "#374c80", "#7a5195", "#bc5090",
            "#ef5675", "#ff764a", "#ffa600", "#003d82"
        ],
        "background": "#F8F9FA", 
        "foreground": "#212529"
    },
    "business": {
        "name": "Business",
        "dataColors": [
            "#1f4e79", "#2e75b6", "#70ad47", "#ffc000",
            "#c5504b", "#9b59b6", "#34495e", "#16a085"
        ],
        "background": "#FFFFFF",
        "foreground": "#2F4F4F"
    }
}

# Convenience functions
def create_pbip_generator(config: Dict[str, Any]) -> PBIPGenerator:
    """
    Factory function to create a PBIP generator.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configured PBIPGenerator instance
    
    Example:
        >>> config = load_config("config.yaml")
        >>> generator = create_pbip_generator(config)
    """
    return PBIPGenerator(config)

def validate_project_structure(project_path: str) -> Dict[str, Any]:
    """
    Validate Power BI project structure.
    
    Args:
        project_path: Path to the project directory
        
    Returns:
        Validation result with status and details
    """
    result = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "files_found": []
    }
    
    project_dir = Path(project_path)
    
    if not project_dir.exists():
        result["valid"] = False
        result["errors"].append(f"Project directory does not exist: {project_path}")
        return result
    
    # Check for .pbip file
    pbip_files = list(project_dir.glob("*.pbip"))
    if not pbip_files:
        result["errors"].append("No .pbip file found in project directory")
        result["valid"] = False
    else:
        result["files_found"].extend([str(f) for f in pbip_files])
    
    # Check for dataset directory
    dataset_dirs = [d for d in project_dir.iterdir() if d.is_dir() and d.name.endswith('.Dataset')]
    if not dataset_dirs:
        result["warnings"].append("No .Dataset directory found")
    else:
        result["files_found"].extend([str(d) for d in dataset_dirs])
    
    # Check for report directory  
    report_dirs = [d for d in project_dir.iterdir() if d.is_dir() and d.name.endswith('.Report')]
    if not report_dirs:
        result["warnings"].append("No .Report directory found")
    else:
        result["files_found"].extend([str(d) for d in report_dirs])
    
    return result

def get_supported_data_sources() -> List[str]:
    """Get list of supported data source types."""
    return SUPPORTED_DATA_SOURCES.copy()

def get_supported_visuals() -> Dict[str, str]:
    """Get mapping of supported visual types."""
    return POWER_BI_VISUALS.copy()

def get_default_themes() -> Dict[str, Dict]:
    """Get available default themes."""
    return DEFAULT_THEMES.copy()

def create_project_template(project_name: str, output_path: str) -> Dict[str, Any]:
    """
    Create a basic Power BI project template.
    
    Args:
        project_name: Name for the project
        output_path: Where to create the template
        
    Returns:
        Result of template creation
    """
    try:
        project_dir = Path(output_path) / project_name
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Create basic .pbip file
        pbip_content = {
            "version": "1.0",
            "artifacts": [
                {
                    "report": {
                        "path": f"{project_name}.Report",
                        "mode": "directory"
                    }
                },
                {
                    "dataset": {
                        "path": f"{project_name}.Dataset", 
                        "mode": "directory"
                    }
                }
            ]
        }
        
        pbip_file = project_dir / f"{project_name}.pbip"
        with open(pbip_file, 'w', encoding='utf-8') as f:
            json.dump(pbip_content, f, indent=2)
        
        # Create dataset directory
        dataset_dir = project_dir / f"{project_name}.Dataset"
        dataset_dir.mkdir(exist_ok=True)
        
        # Create report directory
        report_dir = project_dir / f"{project_name}.Report"
        report_dir.mkdir(exist_ok=True)
        
        # Create .gitignore
        gitignore_content = """# Power BI
*.pbix
*.pbit
temp/
cache/

# OS
.DS_Store
Thumbs.db

# IDEs
.vscode/
.idea/
"""
        
        gitignore_file = project_dir / ".gitignore"
        with open(gitignore_file, 'w', encoding='utf-8') as f:
            f.write(gitignore_content)
        
        return {
            "status": "success",
            "project_path": str(project_dir),
            "files_created": [
                str(pbip_file),
                str(dataset_dir),
                str(report_dir),
                str(gitignore_file)
            ]
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }

# Data type mapping utilities
def map_source_to_powerbi_type(source_type: str) -> str:
    """Map source data type to Power BI data type."""
    type_mapping = {
        # Numeric types
        'int': 'int64',
        'integer': 'int64', 
        'bigint': 'int64',
        'smallint': 'int64',
        'tinyint': 'int64',
        'float': 'double',
        'real': 'double',
        'double': 'double',
        'decimal': 'decimal',
        'numeric': 'decimal',
        'money': 'decimal',
        'currency': 'decimal',
        
        # Text types
        'varchar': 'string',
        'char': 'string',
        'text': 'string',
        'nvarchar': 'string',
        'nchar': 'string', 
        'string': 'string',
        
        # Date/Time types
        'date': 'dateTime',
        'datetime': 'dateTime',
        'datetime2': 'dateTime',
        'timestamp': 'dateTime',
        'time': 'dateTime',
        
        # Boolean types
        'bit': 'boolean',
        'boolean': 'boolean',
        'bool': 'boolean',
        
        # Binary types
        'binary': 'binary',
        'varbinary': 'binary',
        'image': 'binary'
    }
    
    return type_mapping.get(source_type.lower(), 'string')

def generate_dax_format_string(data_type: str, column_name: str = "") -> str:
    """Generate appropriate DAX format string based on data type and column name."""
    data_type_lower = data_type.lower()
    column_name_lower = column_name.lower()
    
    # Currency detection
    if ('currency' in data_type_lower or 'money' in data_type_lower or
        any(keyword in column_name_lower for keyword in ['price', 'cost', 'revenue', 'sales', 'amount'])):
        return '"$"#,0.00'
    
    # Percentage detection
    if ('percent' in column_name_lower or 'rate' in column_name_lower):
        return '0.00%'
    
    # Date detection
    if 'date' in data_type_lower or 'time' in data_type_lower:
        return 'mm/dd/yyyy'
    
    # Integer detection
    if any(int_type in data_type_lower for int_type in ['int', 'bigint', 'smallint']):
        if any(keyword in column_name_lower for keyword in ['count', 'quantity', 'number']):
            return '#,0'
    
    # Decimal numbers
    if any(dec_type in data_type_lower for dec_type in ['float', 'double', 'decimal', 'numeric']):
        return '#,0.00'
    
    # Default
    return 'General'

# Package metadata
__all__ = [
    # Main class
    'PBIPGenerator',
    
    # Constants
    'SUPPORTED_ARTIFACT_TYPES',
    'SUPPORTED_DATA_SOURCES', 
    'POWER_BI_VISUALS',
    'DEFAULT_THEMES',
    
    # Factory functions
    'create_pbip_generator',
    'create_project_template',
    
    # Utility functions
    'validate_project_structure',
    'get_supported_data_sources',
    'get_supported_visuals',
    'get_default_themes',
    'map_source_to_powerbi_type',
    'generate_dax_format_string'
]

# Version compatibility check
def check_dependencies() -> Dict[str, bool]:
    """Check if required dependencies are available."""
    dependencies = {}
    
    try:
        import pandas
        dependencies['pandas'] = True
    except ImportError:
        dependencies['pandas'] = False
    
    try:
        import yaml
        dependencies['yaml'] = True
    except ImportError:
        dependencies['yaml'] = False
        
    try:
        import json
        dependencies['json'] = True
    except ImportError:
        dependencies['json'] = False
    
    return dependencies

# Package information
PACKAGE_INFO = {
    "name": "powerbi_generator",
    "version": __version__,
    "description": __description__, 
    "author": __author__,
    "supported_data_sources": len(SUPPORTED_DATA_SOURCES),
    "supported_visuals": len(POWER_BI_VISUALS),
    "dependencies": check_dependencies()
}

def get_package_info() -> Dict[str, Any]:
    """Get package information and capabilities."""
    return PACKAGE_INFO.copy()
