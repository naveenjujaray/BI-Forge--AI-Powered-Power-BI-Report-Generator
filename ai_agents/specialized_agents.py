import json
import pandas as pd
from typing import Dict, List, Any, Optional
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage

from .agent_framework import BaseAIAgent, AgentType, AgentTask, TaskStatus
from ..data_profiler import DataProfiler # type: ignore
# from ..schema_drift_detector import SchemaDriftDetector

class DataAnalystAgent(BaseAIAgent):
    """Agent specialized in data analysis and profiling."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.DATA_ANALYST, name, config, **kwargs)
        self.data_profiler = DataProfiler(config)
    
    def _get_system_prompt(self) -> str:
        return """You are a Data Analyst Agent specialized in analyzing data sources for Power BI report generation.

Your responsibilities include:
1. Data profiling and quality assessment
2. Schema analysis and documentation
3. Identifying data relationships
4. Recommending data transformations
5. Detecting data anomalies and outliers

Always provide detailed, actionable insights based on the data analysis."""
    
    def _initialize_tools(self):
        """Initialize data analysis specific tools."""
        
        @tool
        def analyze_data_quality(data_source: str, sample_data: dict) -> dict:
            """Analyze data quality for a given data source."""
            try:
                df = pd.DataFrame(sample_data)
                profile = self.data_profiler.profile_data(df, data_source)
                return {
                    "status": "success",
                    "profile": profile,
                    "recommendations": self._generate_quality_recommendations(profile)
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def identify_key_columns(schema: dict) -> dict:
            """Identify key columns and relationships in a schema."""
            try:
                key_columns = []
                relationships = []
                
                for table_name, table_info in schema.items():
                    columns = table_info.get('columns', [])
                    
                    for column in columns:
                        col_name = column.get('name', '')
                        col_type = column.get('type', '')
                        
                        # Identify potential key columns
                        if any(keyword in col_name.lower() for keyword in ['id', 'key', 'pk']):
                            key_columns.append({
                                "table": table_name,
                                "column": col_name,
                                "type": col_type,
                                "category": "primary_key"
                            })
                        
                        # Identify foreign key candidates
                        if col_name.lower().endswith('_id') or col_name.lower().endswith('id'):
                            key_columns.append({
                                "table": table_name,
                                "column": col_name,
                                "type": col_type,
                                "category": "foreign_key_candidate"
                            })
                
                return {
                    "status": "success",
                    "key_columns": key_columns,
                    "relationships": relationships
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def suggest_measures_and_dimensions(schema: dict, requirements: str) -> dict:
            """Suggest measures and dimensions based on schema and requirements."""
            try:
                measures = []
                dimensions = []
                
                for table_name, table_info in schema.items():
                    columns = table_info.get('columns', [])
                    
                    for column in columns:
                        col_name = column.get('name', '')
                        col_type = column.get('type', '').lower()
                        
                        # Identify potential measures (numeric columns)
                        if any(numeric_type in col_type for numeric_type in ['int', 'float', 'decimal', 'money', 'numeric']):
                            if any(keyword in col_name.lower() for keyword in ['amount', 'total', 'sum', 'count', 'quantity', 'price', 'value']):
                                measures.append({
                                    "table": table_name,
                                    "column": col_name,
                                    "type": col_type,
                                    "suggested_aggregation": self._suggest_aggregation(col_name)
                                })
                        
                        # Identify potential dimensions
                        elif any(dim_type in col_type for dim_type in ['varchar', 'char', 'text', 'string', 'date', 'datetime']):
                            dimensions.append({
                                "table": table_name,
                                "column": col_name,
                                "type": col_type,
                                "category": self._categorize_dimension(col_name, col_type)
                            })
                
                return {
                    "status": "success",
                    "measures": measures,
                    "dimensions": dimensions
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([analyze_data_quality, identify_key_columns, suggest_measures_and_dimensions])
    
    def _generate_quality_recommendations(self, profile: Dict[str, Any]) -> List[str]:
        """Generate data quality recommendations based on profile."""
        recommendations = []
        
        overall_score = profile.get('overall_score', 100)
        if overall_score < 80:
            recommendations.append("Consider data cleansing to improve overall quality score")
        
        for issue in profile.get('issues', []):
            if issue.get('type') == 'high_null_percentage':
                recommendations.append(f"Address null values in column '{issue.get('column')}'")
            elif issue.get('type') == 'high_duplicate_percentage':
                recommendations.append(f"Consider deduplication for column '{issue.get('column')}'")
        
        return recommendations
    
    def _suggest_aggregation(self, column_name: str) -> str:
        """Suggest aggregation method based on column name."""
        column_lower = column_name.lower()
        
        if any(keyword in column_lower for keyword in ['count', 'quantity', 'number']):
            return "SUM"
        elif any(keyword in column_lower for keyword in ['amount', 'total', 'value', 'price']):
            return "SUM"
        elif any(keyword in column_lower for keyword in ['average', 'avg', 'mean']):
            return "AVERAGE"
        elif any(keyword in column_lower for keyword in ['rate', 'percentage', 'percent']):
            return "AVERAGE"
        else:
            return "SUM"
    
    def _categorize_dimension(self, column_name: str, column_type: str) -> str:
        """Categorize a dimension column."""
        column_lower = column_name.lower()
        type_lower = column_type.lower()
        
        if any(date_keyword in type_lower for date_keyword in ['date', 'time', 'timestamp']):
            return "time"
        elif any(geo_keyword in column_lower for geo_keyword in ['country', 'region', 'city', 'state', 'location']):
            return "geography"
        elif any(cat_keyword in column_lower for cat_keyword in ['category', 'type', 'class', 'group']):
            return "category"
        else:
            return "attribute"

class ReportDesignerAgent(BaseAIAgent):
    """Agent specialized in report design and visualization."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.REPORT_DESIGNER, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are a Report Designer Agent specialized in creating Power BI report layouts and visualizations.

Your responsibilities include:
1. Designing report layouts and page structures
2. Selecting appropriate visualizations for different data types
3. Creating user-friendly navigation and filters
4. Ensuring visual consistency and branding
5. Optimizing for different screen sizes and devices

Always consider user experience and data storytelling principles."""
    
    def _initialize_tools(self):
        """Initialize report design specific tools."""
        
        @tool
        def suggest_visualizations(measures: list, dimensions: list, requirements: str) -> dict:
            """Suggest appropriate visualizations based on measures and dimensions."""
            try:
                suggestions = []
                
                # Analyze the data structure
                num_measures = len(measures)
                num_dimensions = len(dimensions)
                
                # Time series check
                has_time_dimension = any(d.get('category') == 'time' for d in dimensions)
                has_geography = any(d.get('category') == 'geography' for d in dimensions)
                
                # Suggest visualizations based on data characteristics
                if has_time_dimension and num_measures > 0:
                    suggestions.append({
                        "type": "line_chart",
                        "title": "Trend Analysis",
                        "description": "Show trends over time",
                        "recommended_measures": measures[:3],
                        "recommended_dimensions": [d for d in dimensions if d.get('category') == 'time'][:1],
                        "priority": 1
                    })
                
                if has_geography and num_measures > 0:
                    suggestions.append({
                        "type": "map",
                        "title": "Geographic Distribution",
                        "description": "Show data distribution across locations",
                        "recommended_measures": measures[:1],
                        "recommended_dimensions": [d for d in dimensions if d.get('category') == 'geography'][:1],
                        "priority": 2
                    })
                
                if num_measures >= 1 and num_dimensions >= 1:
                    category_dims = [d for d in dimensions if d.get('category') == 'category']
                    if category_dims:
                        suggestions.append({
                            "type": "column_chart",
                            "title": "Category Comparison",
                            "description": "Compare values across categories",
                            "recommended_measures": measures[:2],
                            "recommended_dimensions": category_dims[:1],
                            "priority": 1
                        })
                        
                        suggestions.append({
                            "type": "pie_chart",
                            "title": "Category Distribution",
                            "description": "Show proportion of each category",
                            "recommended_measures": measures[:1],
                            "recommended_dimensions": category_dims[:1],
                            "priority": 3
                        })
                
                # Always suggest a summary card
                if num_measures > 0:
                    suggestions.append({
                        "type": "card",
                        "title": "Key Metrics",
                        "description": "Display key performance indicators",
                        "recommended_measures": measures[:4],
                        "recommended_dimensions": [],
                        "priority": 1
                    })
                
                # Suggest a detailed table
                suggestions.append({
                    "type": "table",
                    "title": "Detailed Data",
                    "description": "Show detailed data in tabular format",
                    "recommended_measures": measures,
                    "recommended_dimensions": dimensions,
                    "priority": 4
                })
                
                return {
                    "status": "success",
                    "suggestions": sorted(suggestions, key=lambda x: x['priority'])
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def design_report_layout(visualizations: list, requirements: str) -> dict:
            """Design the overall report layout."""
            try:
                # Group visualizations by priority and type
                high_priority = [v for v in visualizations if v.get('priority', 5) <= 2]
                medium_priority = [v for v in visualizations if v.get('priority', 5) == 3]
                low_priority = [v for v in visualizations if v.get('priority', 5) >= 4]
                
                pages = []
                
                # Main dashboard page
                main_page = {
                    "name": "Overview",
                    "description": "Main dashboard with key insights",
                    "layout": "grid",
                    "visuals": []
                }
                
                # Add high priority visuals to main page
                row = 0
                col = 0
                for visual in high_priority[:6]:  # Limit to 6 visuals on main page
                    visual_config = {
                        "type": visual["type"],
                        "title": visual["title"],
                        "description": visual["description"],
                        "position": {
                            "row": row,
                            "column": col,
                            "width": self._get_visual_width(visual["type"]),
                            "height": self._get_visual_height(visual["type"])
                        },
                        "data_fields": {
                            "measures": visual.get("recommended_measures", []),
                            "dimensions": visual.get("recommended_dimensions", [])
                        }
                    }
                    main_page["visuals"].append(visual_config)
                    
                    # Update position for next visual
                    col += visual_config["position"]["width"]
                    if col >= 12:  # Assuming 12-column grid
                        col = 0
                        row += visual_config["position"]["height"]
                
                pages.append(main_page)
                
                # Details page if there are more visuals
                if medium_priority or low_priority:
                    details_page = {
                        "name": "Details",
                        "description": "Detailed analysis and data exploration",
                        "layout": "grid",
                        "visuals": []
                    }
                    
                    row = 0
                    col = 0
                    for visual in medium_priority + low_priority:
                        visual_config = {
                            "type": visual["type"],
                            "title": visual["title"],
                            "description": visual["description"],
                            "position": {
                                "row": row,
                                "column": col,
                                "width": self._get_visual_width(visual["type"]),
                                "height": self._get_visual_height(visual["type"])
                            },
                            "data_fields": {
                                "measures": visual.get("recommended_measures", []),
                                "dimensions": visual.get("recommended_dimensions", [])
                            }
                        }
                        details_page["visuals"].append(visual_config)
                        
                        # Update position for next visual
                        col += visual_config["position"]["width"]
                        if col >= 12:
                            col = 0
                            row += visual_config["position"]["height"]
                    
                    pages.append(details_page)
                
                return {
                    "status": "success",
                    "layout": {
                        "pages": pages,
                        "theme": self._suggest_theme(requirements),
                        "navigation": self._design_navigation(pages),
                        "filters": self._suggest_filters(visualizations)
                    }
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def create_color_palette(requirements: str, brand_colors: list = None) -> dict:
            """Create a color palette for the report."""
            try:
                if brand_colors:
                    palette = brand_colors[:8]  # Use up to 8 brand colors
                else:
                    # Default professional palette
                    palette = [
                        "#1f77b4",  # Blue
                        "#ff7f0e",  # Orange
                        "#2ca02c",  # Green
                        "#d62728",  # Red
                        "#9467bd",  # Purple
                        "#8c564b",  # Brown
                        "#e377c2",  # Pink
                        "#7f7f7f"   # Gray
                    ]
                
                return {
                    "status": "success",
                    "palette": {
                        "primary": palette,
                        "semantic": {
                            "positive": "#2ca02c",
                            "negative": "#d62728",
                            "neutral": "#7f7f7f",
                            "warning": "#ff7f0e"
                        },
                        "backgrounds": {
                            "primary": "#ffffff",
                            "secondary": "#f8f9fa",
                            "accent": "#e9ecef"
                        }
                    }
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([suggest_visualizations, design_report_layout, create_color_palette])
    
    def _get_visual_width(self, visual_type: str) -> int:
        """Get recommended width for a visual type."""
        width_map = {
            "card": 3,
            "line_chart": 6,
            "column_chart": 6,
            "pie_chart": 4,
            "map": 8,
            "table": 12,
            "matrix": 8,
            "gauge": 4,
            "kpi": 3
        }
        return width_map.get(visual_type, 6)
    
    def _get_visual_height(self, visual_type: str) -> int:
        """Get recommended height for a visual type."""
        height_map = {
            "card": 2,
            "line_chart": 4,
            "column_chart": 4,
            "pie_chart": 4,
            "map": 5,
            "table": 6,
            "matrix": 5,
            "gauge": 3,
            "kpi": 2
        }
        return height_map.get(visual_type, 4)
    
    def _suggest_theme(self, requirements: str) -> str:
        """Suggest a theme based on requirements."""
        requirements_lower = requirements.lower()
        
        if any(keyword in requirements_lower for keyword in ['executive', 'board', 'management']):
            return "executive"
        elif any(keyword in requirements_lower for keyword in ['sales', 'revenue', 'profit']):
            return "business"
        elif any(keyword in requirements_lower for keyword in ['technical', 'engineering', 'development']):
            return "technical"
        else:
            return "standard"
    
    def _design_navigation(self, pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Design navigation for the report."""
        return {
            "type": "tabs",
            "position": "top",
            "pages": [{"name": page["name"], "icon": self._get_page_icon(page["name"])} for page in pages]
        }
    
    def _get_page_icon(self, page_name: str) -> str:
        """Get icon for a page based on its name."""
        icon_map = {
            "overview": "dashboard",
            "details": "table",
            "trends": "trending_up",
            "geography": "map",
            "performance": "analytics"
        }
        return icon_map.get(page_name.lower(), "assessment")
    
    def _suggest_filters(self, visualizations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Suggest filters based on visualizations."""
        filters = []
        
        # Collect all dimensions from visualizations
        all_dimensions = []
        for viz in visualizations:
            all_dimensions.extend(viz.get("recommended_dimensions", []))
        
        # Group dimensions by category
        time_dims = [d for d in all_dimensions if d.get('category') == 'time']
        geo_dims = [d for d in all_dimensions if d.get('category') == 'geography']
        cat_dims = [d for d in all_dimensions if d.get('category') == 'category']
        
        # Suggest time filters
        if time_dims:
            filters.append({
                "type": "date_range",
                "field": time_dims[0],
                "position": "top",
                "default_range": "last_12_months"
            })
        
        # Suggest category filters
        for cat_dim in cat_dims[:3]:  # Limit to 3 category filters
            filters.append({
                "type": "dropdown",
                "field": cat_dim,
                "position": "top",
                "multi_select": True
            })
        
        return filters

class DAXDeveloperAgent(BaseAIAgent):
    """Agent specialized in DAX development and measure creation."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.DAX_DEVELOPER, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are a DAX Developer Agent specialized in creating DAX measures and calculated columns for Power BI.

Your responsibilities include:
1. Writing efficient DAX expressions
2. Creating calculated measures and columns
3. Implementing time intelligence functions
4. Optimizing DAX performance
5. Creating advanced calculations and KPIs

Always follow DAX best practices and write performance-optimized code."""
    
    def _initialize_tools(self):
        """Initialize DAX development specific tools."""
        
        @tool
        def generate_basic_measures(measures_spec: list, table_info: dict) -> dict:
            """Generate basic DAX measures based on specifications."""
            try:
                dax_measures = {}
                
                for measure in measures_spec:
                    table_name = measure.get('table', '')
                    column_name = measure.get('column', '')
                    aggregation = measure.get('suggested_aggregation', 'SUM')
                    
                    measure_name = f"Total {column_name.replace('_', ' ').title()}"
                    
                    if aggregation == 'SUM':
                        dax_expression = f"SUM('{table_name}'[{column_name}])"
                    elif aggregation == 'AVERAGE':
                        dax_expression = f"AVERAGE('{table_name}'[{column_name}])"
                    elif aggregation == 'COUNT':
                        dax_expression = f"COUNT('{table_name}'[{column_name}])"
                    elif aggregation == 'DISTINCTCOUNT':
                        dax_expression = f"DISTINCTCOUNT('{table_name}'[{column_name}])"
                    else:
                        dax_expression = f"SUM('{table_name}'[{column_name}])"
                    
                    dax_measures[measure_name] = {
                        "expression": dax_expression,
                        "format": self._suggest_format(column_name, aggregation),
                        "description": f"{aggregation} of {column_name}",
                        "category": "Basic Measures"
                    }
                
                return {
                    "status": "success",
                    "measures": dax_measures
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def create_time_intelligence_measures(base_measures: dict, date_table: str) -> dict:
            """Create time intelligence measures."""
            try:
                time_intelligence_measures = {}
                
                for measure_name, measure_info in base_measures.items():
                    base_expression = measure_info.get('expression', '')
                    
                    # Previous Year
                    py_name = f"{measure_name} PY"
                    time_intelligence_measures[py_name] = {
                        "expression": f"CALCULATE({base_expression}, SAMEPERIODLASTYEAR('{date_table}'[Date]))",
                        "format": measure_info.get('format', ''),
                        "description": f"Previous year value of {measure_name}",
                        "category": "Time Intelligence"
                    }
                    
                    # Year over Year Growth
                    yoy_name = f"{measure_name} YoY Growth"
                    time_intelligence_measures[yoy_name] = {
                        "expression": f"DIVIDE([{measure_name}] - [{py_name}], [{py_name}])",
                        "format": "Percentage",
                        "description": f"Year over year growth of {measure_name}",
                        "category": "Time Intelligence"
                    }
                    
                    # Year to Date
                    ytd_name = f"{measure_name} YTD"
                    time_intelligence_measures[ytd_name] = {
                        "expression": f"TOTALYTD({base_expression}, '{date_table}'[Date])",
                        "format": measure_info.get('format', ''),
                        "description": f"Year to date value of {measure_name}",
                        "category": "Time Intelligence"
                    }
                    
                    # Previous Year to Date
                    pytd_name = f"{measure_name} PYTD"
                    time_intelligence_measures[pytd_name] = {
                        "expression": f"CALCULATE([{ytd_name}], SAMEPERIODLASTYEAR('{date_table}'[Date]))",
                        "format": measure_info.get('format', ''),
                        "description": f"Previous year to date value of {measure_name}",
                        "category": "Time Intelligence"
                    }
                
                return {
                    "status": "success",
                    "measures": time_intelligence_measures
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def create_kpi_measures(measures_spec: list, targets: dict) -> dict:
            """Create KPI measures with targets and status indicators."""
            try:
                kpi_measures = {}
                
                for measure in measures_spec:
                    measure_name = measure.get('name', '')
                    target_value = targets.get(measure_name)
                    
                    if target_value:
                        # Target measure
                        target_name = f"{measure_name} Target"
                        kpi_measures[target_name] = {
                            "expression": str(target_value),
                            "format": measure.get('format', ''),
                            "description": f"Target value for {measure_name}",
                            "category": "KPI"
                        }
                        
                        # Variance measure
                        variance_name = f"{measure_name} Variance"
                        kpi_measures[variance_name] = {
                            "expression": f"[{measure_name}] - [{target_name}]",
                            "format": measure.get('format', ''),
                            "description": f"Variance from target for {measure_name}",
                            "category": "KPI"
                        }
                        
                        # Variance percentage
                        variance_pct_name = f"{measure_name} Variance %"
                        kpi_measures[variance_pct_name] = {
                            "expression": f"DIVIDE([{variance_name}], [{target_name}])",
                            "format": "Percentage",
                            "description": f"Variance percentage from target for {measure_name}",
                            "category": "KPI"
                        }
                        
                        # Status indicator
                        status_name = f"{measure_name} Status"
                        kpi_measures[status_name] = {
                            "expression": f"""
                                SWITCH(
                                    TRUE(),
                                    [{variance_pct_name}] >= 0.05, "Above Target",
                                    [{variance_pct_name}] >= -0.05, "On Target",
                                    "Below Target"
                                )
                            """,
                            "format": "Text",
                            "description": f"Status indicator for {measure_name}",
                            "category": "KPI"
                        }
                
                return {
                    "status": "success",
                    "measures": kpi_measures
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def optimize_dax_expressions(measures: dict) -> dict:
            """Optimize DAX expressions for better performance."""
            try:
                optimized_measures = {}
                
                for measure_name, measure_info in measures.items():
                    expression = measure_info.get('expression', '')
                    
                    # Apply optimization rules
                    optimized_expression = self._optimize_dax_expression(expression)
                    
                    optimized_measures[measure_name] = {
                        **measure_info,
                        "expression": optimized_expression,
                        "optimization_applied": optimized_expression != expression
                    }
                
                return {
                    "status": "success",
                    "measures": optimized_measures
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([generate_basic_measures, create_time_intelligence_measures, create_kpi_measures, optimize_dax_expressions])
    
    def _suggest_format(self, column_name: str, aggregation: str) -> str:
        """Suggest format for a measure based on column name and aggregation."""
        column_lower = column_name.lower()
        
        if any(keyword in column_lower for keyword in ['amount', 'price', 'cost', 'revenue', 'sales']):
            return "Currency"
        elif any(keyword in column_lower for keyword in ['percentage', 'percent', 'rate']):
            return "Percentage"
        elif aggregation == 'COUNT' or any(keyword in column_lower for keyword in ['count', 'quantity']):
            return "Whole Number"
        else:
            return "General Number"
    
    def _optimize_dax_expression(self, expression: str) -> str:
        """Apply DAX optimization rules to an expression."""
        # Remove unnecessary whitespace
        optimized = ' '.join(expression.split())
        
        # Replace inefficient patterns
        optimizations = [
            # Use SUMX instead of SUM where appropriate
            (r'SUM\(([^)]+)\[([^]]+)\]\s*\*\s*([^)]+)\)', r'SUMX(\1, [\2] * \3)'),
            
            # Use DIVIDE instead of division operator for better error handling
            (r'([^/]+)\s*/\s*([^/]+)', r'DIVIDE(\1, \2)'),
            
            # Use BLANK() instead of 0 where appropriate for better performance
            (r'\b0\b(?=\s*,|\s*\))', 'BLANK()'),
        ]
        
        import re
        for pattern, replacement in optimizations:
            optimized = re.sub(pattern, replacement, optimized)
        
        return optimized

class SchemaArchitectAgent(BaseAIAgent):
    """Agent specialized in data model architecture and relationships."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.SCHEMA_ARCHITECT, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are a Schema Architect Agent specialized in designing optimal data models for Power BI.

Your responsibilities include:
1. Designing star schema and snowflake schema models
2. Identifying and creating table relationships
3. Optimizing data model performance
4. Creating calculated tables and columns
5. Implementing role-playing dimensions

Always follow dimensional modeling best practices and optimize for query performance."""
    
    def _initialize_tools(self):
        """Initialize schema architecture specific tools."""
        
        @tool
        def design_star_schema(tables: dict, key_columns: list) -> dict:
            """Design a star schema from existing tables."""
            try:
                fact_tables = []
                dimension_tables = []
                relationships = []
                
                # Identify fact and dimension tables
                for table_name, table_info in tables.items():
                    columns = table_info.get('columns', [])
                    
                    # Heuristics to identify fact tables
                    has_measures = any(self._is_measure_column(col) for col in columns)
                    has_foreign_keys = any(self._is_foreign_key(col, key_columns) for col in columns)
                    
                    if has_measures and has_foreign_keys:
                        fact_tables.append({
                            "name": table_name,
                            "type": "fact",
                            "columns": columns,
                            "grain": self._determine_grain(columns, key_columns)
                        })
                    else:
                        dimension_tables.append({
                            "name": table_name,
                            "type": "dimension",
                            "columns": columns,
                            "primary_key": self._find_primary_key(columns, key_columns)
                        })
                
                # Create relationships
                for fact_table in fact_tables:
                    for dim_table in dimension_tables:
                        relationship = self._find_relationship(fact_table, dim_table, key_columns)
                        if relationship:
                            relationships.append(relationship)
                
                return {
                    "status": "success",
                    "schema": {
                        "type": "star",
                        "fact_tables": fact_tables,
                        "dimension_tables": dimension_tables,
                        "relationships": relationships
                    }
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def create_date_dimension(start_date: str, end_date: str) -> dict:
            """Create a comprehensive date dimension table."""
            try:
                import pandas as pd
                from datetime import datetime, timedelta
                
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                
                date_range = pd.date_range(start=start, end=end, freq='D')
                
                date_columns = []
                for date in date_range:
                    date_columns.append({
                        "Date": date.strftime('%Y-%m-%d'),
                        "Year": date.year,
                        "Quarter": f"Q{date.quarter}",
                        "Month": date.month,
                        "MonthName": date.strftime('%B'),
                        "MonthShort": date.strftime('%b'),
                        "Day": date.day,
                        "DayOfWeek": date.weekday() + 1,
                        "DayName": date.strftime('%A'),
                        "DayShort": date.strftime('%a'),
                        "WeekOfYear": date.isocalendar()[1],
                        "IsWeekend": date.weekday() >= 5,
                        "YearMonth": date.strftime('%Y-%m'),
                        "YearQuarter": f"{date.year}-Q{date.quarter}"
                    })
                
                return {
                    "status": "success",
                    "date_dimension": {
                        "name": "DateDimension",
                        "columns": [
                            {"name": "Date", "type": "Date", "is_key": True},
                            {"name": "Year", "type": "Integer"},
                            {"name": "Quarter", "type": "Text"},
                            {"name": "Month", "type": "Integer"},
                            {"name": "MonthName", "type": "Text"},
                            {"name": "MonthShort", "type": "Text"},
                            {"name": "Day", "type": "Integer"},
                            {"name": "DayOfWeek", "type": "Integer"},
                            {"name": "DayName", "type": "Text"},
                            {"name": "DayShort", "type": "Text"},
                            {"name": "WeekOfYear", "type": "Integer"},
                            {"name": "IsWeekend", "type": "Boolean"},
                            {"name": "YearMonth", "type": "Text"},
                            {"name": "YearQuarter", "type": "Text"}
                        ],
                        "data": date_columns
                    }
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def optimize_relationships(relationships: list, tables: dict) -> dict:
            """Optimize table relationships for better performance."""
            try:
                optimized_relationships = []
                
                for rel in relationships:
                    from_table = rel.get('from_table')
                    to_table = rel.get('to_table')
                    from_column = rel.get('from_column')
                    to_column = rel.get('to_column')
                    
                    # Determine optimal cardinality
                    cardinality = self._determine_cardinality(
                        from_table, from_column, to_table, to_column, tables
                    )
                    
                    # Determine cross filter direction
                    cross_filter = self._determine_cross_filter_direction(
                        from_table, to_table, tables
                    )
                    
                    optimized_relationships.append({
                        "from_table": from_table,
                        "from_column": from_column,
                        "to_table": to_table,
                        "to_column": to_column,
                        "cardinality": cardinality,
                        "cross_filter_direction": cross_filter,
                        "is_active": True,
                        "security_filtering": "None"
                    })
                
                return {
                    "status": "success",
                    "relationships": optimized_relationships
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([design_star_schema, create_date_dimension, optimize_relationships])
    
    def _is_measure_column(self, column: Dict[str, Any]) -> bool:
        """Check if a column is likely a measure column."""
        col_name = column.get('name', '').lower()
        col_type = column.get('type', '').lower()
        
        # Check for numeric types
        is_numeric = any(num_type in col_type for num_type in ['int', 'float', 'decimal', 'money', 'numeric'])
        
        # Check for measure-like names
        is_measure_name = any(keyword in col_name for keyword in [
            'amount', 'total', 'sum', 'count', 'quantity', 'price', 'value', 'revenue', 'sales', 'cost'
        ])
        
        return is_numeric and is_measure_name
    
    def _is_foreign_key(self, column: Dict[str, Any], key_columns: List[Dict[str, Any]]) -> bool:
        """Check if a column is likely a foreign key."""
        col_name = column.get('name', '').lower()
        
        # Check if it's in the identified key columns
        for key_col in key_columns:
            if (key_col.get('column', '').lower() == col_name and 
                key_col.get('category') == 'foreign_key_candidate'):
                return True
        
        return False
    
    def _determine_grain(self, columns: List[Dict[str, Any]], key_columns: List[Dict[str, Any]]) -> List[str]:
        """Determine the grain (granularity) of a fact table."""
        grain_columns = []
        
        for column in columns:
            col_name = column.get('name', '')
            
            # Check if it's a key column
            for key_col in key_columns:
                if key_col.get('column') == col_name:
                    grain_columns.append(col_name)
        
        return grain_columns
    
    def _find_primary_key(self, columns: List[Dict[str, Any]], key_columns: List[Dict[str, Any]]) -> Optional[str]:
        """Find the primary key of a table."""
        for column in columns:
            col_name = column.get('name', '')
            
            for key_col in key_columns:
                if (key_col.get('column') == col_name and 
                    key_col.get('category') == 'primary_key'):
                    return col_name
        
        # Fallback: look for ID columns
        for column in columns:
            col_name = column.get('name', '').lower()
            if col_name in ['id', 'key'] or col_name.endswith('_id') or col_name.endswith('id'):
                return column.get('name')
        
        return None
    
    def _find_relationship(self, fact_table: Dict[str, Any], dim_table: Dict[str, Any], 
                          key_columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find relationship between fact and dimension tables."""
        fact_columns = [col.get('name', '') for col in fact_table.get('columns', [])]
        dim_pk = dim_table.get('primary_key')
        
        if not dim_pk:
            return None
        
        # Look for foreign key in fact table that matches dimension primary key
        for col_name in fact_columns:
            if (col_name.lower() == dim_pk.lower() or 
                col_name.lower() == f"{dim_table['name'].lower()}_{dim_pk.lower()}" or
                col_name.lower() == f"{dim_table['name'].lower()}id"):
                
                return {
                    "from_table": fact_table['name'],
                    "from_column": col_name,
                    "to_table": dim_table['name'],
                    "to_column": dim_pk,
                    "cardinality": "many_to_one"
                }
        
        return None
    
    def _determine_cardinality(self, from_table: str, from_column: str, 
                              to_table: str, to_column: str, tables: Dict[str, Any]) -> str:
        """Determine the cardinality of a relationship."""
        # In a real implementation, this would analyze the actual data
        # For now, we'll use heuristics based on table and column names
        
        # Most relationships in star schema are many-to-one (fact to dimension)
        fact_keywords = ['fact', 'transaction', 'sales', 'order', 'event']
        dim_keywords = ['dim', 'dimension', 'lookup', 'master']
        
        from_is_fact = any(keyword in from_table.lower() for keyword in fact_keywords)
        to_is_dim = any(keyword in to_table.lower() for keyword in dim_keywords)
        
        if from_is_fact and to_is_dim:
            return "many_to_one"
        else:
            return "one_to_many"
    
    def _determine_cross_filter_direction(self, from_table: str, to_table: str, 
                                        tables: Dict[str, Any]) -> str:
        """Determine the cross filter direction."""
        # Default to single direction for better performance
        # In most star schemas, filters flow from dimension to fact
        return "single"
