import os
import json
import yaml
import zipfile
import tempfile
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PBIPGenerator:
    """Generator for Power BI Project files (.pbip)."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.project_structure = {
            "metadata": {},
            "data_model": {},
            "reports": {},
            "datasets": {},
            "measures": {},
            "relationships": {}
        }
    
    def generate_pbip_project(self, 
                             project_name: str,
                             data_sources: List[Dict[str, Any]],
                             schema: Dict[str, Any],
                             measures: Dict[str, Any],
                             relationships: List[Dict[str, Any]],
                             report_layout: Dict[str, Any],
                             output_path: str) -> Dict[str, Any]:
        """Generate a complete PBIP project."""
        try:
            # Create project directory structure
            project_dir = os.path.join(output_path, project_name)
            os.makedirs(project_dir, exist_ok=True)
            
            # Generate project files
            files_created = []
            
            # 1. Generate project definition file
            definition_file = self._generate_definition_file(project_name, project_dir)
            files_created.append(definition_file)
            
            # 2. Generate data model
            model_files = self._generate_data_model(schema, relationships, project_dir)
            files_created.extend(model_files)
            
            # 3. Generate measures
            measures_files = self._generate_measures(measures, project_dir)
            files_created.extend(measures_files)
            
            # 4. Generate reports
            report_files = self._generate_reports(report_layout, project_dir)
            files_created.extend(report_files)
            
            # 5. Generate data sources
            datasource_files = self._generate_data_sources(data_sources, project_dir)
            files_created.extend(datasource_files)
            
            # 6. Generate metadata
            metadata_files = self._generate_metadata(project_name, project_dir)
            files_created.extend(metadata_files)
            
            # 7. Create .pbip file
            pbip_file = self._create_pbip_file(project_name, project_dir)
            files_created.append(pbip_file)
            
            return {
                "status": "success",
                "project_path": project_dir,
                "pbip_file": pbip_file,
                "files_created": files_created,
                "message": f"PBIP project '{project_name}' generated successfully"
            }
            
        except Exception as e:
            logger.error(f"Error generating PBIP project: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _generate_definition_file(self, project_name: str, project_dir: str) -> str:
        """Generate the project definition file."""
        definition = {
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
        
        definition_file = os.path.join(project_dir, f"{project_name}.pbip")
        with open(definition_file, 'w', encoding='utf-8') as f:
            json.dump(definition, f, indent=2)
        
        return definition_file
    
    def _generate_data_model(self, schema: Dict[str, Any], 
                           relationships: List[Dict[str, Any]], 
                           project_dir: str) -> List[str]:
        """Generate data model files."""
        files_created = []
        
        # Create dataset directory
        dataset_dir = os.path.join(project_dir, f"{os.path.basename(project_dir)}.Dataset")
        os.makedirs(dataset_dir, exist_ok=True)
        
        # Generate model.bim file (Tabular model definition)
        model_bim = self._create_model_bim(schema, relationships)
        model_file = os.path.join(dataset_dir, "model.bim")
        
        with open(model_file, 'w', encoding='utf-8') as f:
            json.dump(model_bim, f, indent=2)
        files_created.append(model_file)
        
        # Generate table definitions
        tables_dir = os.path.join(dataset_dir, "tables")
        os.makedirs(tables_dir, exist_ok=True)
        
        for table_name, table_info in schema.items():
            table_file = self._create_table_definition(table_name, table_info, tables_dir)
            files_created.append(table_file)
        
        # Generate relationships file
        if relationships:
            relationships_file = os.path.join(dataset_dir, "relationships.json")
            with open(relationships_file, 'w', encoding='utf-8') as f:
                json.dump(relationships, f, indent=2)
            files_created.append(relationships_file)
        
        return files_created
    
    def _create_model_bim(self, schema: Dict[str, Any], 
                         relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create the model.bim file content."""
        tables = []
        
        for table_name, table_info in schema.items():
            columns = []
            
            for column_info in table_info.get('columns', []):
                column_def = {
                    "name": column_info.get('name', ''),
                    "dataType": self._map_data_type(column_info.get('type', '')),
                    "isHidden": False,
                    "isKey": column_info.get('is_key', False),
                    "isNullable": column_info.get('nullable', True)
                }
                
                # Add format string for specific data types
                if column_def["dataType"] == "dateTime":
                    column_def["formatString"] = "mm/dd/yyyy"
                elif column_def["dataType"] == "currency":
                    column_def["formatString"] = "$#,0.00"
                elif column_def["dataType"] == "percentage":
                    column_def["formatString"] = "0.00%"
                
                columns.append(column_def)
            
            table_def = {
                "name": table_name,
                "columns": columns,
                "partitions": [
                    {
                        "name": f"{table_name}_Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": f"let\n    Source = {table_name}\nin\n    Source"
                        }
                    }
                ]
            }
            
            tables.append(table_def)
        
        # Convert relationships to BIM format
        bim_relationships = []
        for rel in relationships:
            bim_rel = {
                "name": f"{rel.get('from_table')}_{rel.get('to_table')}",
                "fromTable": rel.get('from_table'),
                "fromColumn": rel.get('from_column'),
                "toTable": rel.get('to_table'),
                "toColumn": rel.get('to_column'),
                "crossFilteringBehavior": self._map_cross_filter_direction(
                    rel.get('cross_filter_direction', 'single')
                ),
                "cardinality": self._map_cardinality(rel.get('cardinality', 'many_to_one')),
                "isActive": rel.get('is_active', True)
            }
            bim_relationships.append(bim_rel)
        
        model_bim = {
            "name": "Model",
            "compatibilityLevel": 1600,
            "model": {
                "culture": "en-US",
                "dataSources": [],
                "tables": tables,
                "relationships": bim_relationships,
                "annotations": [
                    {
                        "name": "PBI_QueryOrder",
                        "value": json.dumps(list(schema.keys()))
                    }
                ]
            }
        }
        
        return model_bim
    
    # In pbip_generator.py, line ~450
    def _create_table_definition(self, table_name: str, table_info: Dict[str, Any], tables_dir: str) -> str:
        """Create individual table definition file."""
        # Sanitize table name for file system
        safe_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
        
        table_def = {
            "name": table_name,  # Keep original name for Power BI
            "description": table_info.get('description', f"Table containing {table_name} data"),
            "columns": table_info.get('columns', []),
            "partitions": [
                {
                    "name": f"{table_name}_Partition",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": self._generate_m_query(table_name, table_info)
                    }
                }
            ]
        }
        
        table_file = os.path.join(tables_dir, f"{safe_table_name}.json")
        with open(table_file, 'w', encoding='utf-8') as f:
            json.dump(table_def, f, indent=2)
        
        return table_file
    
    def _generate_m_query(self, table_name: str, table_info: Dict[str, Any]) -> str:
        """Generate M query for data loading."""
        # This is a simplified M query - in practice, this would be more complex
        # and depend on the actual data source configuration
        
        m_query = f"""let
    Source = {table_name},
    #"Changed Type" = Table.TransformColumnTypes(Source, {{"""
        
        type_transformations = []
        for column in table_info.get('columns', []):
            col_name = column.get('name', '')
            col_type = column.get('type', '')
            m_type = self._map_to_m_type(col_type)
            type_transformations.append(f'        {{"{col_name}", {m_type}}}')
        
        m_query += ",\n".join(type_transformations)
        m_query += """
    })
in
    #"Changed Type\""""
        
        return m_query
    
    def _generate_measures(self, measures: Dict[str, Any], project_dir: str) -> List[str]:
        """Generate measures files."""
        files_created = []
        
        if not measures:
            return files_created
        
        # Create measures directory
        dataset_dir = os.path.join(project_dir, f"{os.path.basename(project_dir)}.Dataset")
        measures_dir = os.path.join(dataset_dir, "measures")
        os.makedirs(measures_dir, exist_ok=True)
        
        # Group measures by category
        measures_by_category = {}
        for measure_name, measure_info in measures.items():
            category = measure_info.get('category', 'General')
            if category not in measures_by_category:
                measures_by_category[category] = {}
            measures_by_category[category][measure_name] = measure_info
        
        # Create a file for each category
        for category, category_measures in measures_by_category.items():
            measures_def = {
                "measures": []
            }
            
            for measure_name, measure_info in category_measures.items():
                measure_def = {
                    "name": measure_name,
                    "expression": measure_info.get('expression', ''),
                    "formatString": self._get_format_string(measure_info.get('format', '')),
                    "description": measure_info.get('description', ''),
                    "displayFolder": category,
                    "isHidden": False
                }
                measures_def["measures"].append(measure_def)
            
            category_file = os.path.join(measures_dir, f"{category.replace(' ', '_').lower()}.json")
            with open(category_file, 'w', encoding='utf-8') as f:
                json.dump(measures_def, f, indent=2)
            files_created.append(category_file)
        
        return files_created
    
    def _generate_reports(self, report_layout: Dict[str, Any], project_dir: str) -> List[str]:
        """Generate report files."""
        files_created = []
        
        # Create report directory
        report_dir = os.path.join(project_dir, f"{os.path.basename(project_dir)}.Report")
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate report definition
        report_def = self._create_report_definition(report_layout)
        report_file = os.path.join(report_dir, "report.json")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_def, f, indent=2)
        files_created.append(report_file)
        
        # Generate individual page files
        pages_dir = os.path.join(report_dir, "pages")
        os.makedirs(pages_dir, exist_ok=True)
        
        for page in report_layout.get('pages', []):
            page_file = self._create_page_definition(page, pages_dir)
            files_created.append(page_file)
        
        # Generate static resources (if any)
        static_dir = os.path.join(report_dir, "StaticResources")
        os.makedirs(static_dir, exist_ok=True)
        
        # Create a basic theme file
        theme_file = self._create_theme_file(report_layout, static_dir)
        files_created.append(theme_file)
        
        return files_created
    
    def _create_report_definition(self, report_layout: Dict[str, Any]) -> Dict[str, Any]:
        """Create the main report definition."""
        pages = []
        
        for page in report_layout.get('pages', []):
            page_def = {
                "name": page.get('name', ''),
                "displayName": page.get('name', ''),
                "width": 1280,
                "height": 720,
                "displayOption": 1
            }
            pages.append(page_def)
        
        report_def = {
            "version": "4.0",
            "config": json.dumps({
                "version": "4.0",
                "themeCollection": {
                    "baseTheme": {
                        "name": "CY19SU06",
                        "version": "4.0",
                        "type": 2
                    }
                },
                "activeSectionIndex": 0,
                "defaultDrillFilterOtherVisuals": True,
                "defaultSlicerOrientation": 1
            }),
            "layoutOptimization": 0,
            "sections": pages,
            "bookmarks": [],
            "filters": self._create_report_filters(report_layout)
        }
        
        return report_def
    
    def _create_page_definition(self, page: Dict[str, Any], pages_dir: str) -> str:
        """Create individual page definition file."""
        visuals = []
        
        for visual in page.get('visuals', []):
            visual_def = self._create_visual_definition(visual)
            visuals.append(visual_def)
        
        page_def = {
            "name": page.get('name', ''),
            "displayName": page.get('name', ''),
            "width": 1280,
            "height": 720,
            "displayOption": 1,
            "visualContainers": visuals,
            "filters": self._create_page_filters(page)
        }
        
        page_file = os.path.join(pages_dir, f"{page.get('name', 'page').replace(' ', '_').lower()}.json")
        with open(page_file, 'w', encoding='utf-8') as f:
            json.dump(page_def, f, indent=2)
        
        return page_file
    
    def _create_visual_definition(self, visual: Dict[str, Any]) -> Dict[str, Any]:
        """Create visual definition."""
        position = visual.get('position', {})
        data_fields = visual.get('data_fields', {})
        
        # Map visual type to Power BI visual type
        visual_type_map = {
            "column_chart": "columnChart",
            "line_chart": "lineChart",
            "pie_chart": "pieChart",
            "card": "card",
            "table": "table",
            "matrix": "matrix",
            "map": "map",
            "gauge": "gauge",
            "kpi": "kpi"
        }
        
        pbi_visual_type = visual_type_map.get(visual.get('type', ''), 'columnChart')
        
        visual_def = {
            "x": position.get('column', 0) * 106.67,  # Convert grid position to pixels
            "y": position.get('row', 0) * 120,
            "z": 1000,
            "width": position.get('width', 6) * 106.67,
            "height": position.get('height', 4) * 120,
            "config": json.dumps({
                "name": f"{visual.get('type', 'visual')}_{len(str(visual))}",
                "layouts": [
                    {
                        "id": 0,
                        "position": {
                            "x": position.get('column', 0) * 106.67,
                            "y": position.get('row', 0) * 120,
                            "z": 1000,
                            "width": position.get('width', 6) * 106.67,
                            "height": position.get('height', 4) * 120
                        }
                    }
                ],
                "singleVisual": {
                    "visualType": pbi_visual_type,
                    "projections": self._create_visual_projections(visual, data_fields),
                    "prototypeQuery": self._create_prototype_query(visual, data_fields),
                    "objects": self._create_visual_objects(visual)
                }
            })
        }
        
        return visual_def
    
    def _create_visual_projections(self, visual: Dict[str, Any], 
                                 data_fields: Dict[str, Any]) -> Dict[str, Any]:
        """Create visual data projections."""
        projections = {}
        
        # Map fields based on visual type
        visual_type = visual.get('type', '')
        measures = data_fields.get('measures', [])
        dimensions = data_fields.get('dimensions', [])
        
        if visual_type in ['column_chart', 'line_chart']:
            if dimensions:
                projections['Category'] = [self._create_field_projection(dimensions[0])]
            if measures:
                projections['Y'] = [self._create_field_projection(measure) for measure in measures[:2]]
        
        elif visual_type == 'pie_chart':
            if dimensions:
                projections['Category'] = [self._create_field_projection(dimensions[0])]
            if measures:
                projections['Y'] = [self._create_field_projection(measures[0])]
        
        elif visual_type == 'card':
            if measures:
                projections['Values'] = [self._create_field_projection(measures[0])]
        
        elif visual_type == 'table':
            all_fields = measures + dimensions
            projections['Values'] = [self._create_field_projection(field) for field in all_fields[:10]]
        
        elif visual_type == 'map':
            if dimensions:
                geo_dims = [d for d in dimensions if d.get('category') == 'geography']
                if geo_dims:
                    projections['Location'] = [self._create_field_projection(geo_dims[0])]
            if measures:
                projections['Size'] = [self._create_field_projection(measures[0])]
        
        return projections
    
    def _create_field_projection(self, field: Dict[str, Any]) -> Dict[str, Any]:
        """Create field projection for a visual."""
        return {
            "queryRef": f"{field.get('table', '')}.{field.get('column', '')}",
            "active": True
        }
    
    def _create_prototype_query(self, visual: Dict[str, Any], 
                              data_fields: Dict[str, Any]) -> Dict[str, Any]:
        """Create prototype query for a visual."""
        # This is a simplified prototype query
        # In practice, this would be much more complex
        return {
            "Version": 2,
            "From": [
                {
                    "Name": "table1",
                    "Entity": data_fields.get('measures', [{}])[0].get('table', 'Table') if data_fields.get('measures') else 'Table'
                }
            ],
            "Select": [],
            "Where": [],
            "OrderBy": []
        }
    
    def _create_visual_objects(self, visual: Dict[str, Any]) -> Dict[str, Any]:
        """Create visual formatting objects."""
        objects = {}
        
        # Add basic formatting based on visual type
        visual_type = visual.get('type', '')
        
        if visual_type in ['column_chart', 'line_chart']:
            objects['categoryAxis'] = [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "axisType": {"expr": {"Literal": {"Value": "'Categorical'"}}}
                    }
                }
            ]
            objects['valueAxis'] = [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "axisType": {"expr": {"Literal": {"Value": "'Linear'"}}}
                    }
                }
            ]
        
        elif visual_type == 'card':
            objects['labels'] = [
                {
                    "properties": {
                        "fontSize": {"expr": {"Literal": {"Value": "20D"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#000000'"}}}}}
                    }
                }
            ]
        
        return objects
    
    def _create_report_filters(self, report_layout: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create report-level filters."""
        filters = []
        
        for filter_def in report_layout.get('filters', []):
            if filter_def.get('position') == 'top':
                filter_obj = {
                    "name": f"Filter_{filter_def.get('field', {}).get('column', '')}",
                    "field": filter_def.get('field', {}),
                    "type": filter_def.get('type', 'dropdown'),
                    "defaultValue": filter_def.get('default_value'),
                    "multiSelect": filter_def.get('multi_select', False)
                }
                filters.append(filter_obj)
        
        return filters
    
    def _create_page_filters(self, page: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create page-level filters."""
        # For now, return empty - page-specific filters can be added here
        return []
    
    def _create_theme_file(self, report_layout: Dict[str, Any], static_dir: str) -> str:
        """Create theme file for the report."""
        theme = {
            "name": "Custom Theme",
            "dataColors": [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", 
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
            ],
            "background": "#FFFFFF",
            "foreground": "#000000",
            "tableAccent": "#EEEEEE"
        }
        
        # Override with layout-specific theme if provided
        layout_theme = report_layout.get('theme', {})
        if 'color_palette' in layout_theme:
            theme['dataColors'] = layout_theme['color_palette']
        
        theme_file = os.path.join(static_dir, "RegisteredResources", "theme.json")
        os.makedirs(os.path.dirname(theme_file), exist_ok=True)
        
        with open(theme_file, 'w', encoding='utf-8') as f:
            json.dump(theme, f, indent=2)
        
        return theme_file
    
    def _generate_data_sources(self, data_sources: List[Dict[str, Any]], 
                              project_dir: str) -> List[str]:
        """Generate data source files."""
        files_created = []
        
        # Create dataset directory
        dataset_dir = os.path.join(project_dir, f"{os.path.basename(project_dir)}.Dataset")
        
        # Create data sources definition
        datasources_def = {
            "dataSources": []
        }
        
        for ds in data_sources:
            datasource_def = {
                "type": self._map_datasource_type(ds.get('type', '')),
                "name": ds.get('name', ''),
                "connectionDetails": self._create_connection_details(ds)
            }
            datasources_def["dataSources"].append(datasource_def)
        
        datasources_file = os.path.join(dataset_dir, "dataSources.json")
        with open(datasources_file, 'w', encoding='utf-8') as f:
            json.dump(datasources_def, f, indent=2)
        files_created.append(datasources_file)
        
        return files_created
    
    def _generate_metadata(self, project_name: str, project_dir: str) -> List[str]:
        """Generate metadata files."""
        files_created = []
        
        # Create .pbitool.json for compatibility
        pbitool_config = {
            "version": "1.0.0",
            "settings": {
                "PBIDesktop": {
                    "generatePbip": True
                }
            }
        }
        
        pbitool_file = os.path.join(project_dir, ".pbitool.json")
        with open(pbitool_file, 'w', encoding='utf-8') as f:
            json.dump(pbitool_config, f, indent=2)
        files_created.append(pbitool_file)
        
        # Create .gitignore for version control
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
        
        gitignore_file = os.path.join(project_dir, ".gitignore")
        with open(gitignore_file, 'w', encoding='utf-8') as f:
            f.write(gitignore_content)
        files_created.append(gitignore_file)
        
        return files_created
    
    def _create_pbip_file(self, project_name: str, project_dir: str) -> str:
        """Create the main .pbip file."""
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
        
        pbip_file = os.path.join(project_dir, f"{project_name}.pbip")
        with open(pbip_file, 'w', encoding='utf-8') as f:
            json.dump(pbip_content, f, indent=2)
        
        return pbip_file
    
    # Helper methods for data type mapping
    
    def _map_data_type(self, source_type: str) -> str:
        """Map source data type to Power BI data type."""
        type_map = {
            'int': 'int64',
            'integer': 'int64',
            'bigint': 'int64',
            'smallint': 'int64',
            'float': 'double',
            'real': 'double',
            'decimal': 'decimal',
            'money': 'decimal',
            'varchar': 'string',
            'char': 'string',
            'text': 'string',
            'nvarchar': 'string',
            'nchar': 'string',
            'date': 'dateTime',
            'datetime': 'dateTime',
            'timestamp': 'dateTime',
            'bit': 'boolean',
            'boolean': 'boolean'
        }
        
        return type_map.get(source_type.lower(), 'string')
    
    def _map_to_m_type(self, source_type: str) -> str:
        """Map source data type to M type."""
        type_map = {
            'int': 'Int64.Type',
            'integer': 'Int64.Type',
            'bigint': 'Int64.Type',
            'smallint': 'Int64.Type',
            'float': 'Number.Type',
            'real': 'Number.Type',
            'decimal': 'Currency.Type',
            'money': 'Currency.Type',
            'varchar': 'Text.Type',
            'char': 'Text.Type',
            'text': 'Text.Type',
            'nvarchar': 'Text.Type',
            'nchar': 'Text.Type',
            'date': 'Date.Type',
            'datetime': 'DateTime.Type',
            'timestamp': 'DateTime.Type',
            'bit': 'Logical.Type',
            'boolean': 'Logical.Type'
        }
        
        return type_map.get(source_type.lower(), 'Text.Type')
    
    def _map_cross_filter_direction(self, direction: str) -> str:
        """Map cross filter direction to BIM format."""
        direction_map = {
            'single': 'OneDirection',
            'both': 'BothDirections',
            'none': 'None'
        }
        
        return direction_map.get(direction.lower(), 'OneDirection')
    
    def _map_cardinality(self, cardinality: str) -> str:
        """Map cardinality to BIM format."""
        cardinality_map = {
            'one_to_one': 'OneToOne',
            'one_to_many': 'OneToMany',
            'many_to_one': 'ManyToOne',
            'many_to_many': 'ManyToMany'
        }
        
        return cardinality_map.get(cardinality.lower(), 'ManyToOne')
    
    def _get_format_string(self, format_type: str) -> str:
        """Get format string for measures."""
        format_map = {
            'currency': '$#,0.00',
            'percentage': '0.00%',
            'whole_number': '#,0',
            'general_number': '#,0.00',
            'text': ''
        }
        
        return format_map.get(format_type.lower(), '#,0.00')
    
    def _map_datasource_type(self, source_type: str) -> str:
        """Map data source type to Power BI data source type."""
        type_map = {
            'sql_server': 'SqlServer',
            'postgresql': 'PostgreSQL',
            'mysql': 'MySQL',
            'oracle': 'Oracle',
            'csv': 'Csv',
            'excel': 'Excel',
            'api': 'Web',
            'bigquery': 'GoogleBigQuery',
            'salesforce': 'Salesforce',
            'azure_sql': 'AzureSqlDatabase',
            'synapse': 'AzureSynapseAnalytics'
        }
        
        return type_map.get(source_type.lower(), 'Csv')
    
    def _create_connection_details(self, data_source: Dict[str, Any]) -> Dict[str, Any]:
        """Create connection details for a data source."""
        ds_type = data_source.get('type', '')
        
        if ds_type == 'sql_server':
            return {
                "server": data_source.get('server', ''),
                "database": data_source.get('database', ''),
                "authenticationType": "Windows"
            }
        elif ds_type == 'csv':
            return {
                "path": data_source.get('file_path', ''),
                "delimiter": ",",
                "encoding": "UTF-8"
            }
        elif ds_type == 'api':
            return {
                "url": data_source.get('url', ''),
                "method": "GET",
                "headers": data_source.get('headers', {})
            }
        else:
            return {}
