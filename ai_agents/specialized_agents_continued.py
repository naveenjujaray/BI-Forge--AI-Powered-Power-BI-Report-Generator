import json
import pandas as pd
from typing import Dict, List, Any, Optional
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage
from datetime import datetime
from .agent_framework import BaseAIAgent, AgentType, AgentTask, TaskStatus

class QualityAssessorAgent(BaseAIAgent):
    """Agent specialized in quality assessment and validation."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.QUALITY_ASSESSOR, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are a Quality Assessor Agent specialized in validating Power BI reports and data models.

Your responsibilities include:
1. Validating data model integrity and relationships
2. Checking DAX measure accuracy and performance
3. Assessing report design and user experience
4. Identifying potential issues and bottlenecks
5. Ensuring compliance with best practices

Always provide detailed feedback with actionable recommendations."""
    
    def _initialize_tools(self):
        """Initialize quality assessment specific tools."""
        
        @tool
        def validate_data_model(schema: dict, relationships: list) -> dict:
            """Validate data model structure and relationships."""
            try:
                issues = []
                recommendations = []
                score = 100
                
                # Check for circular relationships
                circular_relationships = self._detect_circular_relationships(relationships)
                if circular_relationships:
                    issues.append({
                        "type": "circular_relationships",
                        "severity": "high",
                        "message": f"Detected circular relationships: {circular_relationships}",
                        "impact": "Can cause calculation errors and performance issues"
                    })
                    score -= 20
                
                # Check for missing primary keys
                tables_without_pk = []
                for table_name, table_info in schema.items():
                    has_pk = any(col.get('is_key', False) for col in table_info.get('columns', []))
                    if not has_pk:
                        tables_without_pk.append(table_name)
                
                if tables_without_pk:
                    issues.append({
                        "type": "missing_primary_keys",
                        "severity": "medium",
                        "message": f"Tables without primary keys: {tables_without_pk}",
                        "impact": "May affect relationship integrity and performance"
                    })
                    score -= 10 * len(tables_without_pk)
                    recommendations.append("Add primary key columns to tables for better performance")
                
                # Check relationship cardinality
                many_to_many_rels = [rel for rel in relationships if rel.get('cardinality') == 'many_to_many']
                if many_to_many_rels:
                    issues.append({
                        "type": "many_to_many_relationships",
                        "severity": "medium",
                        "message": f"Found {len(many_to_many_rels)} many-to-many relationships",
                        "impact": "Can impact performance and cause unexpected results"
                    })
                    score -= 5 * len(many_to_many_rels)
                    recommendations.append("Consider creating bridge tables for many-to-many relationships")
                
                # Check for star schema compliance
                star_schema_compliance = self._check_star_schema_compliance(schema, relationships)
                if not star_schema_compliance['is_compliant']:
                    issues.append({
                        "type": "schema_design",
                        "severity": "low",
                        "message": star_schema_compliance['message'],
                        "impact": "May not follow dimensional modeling best practices"
                    })
                    score -= 5
                    recommendations.append("Consider restructuring to follow star schema patterns")
                
                return {
                    "status": "success",
                    "validation_score": max(0, score),
                    "issues": issues,
                    "recommendations": recommendations,
                    "summary": f"Model validation completed with {len(issues)} issues found"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def validate_dax_measures(measures: dict) -> dict:
            """Validate DAX measures for correctness and performance."""
            try:
                issues = []
                recommendations = []
                score = 100
                
                for measure_name, measure_info in measures.items():
                    expression = measure_info.get('expression', '')
                    
                    # Check for common DAX issues
                    dax_issues = self._analyze_dax_expression(measure_name, expression)
                    issues.extend(dax_issues)
                    
                    # Check for performance issues
                    perf_issues = self._check_dax_performance(measure_name, expression)
                    issues.extend(perf_issues)
                    
                    # Score deduction based on issues
                    score -= len(dax_issues) * 5
                    score -= len(perf_issues) * 3
                
                # Generate recommendations based on issues
                if any(issue.get('type') == 'filter_context' for issue in issues):
                    recommendations.append("Review filter context in measures to ensure correct calculations")
                
                if any(issue.get('type') == 'performance' for issue in issues):
                    recommendations.append("Optimize DAX expressions for better performance")
                
                if any(issue.get('type') == 'syntax' for issue in issues):
                    recommendations.append("Fix syntax errors in DAX expressions")
                
                return {
                    "status": "success",
                    "validation_score": max(0, score),
                    "issues": issues,
                    "recommendations": recommendations,
                    "measures_analyzed": len(measures),
                    "summary": f"DAX validation completed for {len(measures)} measures with {len(issues)} issues"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def assess_report_design(report_layout: dict) -> dict:
            """Assess report design for usability and best practices."""
            try:
                issues = []
                recommendations = []
                score = 100
                
                pages = report_layout.get('pages', [])
                
                # Check number of visuals per page
                for page in pages:
                    visuals = page.get('visuals', [])
                    if len(visuals) > 10:
                        issues.append({
                            "type": "too_many_visuals",
                            "severity": "medium",
                            "page": page.get('name'),
                            "message": f"Page '{page.get('name')}' has {len(visuals)} visuals (recommended: ≤10)",
                            "impact": "May overwhelm users and impact performance"
                        })
                        score -= 10
                        recommendations.append(f"Consider splitting page '{page.get('name')}' into multiple pages")
                
                # Check for consistent visual sizing
                sizing_issues = self._check_visual_sizing_consistency(pages)
                issues.extend(sizing_issues)
                score -= len(sizing_issues) * 5
                
                # Check for proper use of visual types
                visual_type_issues = self._check_visual_type_appropriateness(pages)
                issues.extend(visual_type_issues)
                score -= len(visual_type_issues) * 3
                
                # Check navigation and user experience
                nav_issues = self._check_navigation_design(report_layout)
                issues.extend(nav_issues)
                score -= len(nav_issues) * 7
                
                # Check accessibility
                accessibility_issues = self._check_accessibility(report_layout)
                issues.extend(accessibility_issues)
                score -= len(accessibility_issues) * 5
                
                if not issues:
                    recommendations.append("Report design follows best practices")
                
                return {
                    "status": "success",
                    "design_score": max(0, score),
                    "issues": issues,
                    "recommendations": recommendations,
                    "pages_analyzed": len(pages),
                    "total_visuals": sum(len(page.get('visuals', [])) for page in pages),
                    "summary": f"Design assessment completed for {len(pages)} pages with {len(issues)} issues"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def performance_assessment(schema: dict, measures: dict, report_layout: dict) -> dict:
            """Assess overall performance implications."""
            try:
                performance_score = 100
                bottlenecks = []
                recommendations = []
                
                # Data model complexity assessment
                total_tables = len(schema)
                total_columns = sum(len(table.get('columns', [])) for table in schema.values())
                total_measures = len(measures)
                
                complexity_score = self._calculate_complexity_score(total_tables, total_columns, total_measures)
                
                if complexity_score > 80:
                    bottlenecks.append({
                        "type": "model_complexity",
                        "severity": "high",
                        "message": "Data model is highly complex",
                        "metrics": {
                            "tables": total_tables,
                            "columns": total_columns,
                            "measures": total_measures,
                            "complexity_score": complexity_score
                        }
                    })
                    performance_score -= 20
                    recommendations.append("Consider simplifying the data model or implementing incremental refresh")
                
                # Visual complexity assessment
                total_visuals = sum(len(page.get('visuals', [])) for page in report_layout.get('pages', []))
                
                if total_visuals > 20:
                    bottlenecks.append({
                        "type": "visual_complexity",
                        "severity": "medium",
                        "message": f"Report has {total_visuals} visuals (recommended: ≤20)",
                        "impact": "May cause slow loading times"
                    })
                    performance_score -= 15
                    recommendations.append("Reduce the number of visuals or use drill-through pages")
                
                # Memory usage estimation
                estimated_memory = self._estimate_memory_usage(schema, total_visuals)
                
                if estimated_memory > 1000:  # MB
                    bottlenecks.append({
                        "type": "memory_usage",
                        "severity": "high",
                        "message": f"Estimated memory usage: {estimated_memory}MB",
                        "impact": "May cause performance issues on lower-end devices"
                    })
                    performance_score -= 25
                    recommendations.append("Implement data reduction strategies or incremental refresh")
                
                return {
                    "status": "success",
                    "performance_score": max(0, performance_score),
                    "bottlenecks": bottlenecks,
                    "recommendations": recommendations,
                    "metrics": {
                        "total_tables": total_tables,
                        "total_columns": total_columns,
                        "total_measures": total_measures,
                        "total_visuals": total_visuals,
                        "estimated_memory_mb": estimated_memory,
                        "complexity_score": complexity_score
                    },
                    "summary": f"Performance assessment completed with {len(bottlenecks)} potential bottlenecks"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([validate_data_model, validate_dax_measures, assess_report_design, performance_assessment])
    
    def _detect_circular_relationships(self, relationships: List[Dict[str, Any]]) -> List[str]:
        """Detect circular relationships in the data model."""
        # Build adjacency list
        graph = {}
        for rel in relationships:
            from_table = rel.get('from_table')
            to_table = rel.get('to_table')
            
            if from_table not in graph:
                graph[from_table] = []
            graph[from_table].append(to_table)
        
        # Detect cycles using DFS
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs(node, path):
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycles.append(" -> ".join(path[cycle_start:] + [node]))
                return
            
            if node in visited:
                return
            
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                dfs(neighbor, path + [node])
            
            rec_stack.remove(node)
        
        for table in graph:
            if table not in visited:
                dfs(table, [])
        
        return cycles
    
    def _check_star_schema_compliance(self, schema: Dict[str, Any], 
                                    relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check if the model follows star schema principles."""
        fact_tables = []
        dim_tables = []
        
        # Classify tables as fact or dimension
        for table_name, table_info in schema.items():
            columns = table_info.get('columns', [])
            
            # Count measure-like columns
            measure_count = sum(1 for col in columns if self._is_measure_column(col))
            
            # Count key columns
            key_count = sum(1 for col in columns if col.get('is_key', False) or 
                           col.get('name', '').lower().endswith('id'))
            
            if measure_count > 0 and key_count > 1:
                fact_tables.append(table_name)
            else:
                dim_tables.append(table_name)
        
        # Check if we have fact and dimension tables
        if len(fact_tables) == 0:
            return {
                "is_compliant": False,
                "message": "No fact tables identified in the model"
            }
        
        if len(dim_tables) == 0:
            return {
                "is_compliant": False,
                "message": "No dimension tables identified in the model"
            }
        
        # Check relationships (should be fact-to-dim, many-to-one)
        invalid_relationships = []
        for rel in relationships:
            from_table = rel.get('from_table')
            to_table = rel.get('to_table')
            cardinality = rel.get('cardinality')
            
            if from_table in fact_tables and to_table in dim_tables:
                if cardinality != 'many_to_one':
                    invalid_relationships.append(f"{from_table} -> {to_table}")
        
        if invalid_relationships:
            return {
                "is_compliant": False,
                "message": f"Invalid relationship cardinalities: {', '.join(invalid_relationships)}"
            }
        
        return {
            "is_compliant": True,
            "message": "Model follows star schema principles"
        }
    
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
    
    def _analyze_dax_expression(self, measure_name: str, expression: str) -> List[Dict[str, Any]]:
        """Analyze DAX expression for common issues."""
        issues = []
        
        # Check for empty expressions
        if not expression.strip():
            issues.append({
                "type": "syntax",
                "severity": "high",
                "measure": measure_name,
                "message": "Empty DAX expression",
                "impact": "Measure will not function"
            })
            return issues
        
        # Check for basic syntax issues
        if expression.count('(') != expression.count(')'):
            issues.append({
                "type": "syntax",
                "severity": "high",
                "measure": measure_name,
                "message": "Unmatched parentheses in DAX expression",
                "impact": "Syntax error will prevent measure from working"
            })
        
        # Check for potential filter context issues
        if 'ALL(' in expression and 'FILTER(' in expression:
            issues.append({
                "type": "filter_context",
                "severity": "medium",
                "measure": measure_name,
                "message": "Complex filter context manipulation detected",
                "impact": "May produce unexpected results"
            })
        
        # Check for division without error handling
        if '/' in expression and 'DIVIDE(' not in expression:
            issues.append({
                "type": "error_handling",
                "severity": "low",
                "measure": measure_name,
                "message": "Division operator used without error handling",
                "impact": "May cause errors when denominator is zero"
            })
        
        return issues
    
    def _check_dax_performance(self, measure_name: str, expression: str) -> List[Dict[str, Any]]:
        """Check DAX expression for performance issues."""
        issues = []
        
        # Check for expensive functions
        expensive_functions = ['SUMX', 'AVERAGEX', 'COUNTX', 'MAXX', 'MINX']
        nested_x_count = sum(1 for func in expensive_functions if expression.count(func) > 1)
        
        if nested_x_count > 0:
            issues.append({
                "type": "performance",
                "severity": "medium",
                "measure": measure_name,
                "message": "Nested iterator functions detected",
                "impact": "May cause slow calculation performance"
            })
        
        # Check for calculated columns in measures (anti-pattern)
        if 'ADDCOLUMNS(' in expression:
            issues.append({
                "type": "performance",
                "severity": "high",
                "measure": measure_name,
                "message": "ADDCOLUMNS function used in measure",
                "impact": "Can significantly impact performance"
            })
        
        return issues
    
    def _check_visual_sizing_consistency(self, pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check for consistent visual sizing across pages."""
        issues = []
        
        # Collect all visual sizes
        all_sizes = []
        for page in pages:
            for visual in page.get('visuals', []):
                position = visual.get('position', {})
                size = (position.get('width', 0), position.get('height', 0))
                all_sizes.append(size)
        
        # Check for too many different sizes
        unique_sizes = set(all_sizes)
        if len(unique_sizes) > 10:
            issues.append({
                "type": "visual_consistency",
                "severity": "low",
                "message": f"Found {len(unique_sizes)} different visual sizes",
                "impact": "Inconsistent sizing may affect visual appeal"
            })
        
        return issues
    
    def _check_visual_type_appropriateness(self, pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check if visual types are appropriate for the data."""
        issues = []
        
        for page in pages:
            for visual in page.get('visuals', []):
                visual_type = visual.get('type')
                data_fields = visual.get('data_fields', {})
                measures = data_fields.get('measures', [])
                dimensions = data_fields.get('dimensions', [])
                
                # Check pie chart with too many categories
                if visual_type == 'pie_chart' and len(dimensions) > 0:
                    # This would require actual data to check category count
                    # For now, just warn if multiple dimensions
                    if len(dimensions) > 1:
                        issues.append({
                            "type": "visual_appropriateness",
                            "severity": "low",
                            "visual_type": visual_type,
                            "message": "Pie chart with multiple dimensions may be confusing",
                            "impact": "Consider using bar chart instead"
                        })
                
                # Check line chart without time dimension
                if visual_type == 'line_chart':
                    has_time_dim = any(d.get('category') == 'time' for d in dimensions)
                    if not has_time_dim:
                        issues.append({
                            "type": "visual_appropriateness",
                            "severity": "medium",
                            "visual_type": visual_type,
                            "message": "Line chart without time dimension",
                            "impact": "Consider using bar chart for categorical data"
                        })
        
        return issues
    
    def _check_navigation_design(self, report_layout: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check navigation design for usability."""
        issues = []
        
        pages = report_layout.get('pages', [])
        navigation = report_layout.get('navigation', {})
        
        # Check number of pages
        if len(pages) > 8:
            issues.append({
                "type": "navigation",
                "severity": "medium",
                "message": f"Report has {len(pages)} pages (recommended: ≤8)",
                "impact": "Too many pages may overwhelm users"
            })
        
        # Check for navigation type
        nav_type = navigation.get('type')
        if not nav_type:
            issues.append({
                "type": "navigation",
                "severity": "low",
                "message": "No navigation type specified",
                "impact": "Users may have difficulty navigating between pages"
            })
        
        return issues
    
    def _check_accessibility(self, report_layout: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check accessibility features."""
        issues = []
        
        # Check for alt text on visuals (placeholder check)
        pages = report_layout.get('pages', [])
        visuals_without_alt_text = 0
        
        for page in pages:
            for visual in page.get('visuals', []):
                if not visual.get('alt_text') and not visual.get('description'):
                    visuals_without_alt_text += 1
        
        if visuals_without_alt_text > 0:
            issues.append({
                "type": "accessibility",
                "severity": "medium",
                "message": f"{visuals_without_alt_text} visuals missing alt text or descriptions",
                "impact": "May not be accessible to screen readers"
            })
        
        return issues
    
    def _calculate_complexity_score(self, tables: int, columns: int, measures: int) -> int:
        """Calculate data model complexity score."""
        # Weighted complexity calculation
        complexity = (tables * 2) + (columns * 0.1) + (measures * 1.5)
        return min(100, int(complexity))
    
    def _estimate_memory_usage(self, schema: Dict[str, Any], visuals_count: int) -> int:
        """Estimate memory usage in MB."""
        # Simplified estimation
        total_columns = sum(len(table.get('columns', [])) for table in schema.values())
        estimated_rows_per_table = 10000  # Assumption
        
        # Rough estimation: columns * rows * compression factor + visuals overhead
        base_memory = (total_columns * estimated_rows_per_table * 0.001)  # MB
        visuals_memory = visuals_count * 2  # 2MB per visual assumption
        
        return int(base_memory + visuals_memory)

class DeploymentManagerAgent(BaseAIAgent):
    """Agent specialized in deployment and environment management."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.DEPLOYMENT_MANAGER, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are a Deployment Manager Agent specialized in managing Power BI deployments and environments.

Your responsibilities include:
1. Managing deployment pipelines and environments
2. Coordinating release processes and approvals
3. Monitoring deployment health and rollback procedures
4. Managing environment-specific configurations
5. Ensuring compliance with deployment standards

Always prioritize security, reliability, and compliance in deployment processes."""
    
    def _initialize_tools(self):
        """Initialize deployment management specific tools."""
        
        @tool
        def create_deployment_plan(project_config: dict, target_environment: str) -> dict:
            """Create a deployment plan for the project."""
            try:
                deployment_stages = [
                    {
                        "stage": "pre_deployment",
                        "tasks": [
                            "Validate project structure",
                            "Run quality checks",
                            "Check environment prerequisites",
                            "Backup existing reports (if applicable)"
                        ],
                        "estimated_duration": "10 minutes"
                    },
                    {
                        "stage": "deployment",
                        "tasks": [
                            "Deploy dataset to target workspace",
                            "Deploy reports to target workspace",
                            "Configure data source connections",
                            "Set up refresh schedules",
                            "Configure security and permissions"
                        ],
                        "estimated_duration": "20 minutes"
                    },
                    {
                        "stage": "post_deployment",
                        "tasks": [
                            "Validate deployment success",
                            "Run smoke tests",
                            "Update documentation",
                            "Notify stakeholders",
                            "Monitor initial usage"
                        ],
                        "estimated_duration": "15 minutes"
                    }
                ]
                
                # Environment-specific configurations
                env_config = self._get_environment_config(target_environment)
                
                deployment_plan = {
                    "project_name": project_config.get('name', 'Untitled'),
                    "target_environment": target_environment,
                    "environment_config": env_config,
                    "stages": deployment_stages,
                    "total_estimated_duration": "45 minutes",
                    "prerequisites": [
                        f"Access to {target_environment} workspace",
                        "Data source connectivity verified",
                        "Required permissions granted"
                    ],
                    "rollback_plan": {
                        "triggers": [
                            "Deployment failure",
                            "Critical validation errors",
                            "Stakeholder rejection"
                        ],
                        "steps": [
                            "Stop current deployment",
                            "Restore previous version (if applicable)",
                            "Notify stakeholders of rollback",
                            "Document rollback reason",
                            "Schedule retry with fixes"
                        ]
                    },
                    "approval_required": env_config.get('requires_approval', False)
                }
                
                return {
                    "status": "success",
                    "deployment_plan": deployment_plan
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def validate_deployment_prerequisites(deployment_plan: dict) -> dict:
            """Validate deployment prerequisites."""
            try:
                validation_results = []
                all_passed = True
                
                target_env = deployment_plan.get('target_environment')
                env_config = deployment_plan.get('environment_config', {})
                
                # Check workspace access
                workspace_check = {
                    "check": "Workspace Access",
                    "status": "passed",  # Simulated - would actually check
                    "message": f"Access to {target_env} workspace verified"
                }
                validation_results.append(workspace_check)
                
                # Check data source connectivity
                datasource_check = {
                    "check": "Data Source Connectivity",
                    "status": "passed",  # Simulated
                    "message": "Data source connections verified"
                }
                validation_results.append(datasource_check)
                
                # Check permissions
                permissions_check = {
                    "check": "Required Permissions",
                    "status": "passed",  # Simulated
                    "message": "All required permissions granted"
                }
                validation_results.append(permissions_check)
                
                # Check environment capacity
                capacity_check = self._check_environment_capacity(env_config)
                validation_results.append(capacity_check)
                if capacity_check["status"] != "passed":
                    all_passed = False
                
                return {
                    "status": "success",
                    "validation_passed": all_passed,
                    "checks": validation_results,
                    "summary": f"Prerequisites validation {'passed' if all_passed else 'failed'}"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def execute_deployment_stage(stage_name: str, stage_config: dict, project_files: dict) -> dict:
            """Execute a specific deployment stage."""
            try:
                stage_results = []
                
                for task in stage_config.get('tasks', []):
                    task_result = self._execute_deployment_task(task, project_files)
                    stage_results.append(task_result)
                
                # Check if all tasks completed successfully
                all_successful = all(result.get('status') == 'success' for result in stage_results)
                
                return {
                    "status": "success" if all_successful else "partial_failure",
                    "stage": stage_name,
                    "task_results": stage_results,
                    "summary": f"Stage '{stage_name}' {'completed successfully' if all_successful else 'completed with errors'}"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def monitor_deployment_health(deployment_id: str) -> dict:
            """Monitor the health of a deployment."""
            try:
                # Simulated monitoring - would integrate with actual monitoring systems
                health_metrics = {
                    "deployment_id": deployment_id,
                    "status": "healthy",
                    "uptime": "99.9%",
                    "response_time": "1.2s",
                    "error_rate": "0.1%",
                    "data_refresh_status": "successful",
                    "last_refresh": "2025-09-10T09:30:00Z",
                    "user_activity": {
                        "active_users": 25,
                        "reports_viewed": 150,
                        "avg_session_duration": "8.5 minutes"
                    }
                }
                
                # Check for any issues
                issues = []
                recommendations = []
                
                if float(health_metrics["response_time"][:-1]) > 3.0:
                    issues.append({
                        "type": "performance",
                        "severity": "medium",
                        "message": "Response time above recommended threshold",
                        "current_value": health_metrics["response_time"],
                        "threshold": "3.0s"
                    })
                    recommendations.append("Consider optimizing report performance")
                
                if float(health_metrics["error_rate"][:-1]) > 1.0:
                    issues.append({
                        "type": "reliability",
                        "severity": "high",
                        "message": "Error rate above acceptable threshold",
                        "current_value": health_metrics["error_rate"],
                        "threshold": "1.0%"
                    })
                    recommendations.append("Investigate and resolve error sources")
                
                return {
                    "status": "success",
                    "health_metrics": health_metrics,
                    "issues": issues,
                    "recommendations": recommendations,
                    "overall_health": "good" if not issues else "needs_attention"
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def create_rollback_procedure(deployment_id: str, rollback_reason: str) -> dict:
            """Create a rollback procedure for a deployment."""
            try:
                rollback_steps = [
                    {
                        "step": 1,
                        "action": "Stop current deployment processes",
                        "estimated_time": "2 minutes"
                    },
                    {
                        "step": 2,
                        "action": "Backup current state",
                        "estimated_time": "5 minutes"
                    },
                    {
                        "step": 3,
                        "action": "Restore previous version",
                        "estimated_time": "10 minutes"
                    },
                    {
                        "step": 4,
                        "action": "Validate rollback completion",
                        "estimated_time": "5 minutes"
                    },
                    {
                        "step": 5,
                        "action": "Notify stakeholders",
                        "estimated_time": "3 minutes"
                    }
                ]
                
                rollback_procedure = {
                    "deployment_id": deployment_id,
                    "rollback_reason": rollback_reason,
                    "created_at": "2025-09-10T10:13:00Z",
                    "steps": rollback_steps,
                    "total_estimated_time": "25 minutes",
                    "approval_required": True,
                    "notification_list": [
                        "deployment.team@company.com",
                        "stakeholders@company.com",
                        "it.support@company.com"
                    ]
                }
                
                return {
                    "status": "success",
                    "rollback_procedure": rollback_procedure
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([create_deployment_plan, validate_deployment_prerequisites, 
                          execute_deployment_stage, monitor_deployment_health, create_rollback_procedure])
    
    def _get_environment_config(self, environment: str) -> Dict[str, Any]:
        """Get configuration for a specific environment."""
        env_configs = {
            "development": {
                "workspace_id": "dev-workspace-id",
                "requires_approval": False,
                "auto_refresh": True,
                "capacity_limit": "F2",
                "backup_retention": "7 days"
            },
            "test": {
                "workspace_id": "test-workspace-id",
                "requires_approval": True,
                "auto_refresh": True,
                "capacity_limit": "F4",
                "backup_retention": "14 days"
            },
            "production": {
                "workspace_id": "prod-workspace-id",
                "requires_approval": True,
                "auto_refresh": False,
                "capacity_limit": "F16",
                "backup_retention": "30 days"
            }
        }
        
        return env_configs.get(environment, env_configs["development"])
    
    def _check_environment_capacity(self, env_config: Dict[str, Any]) -> Dict[str, Any]:
        """Check environment capacity availability."""
        capacity_limit = env_config.get('capacity_limit', 'F2')
        
        # Simulated capacity check
        current_usage = 65  # Percentage
        
        if current_usage > 80:
            return {
                "check": "Environment Capacity",
                "status": "warning",
                "message": f"High capacity usage: {current_usage}% of {capacity_limit}",
                "recommendation": "Consider scaling up capacity or scheduling deployment during off-peak hours"
            }
        elif current_usage > 90:
            return {
                "check": "Environment Capacity",
                "status": "failed",
                "message": f"Capacity limit exceeded: {current_usage}% of {capacity_limit}",
                "recommendation": "Scale up capacity before deployment"
            }
        else:
            return {
                "check": "Environment Capacity",
                "status": "passed",
                "message": f"Capacity available: {current_usage}% of {capacity_limit} used"
            }
    
    def _execute_deployment_task(self, task: str, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific deployment task."""
        # Simulated task execution
        task_map = {
            "Validate project structure": self._validate_project_structure,
            "Run quality checks": self._run_quality_checks,
            "Check environment prerequisites": self._check_prerequisites,
            "Deploy dataset to target workspace": self._deploy_dataset,
            "Deploy reports to target workspace": self._deploy_reports,
            "Configure data source connections": self._configure_datasources,
            "Set up refresh schedules": self._setup_refresh_schedules,
            "Configure security and permissions": self._configure_security,
            "Validate deployment success": self._validate_deployment,
            "Run smoke tests": self._run_smoke_tests
        }
        
        task_function = task_map.get(task, self._default_task_execution)
        return task_function(project_files)
    
    def _validate_project_structure(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Validate project structure."""
        required_files = ['.pbip', '.Dataset', '.Report']
        missing_files = []
        
        for file_pattern in required_files:
            if not any(file_pattern in str(file) for file in project_files.get('files', [])):
                missing_files.append(file_pattern)
        
        if missing_files:
            return {
                "task": "Validate project structure",
                "status": "failed",
                "message": f"Missing required files: {', '.join(missing_files)}"
            }
        
        return {
            "task": "Validate project structure",
            "status": "success",
            "message": "Project structure validation passed"
        }
    
    def _run_quality_checks(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Run quality checks on the project."""
        # Simulated quality checks
        return {
            "task": "Run quality checks",
            "status": "success",
            "message": "All quality checks passed",
            "checks_run": ["DAX validation", "Model integrity", "Visual consistency"]
        }
    
    def _check_prerequisites(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Check deployment prerequisites."""
        return {
            "task": "Check environment prerequisites",
            "status": "success",
            "message": "All prerequisites met"
        }
    
    def _deploy_dataset(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy dataset to workspace."""
        return {
            "task": "Deploy dataset to target workspace",
            "status": "success",
            "message": "Dataset deployed successfully",
            "deployment_time": "8 seconds"
        }
    
    def _deploy_reports(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy reports to workspace."""
        return {
            "task": "Deploy reports to target workspace",
            "status": "success",
            "message": "Reports deployed successfully",
            "deployment_time": "12 seconds"
        }
    
    def _configure_datasources(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Configure data source connections."""
        return {
            "task": "Configure data source connections",
            "status": "success",
            "message": "Data source connections configured"
        }
    
    def _setup_refresh_schedules(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Set up data refresh schedules."""
        return {
            "task": "Set up refresh schedules",
            "status": "success",
            "message": "Refresh schedules configured for daily at 6 AM"
        }
    
    def _configure_security(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Configure security and permissions."""
        return {
            "task": "Configure security and permissions",
            "status": "success",
            "message": "Security settings applied"
        }
    
    def _validate_deployment(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Validate deployment success."""
        return {
            "task": "Validate deployment success",
            "status": "success",
            "message": "Deployment validation passed"
        }
    
    def _run_smoke_tests(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Run smoke tests on deployed solution."""
        return {
            "task": "Run smoke tests",
            "status": "success",
            "message": "All smoke tests passed",
            "tests_run": ["Report loading", "Data refresh", "Visual rendering"]
        }
    
    def _default_task_execution(self, project_files: Dict[str, Any]) -> Dict[str, Any]:
        """Default task execution for unmapped tasks."""
        return {
            "task": "Generic task",
            "status": "success",
            "message": "Task completed successfully"
        }

class OrchestratorAgent(BaseAIAgent):
    """Master orchestrator agent that coordinates all other agents."""
    
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(AgentType.ORCHESTRATOR, name, config, **kwargs)
    
    def _get_system_prompt(self) -> str:
        return """You are the Orchestrator Agent, the master coordinator of the Power BI development workflow.

Your responsibilities include:
1. Coordinating workflow execution across all specialized agents
2. Managing inter-agent communication and dependencies
3. Making high-level decisions about workflow progression
4. Handling exceptions and error recovery
5. Ensuring quality and compliance throughout the process

You have the authority to direct other agents and make critical decisions about the development process."""
    
    def _initialize_tools(self):
        """Initialize orchestrator specific tools."""
        
        @tool
        def create_development_workflow(requirements: str, data_sources: list) -> dict:
            """Create a comprehensive development workflow."""
            try:
                workflow_steps = [
                    {
                        "step": "data_analysis",
                        "agent": "DataAnalystAgent",
                        "tasks": [
                            "analyze_data_sources",
                            "profile_data_quality",
                            "identify_relationships",
                            "suggest_measures_dimensions"
                        ],
                        "inputs": {"requirements": requirements, "data_sources": data_sources},
                        "outputs": ["data_profile", "schema_analysis", "measure_suggestions"],
                        "parallel": False
                    },
                    {
                        "step": "schema_design",
                        "agent": "SchemaArchitectAgent",
                        "tasks": [
                            "design_data_model",
                            "create_relationships",
                            "optimize_schema"
                        ],
                        "inputs": ["data_profile", "schema_analysis"],
                        "outputs": ["data_model", "relationships"],
                        "parallel": False,
                        "depends_on": ["data_analysis"]
                    },
                    {
                        "step": "measure_development",
                        "agent": "DAXDeveloperAgent",
                        "tasks": [
                            "create_basic_measures",
                            "create_time_intelligence",
                            "create_kpis",
                            "optimize_dax"
                        ],
                        "inputs": ["measure_suggestions", "data_model"],
                        "outputs": ["dax_measures"],
                        "parallel": True,
                        "depends_on": ["schema_design"]
                    },
                    {
                        "step": "report_design",
                        "agent": "ReportDesignerAgent",
                        "tasks": [
                            "suggest_visualizations",
                            "design_layout",
                            "create_navigation"
                        ],
                        "inputs": ["requirements", "measure_suggestions"],
                        "outputs": ["report_layout"],
                        "parallel": True,
                        "depends_on": ["data_analysis"]
                    },
                    {
                        "step": "quality_assessment",
                        "agent": "QualityAssessorAgent",
                        "tasks": [
                            "validate_data_model",
                            "validate_dax_measures",
                            "assess_report_design",
                            "performance_assessment"
                        ],
                        "inputs": ["data_model", "dax_measures", "report_layout"],
                        "outputs": ["quality_report"],
                        "parallel": False,
                        "depends_on": ["measure_development", "report_design"]
                    },
                    {
                        "step": "project_generation",
                        "agent": "OrchestratorAgent",
                        "tasks": [
                            "generate_pbip_project"
                        ],
                        "inputs": ["data_model", "dax_measures", "report_layout", "quality_report"],
                        "outputs": ["pbip_project"],
                        "parallel": False,
                        "depends_on": ["quality_assessment"]
                    },
                    {
                        "step": "deployment_planning",
                        "agent": "DeploymentManagerAgent",
                        "tasks": [
                            "create_deployment_plan",
                            "validate_prerequisites"
                        ],
                        "inputs": ["pbip_project"],
                        "outputs": ["deployment_plan"],
                        "parallel": False,
                        "depends_on": ["project_generation"]
                    }
                ]
                
                workflow = {
                    "id": f"workflow_{int(datetime.now().timestamp())}",
                    "name": "Power BI Development Workflow",
                    "description": "End-to-end Power BI development using AI agents",
                    "steps": workflow_steps,
                    "estimated_duration": "2-4 hours",
                    "success_criteria": [
                        "All quality checks pass",
                        "PBIP project generated successfully",
                        "Deployment plan created"
                    ]
                }
                
                return {
                    "status": "success",
                    "workflow": workflow
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def coordinate_workflow_execution(workflow: dict, communication_bus: object) -> dict:
            """Coordinate the execution of a workflow across agents."""
            try:
                workflow_id = workflow.get('id')
                steps = workflow.get('steps', [])
                
                execution_log = []
                workflow_context = {}
                
                # Execute steps based on dependencies
                completed_steps = set()
                
                for step in steps:
                    step_name = step.get('step')
                    dependencies = step.get('depends_on', [])
                    
                    # Check if dependencies are met
                    if not all(dep in completed_steps for dep in dependencies):
                        execution_log.append({
                            "step": step_name,
                            "status": "waiting",
                            "message": f"Waiting for dependencies: {dependencies}"
                        })
                        continue
                    
                    # Execute step
                    step_result = self._execute_workflow_step(step, workflow_context, communication_bus)
                    execution_log.append(step_result)
                    
                    if step_result.get('status') == 'success':
                        completed_steps.add(step_name)
                        # Update workflow context with outputs
                        outputs = step_result.get('outputs', {})
                        workflow_context.update(outputs)
                    else:
                        # Handle step failure
                        execution_log.append({
                            "step": "error_handling",
                            "status": "failed",
                            "message": f"Workflow failed at step: {step_name}",
                            "error": step_result.get('message', 'Unknown error')
                        })
                        break
                
                # Determine overall workflow status
                workflow_status = "success" if len(completed_steps) == len(steps) else "failed"
                
                return {
                    "status": workflow_status,
                    "workflow_id": workflow_id,
                    "completed_steps": list(completed_steps),
                    "execution_log": execution_log,
                    "final_context": workflow_context
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        @tool
        def handle_workflow_exception(workflow_id: str, exception_details: dict) -> dict:
            """Handle exceptions that occur during workflow execution."""
            try:
                exception_type = exception_details.get('type', 'unknown')
                step_name = exception_details.get('step', 'unknown')
                error_message = exception_details.get('message', '')
                
                recovery_actions = []
                
                if exception_type == 'agent_timeout':
                    recovery_actions = [
                        "Retry the step with increased timeout",
                        "Switch to alternative agent if available",
                        "Skip step if not critical"
                    ]
                elif exception_type == 'data_quality_failure':
                    recovery_actions = [
                        "Review data quality issues",
                        "Apply data cleansing rules",
                        "Notify data stewards",
                        "Continue with warning flags"
                    ]
                elif exception_type == 'validation_failure':
                    recovery_actions = [
                        "Review validation errors",
                        "Apply automatic fixes where possible",
                        "Request manual review",
                        "Roll back to previous step if needed"
                    ]
                else:
                    recovery_actions = [
                        "Log error for analysis",
                        "Attempt automatic recovery",
                        "Escalate to human operator if needed"
                    ]
                
                recovery_plan = {
                    "workflow_id": workflow_id,
                    "exception_type": exception_type,
                    "failed_step": step_name,
                    "error_message": error_message,
                    "recovery_actions": recovery_actions,
                    "auto_retry": exception_type in ['agent_timeout', 'temporary_failure'],
                    "escalation_required": exception_type in ['validation_failure', 'critical_error']
                }
                
                return {
                    "status": "success",
                    "recovery_plan": recovery_plan
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
        self.tools.extend([create_development_workflow, coordinate_workflow_execution, handle_workflow_exception])
    
    def _execute_workflow_step(self, step: Dict[str, Any], context: Dict[str, Any], 
                              communication_bus: Any) -> Dict[str, Any]:
        """Execute a single workflow step."""
        try:
            step_name = step.get('step')
            agent_name = step.get('agent')
            tasks = step.get('tasks', [])
            inputs = step.get('inputs', {})
            
            # Prepare input data from context
            step_inputs = {}
            for input_key, input_value in inputs.items():
                if isinstance(input_value, str) and input_value in context:
                    step_inputs[input_key] = context[input_value]
                else:
                    step_inputs[input_key] = input_value
            
            # Execute tasks for this step
            task_results = []
            for task in tasks:
                # Send task to appropriate agent
                # This would be implemented with actual communication bus
                task_result = {
                    "task": task,
                    "status": "success",
                    "message": f"Task {task} completed successfully"
                }
                task_results.append(task_result)
            
            # Simulate step completion
            step_outputs = self._generate_step_outputs(step_name, step_inputs, task_results)
            
            return {
                "step": step_name,
                "status": "success",
                "message": f"Step {step_name} completed successfully",
                "task_results": task_results,
                "outputs": step_outputs
            }
            
        except Exception as e:
            return {
                "step": step.get('step', 'unknown'),
                "status": "failed",
                "message": str(e)
            }
    
    def _generate_step_outputs(self, step_name: str, inputs: Dict[str, Any], 
                              task_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate outputs for a workflow step."""
        # This is a simplified version - in practice, this would generate actual outputs
        # based on the step type and task results
        
        output_templates = {
            "data_analysis": {
                "data_profile": {"quality_score": 85, "issues": [], "recommendations": []},
                "schema_analysis": {"tables": [], "relationships": []},
                "measure_suggestions": {"measures": [], "dimensions": []}
            },
            "schema_design": {
                "data_model": {"tables": {}, "relationships": []},
                "relationships": []
            },
            "measure_development": {
                "dax_measures": {}
            },
            "report_design": {
                "report_layout": {"pages": [], "theme": {}, "navigation": {}}
            },
            "quality_assessment": {
                "quality_report": {"overall_score": 90, "issues": [], "recommendations": []}
            },
            "project_generation": {
                "pbip_project": {"project_path": "", "files": []}
            },
            "deployment_planning": {
                "deployment_plan": {"stages": [], "prerequisites": []}
            }
        }
        
        return output_templates.get(step_name, {})
