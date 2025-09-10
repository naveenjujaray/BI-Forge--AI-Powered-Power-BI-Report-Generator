import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

from ai_agents.agent_framework import (
    AgentCommunicationBus, WorkflowEngine, AgentState, AgentMessage
)
from ai_agents.specialized_agents import (
    DataAnalystAgent, ReportDesignerAgent, DAXDeveloperAgent, SchemaArchitectAgent
)
from ai_agents.specialized_agents_continued import (
    QualityAssessorAgent, DeploymentManagerAgent, OrchestratorAgent
)
from powerbi_generator.pbip_generator import PBIPGenerator

logger = logging.getLogger(__name__)

class PowerBIWorkflowOrchestrator:
    """Main orchestrator for the Power BI agentic workflow."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.communication_bus = AgentCommunicationBus()
        self.workflow_engine = WorkflowEngine(self.communication_bus)
        self.pbip_generator = PBIPGenerator(config)
        
        # Initialize agents
        self.agents = {}
        self._initialize_agents()
        
        # Define workflows
        self._define_workflows()
        
        logger.info("Power BI Workflow Orchestrator initialized")
    
    def _initialize_agents(self):
        """Initialize all specialized agents."""
        try:
            # Data Analyst Agent
            self.agents['data_analyst'] = DataAnalystAgent(
                name="DataAnalystAgent",
                config=self.config
            )
            
            # Report Designer Agent
            self.agents['report_designer'] = ReportDesignerAgent(
                name="ReportDesignerAgent",
                config=self.config
            )
            
            # DAX Developer Agent
            self.agents['dax_developer'] = DAXDeveloperAgent(
                name="DAXDeveloperAgent",
                config=self.config
            )
            
            # Schema Architect Agent
            self.agents['schema_architect'] = SchemaArchitectAgent(
                name="SchemaArchitectAgent",
                config=self.config
            )
            
            # Quality Assessor Agent
            self.agents['quality_assessor'] = QualityAssessorAgent(
                name="QualityAssessorAgent",
                config=self.config
            )
            
            # Deployment Manager Agent
            self.agents['deployment_manager'] = DeploymentManagerAgent(
                name="DeploymentManagerAgent",
                config=self.config
            )
            
            # Orchestrator Agent
            self.agents['orchestrator'] = OrchestratorAgent(
                name="OrchestratorAgent",
                config=self.config
            )
            
            # Register agents with communication bus
            for agent in self.agents.values():
                self.communication_bus.register_agent(agent)
            
            # Set up event subscriptions
            self._setup_event_subscriptions()
            
        except Exception as e:
            logger.error(f"Error initializing agents: {str(e)}")
            raise
    
    def _setup_event_subscriptions(self):
        """Set up event subscriptions between agents."""
        # Data quality events
        self.communication_bus.subscribe_to_event("DataAnalystAgent", "data_quality_issue")
        self.communication_bus.subscribe_to_event("QualityAssessorAgent", "data_quality_issue")
        
        # Schema change events
        self.communication_bus.subscribe_to_event("SchemaArchitectAgent", "schema_updated")
        self.communication_bus.subscribe_to_event("DataAnalystAgent", "schema_updated")
        self.communication_bus.subscribe_to_event("DAXDeveloperAgent", "schema_updated")
        
        # Deployment events
        self.communication_bus.subscribe_to_event("DeploymentManagerAgent", "deployment_completed")
        self.communication_bus.subscribe_to_event("QualityAssessorAgent", "deployment_completed")
    
    def _define_workflows(self):
        """Define standard workflows."""
        # Main Power BI Development Workflow
        main_workflow = {
            "start_step": "initialize",
            "steps": {
                "initialize": {
                    "type": "agent_task",
                    "agent": "OrchestratorAgent",
                    "task": {
                        "type": "initialize_workflow",
                        "description": "Initialize the workflow and validate inputs"
                    },
                    "next_step": "analyze_data"
                },
                "analyze_data": {
                    "type": "agent_task",
                    "agent": "DataAnalystAgent",
                    "task": {
                        "type": "comprehensive_analysis",
                        "description": "Analyze data sources and create data profile"
                    },
                    "next_step": "parallel_design"
                },
                "parallel_design": {
                    "type": "parallel",
                    "tasks": [
                        {
                            "type": "agent_task",
                            "agent": "SchemaArchitectAgent",
                            "task": {
                                "type": "design_schema",
                                "description": "Design optimal data model schema"
                            }
                        },
                        {
                            "type": "agent_task",
                            "agent": "ReportDesignerAgent",
                            "task": {
                                "type": "design_report",
                                "description": "Design report layout and visualizations"
                            }
                        }
                    ],
                    "next_step": "develop_measures"
                },
                "develop_measures": {
                    "type": "agent_task",
                    "agent": "DAXDeveloperAgent",
                    "task": {
                        "type": "create_measures",
                        "description": "Create DAX measures and calculations"
                    },
                    "next_step": "quality_check"
                },
                "quality_check": {
                    "type": "agent_task",
                    "agent": "QualityAssessorAgent",
                    "task": {
                        "type": "comprehensive_assessment",
                        "description": "Perform comprehensive quality assessment"
                    },
                    "next_step": "quality_gate"
                },
                "quality_gate": {
                    "type": "condition",
                    "condition": {
                        "type": "simple",
                        "field": "quality_score",
                        "operator": "greater_than",
                        "value": 75
                    },
                    "true_step": "generate_project",
                    "false_step": "quality_remediation"
                },
                "quality_remediation": {
                    "type": "agent_task",
                    "agent": "OrchestratorAgent",
                    "task": {
                        "type": "coordinate_remediation",
                        "description": "Coordinate quality issue remediation"
                    },
                    "next_step": "quality_check"
                },
                "generate_project": {
                    "type": "agent_task",
                    "agent": "OrchestratorAgent",
                    "task": {
                        "type": "generate_pbip",
                        "description": "Generate Power BI project files"
                    },
                    "next_step": "prepare_deployment"
                },
                "prepare_deployment": {
                    "type": "agent_task",
                    "agent": "DeploymentManagerAgent",
                    "task": {
                        "type": "create_deployment_plan",
                        "description": "Create deployment plan and validate prerequisites"
                    },
                    "next_step": "end"
                }
            }
        }
        
        self.workflow_engine.define_workflow("main_powerbi_development", main_workflow)
        
        # Quick Development Workflow (for simpler projects)
        quick_workflow = {
            "start_step": "quick_analysis",
            "steps": {
                "quick_analysis": {
                    "type": "agent_task",
                    "agent": "DataAnalystAgent",
                    "task": {
                        "type": "quick_analysis",
                        "description": "Quick data analysis and profiling"
                    },
                    "next_step": "quick_design"
                },
                "quick_design": {
                    "type": "parallel",
                    "tasks": [
                        {
                            "type": "agent_task",
                            "agent": "SchemaArchitectAgent",
                            "task": {
                                "type": "basic_schema",
                                "description": "Create basic data model"
                            }
                        },
                        {
                            "type": "agent_task",
                            "agent": "ReportDesignerAgent",
                            "task": {
                                "type": "basic_report",
                                "description": "Create basic report layout"
                            }
                        },
                        {
                            "type": "agent_task",
                            "agent": "DAXDeveloperAgent",
                            "task": {
                                "type": "basic_measures",
                                "description": "Create basic DAX measures"
                            }
                        }
                    ],
                    "next_step": "quick_validation"
                },
                "quick_validation": {
                    "type": "agent_task",
                    "agent": "QualityAssessorAgent",
                    "task": {
                        "type": "basic_validation",
                        "description": "Basic quality validation"
                    },
                    "next_step": "generate_project"
                },
                "generate_project": {
                    "type": "agent_task",
                    "agent": "OrchestratorAgent",
                    "task": {
                        "type": "generate_pbip",
                        "description": "Generate Power BI project files"
                    },
                    "next_step": "end"
                }
            }
        }
        
        self.workflow_engine.define_workflow("quick_powerbi_development", quick_workflow)
    
    async def execute_powerbi_development(self, 
                                        requirements: str,
                                        data_source_configs: List[Dict[str, Any]],
                                        workflow_type: str = "main_powerbi_development",
                                        output_path: str = "./output") -> Dict[str, Any]:
        """Execute the complete Power BI development workflow."""
        try:
            logger.info(f"Starting Power BI development workflow: {workflow_type}")
            
            # Prepare initial input
            initial_input = {
                "requirements": requirements,
                "data_source_configs": data_source_configs,
                "output_path": output_path,
                "workflow_type": workflow_type,
                "start_time": datetime.now().isoformat(),
                "quality_threshold": self.config.get('quality_threshold', 75)
            }
            
            # Execute workflow
            result = await self.workflow_engine.execute_workflow(workflow_type, initial_input)
            
            if result["status"] == "completed":
                # Extract final outputs
                final_state = result.get("final_state", {})
                context = final_state.get("context", {})
               
                # Generate PBIP project if not already done
                if "pbip_project" not in context:
                    pbip_result = await self._generate_pbip_project(context, output_path)
                    context["pbip_project"] = pbip_result
                
                # Compile final results
                workflow_result = {
                    "status": "success",
                    "workflow_id": result["execution_id"],
                    "project_details": context.get("pbip_project", {}),
                    "quality_metrics": context.get("quality_report", {}),
                    "deployment_plan": context.get("deployment_plan", {}),
                    "execution_summary": {
                        "workflow_type": workflow_type,
                        "start_time": initial_input["start_time"],
                        "completion_time": datetime.now().isoformat(),
                        "agents_involved": list(self.agents.keys()),
                        "iterations": final_state.get("iteration_count", 0)
                    },
                    "recommendations": await self._compile_recommendations(context),
                    "next_steps": await self._suggest_next_steps(context)
                }
                
                logger.info(f"Power BI development workflow completed successfully")
                return workflow_result
                
            else:
                logger.error(f"Workflow execution failed: {result.get('error', 'Unknown error')}")
                return {
                    "status": "failed",
                    "error": result.get("error", "Unknown error"),
                    "workflow_id": result["execution_id"],
                    "partial_results": result.get("final_state", {})
                }
                
        except Exception as e:
            logger.error(f"Error executing Power BI development workflow: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
async def _execute_fallback_workflow(self, initial_input: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a simplified fallback workflow when main workflow fails."""
    try:
        logger.info("Executing fallback workflow with simplified steps")
        # Implement a simplified workflow that focuses on core functionality
        # This would create a basic report without advanced features
        # ...
        return {"status": "partial_success", "message": "Basic report generated with fallback workflow"}
    except Exception as e:
        logger.error(f"Fallback workflow also failed: {str(e)}")
        return {"status": "critical_failure", "error": str(e)}
    
    async def _generate_pbip_project(self, context: Dict[str, Any], output_path: str) -> Dict[str, Any]:
        """Generate PBIP project from workflow context."""
        try:
            # Extract components from context
            schema = context.get("data_model", {}).get("schema", {})
            measures = context.get("dax_measures", {})
            relationships = context.get("relationships", [])
            report_layout = context.get("report_layout", {})
            data_sources = context.get("data_source_configs", [])
            
            # Generate project name
            project_name = context.get("requirements", "PowerBI_Project")[:50].replace(" ", "_")
            
            # Generate PBIP project
            pbip_result = self.pbip_generator.generate_pbip_project(
                project_name=project_name,
                data_sources=data_sources,
                schema=schema,
                measures=measures,
                relationships=relationships,
                report_layout=report_layout,
                output_path=output_path
            )
            
            return pbip_result
            
        except Exception as e:
            logger.error(f"Error generating PBIP project: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    async def _compile_recommendations(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compile recommendations from all agents."""
        recommendations = []
        
        # Data quality recommendations
        quality_report = context.get("quality_report", {})
        if quality_report.get("recommendations"):
            recommendations.extend([
                {"category": "Data Quality", "recommendation": rec}
                for rec in quality_report["recommendations"]
            ])
        
        # Performance recommendations
        if quality_report.get("performance_recommendations"):
            recommendations.extend([
                {"category": "Performance", "recommendation": rec}
                for rec in quality_report["performance_recommendations"]
            ])
        
        # Design recommendations
        design_assessment = context.get("design_assessment", {})
        if design_assessment.get("recommendations"):
            recommendations.extend([
                {"category": "Design", "recommendation": rec}
                for rec in design_assessment["recommendations"]
            ])
        
        return recommendations
    
    async def _suggest_next_steps(self, context: Dict[str, Any]) -> List[str]:
        """Suggest next steps based on workflow results."""
        next_steps = []
        
        quality_score = context.get("quality_report", {}).get("overall_score", 100)
        
        if quality_score >= 90:
            next_steps.extend([
                "Deploy to development environment for testing",
                "Share with stakeholders for feedback",
                "Plan production deployment"
            ])
        elif quality_score >= 75:
            next_steps.extend([
                "Address quality issues identified in assessment",
                "Perform additional testing in development environment",
                "Consider performance optimizations"
            ])
        else:
            next_steps.extend([
                "Review and address critical quality issues",
                "Redesign components with severe issues",
                "Re-run quality assessment before proceeding"
            ])
        
        # Add deployment-specific next steps
        deployment_plan = context.get("deployment_plan")
        if deployment_plan:
            next_steps.append("Execute deployment plan as outlined")
            if deployment_plan.get("approval_required"):
                next_steps.append("Obtain necessary approvals for deployment")
        
        return next_steps
    
    async def deploy_project(self, project_path: str, target_environment: str) -> Dict[str, Any]:
        """Deploy a generated project to the target environment."""
        try:
            logger.info(f"Deploying project from {project_path} to {target_environment}")
            
            # Send deployment task to deployment manager
            deployment_message = AgentMessage(
                sender="WorkflowOrchestrator",
                receiver="DeploymentManagerAgent",
                message_type="task_request",
                content={
                    "task": {
                        "type": "execute_deployment",
                        "description": f"Deploy project to {target_environment}",
                        "input_data": {
                            "project_path": project_path,
                            "target_environment": target_environment
                        }
                    }
                }
            )
            
            response = await self.communication_bus.send_message(deployment_message)
            
            if response and response.message_type == "task_result":
                return response.content.get("result", {})
            else:
                return {
                    "status": "error",
                    "message": "Failed to get deployment response"
                }
                
        except Exception as e:
            logger.error(f"Error deploying project: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_workflow_status(self, execution_id: str) -> Dict[str, Any]:
        """Get the status of a workflow execution."""
        if execution_id in self.workflow_engine.active_workflows:
            state = self.workflow_engine.active_workflows[execution_id]
            return {
                "status": "running",
                "current_step": state.current_step,
                "iteration_count": state.iteration_count,
                "context_keys": list(state.context.keys())
            }
        else:
            return {
                "status": "not_found",
                "message": f"Workflow {execution_id} not found in active workflows"
            }
    
    def get_agent_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all agents."""
        return {
            name: agent.get_status()
            for name, agent in self.agents.items()
        }
    
    async def shutdown(self):
        """Gracefully shutdown the orchestrator."""
        logger.info("Shutting down Power BI Workflow Orchestrator")
        
        # Stop any running workflows
        for execution_id in list(self.workflow_engine.active_workflows.keys()):
            del self.workflow_engine.active_workflows[execution_id]
        
        # Clear agent queues
        for agent in self.agents.values():
            agent.message_queue.clear()
            agent.task_queue.clear()
            agent.is_running = False
        
        logger.info("Shutdown complete")

# Main execution function
async def main():
    """Main function to demonstrate the workflow orchestrator."""
    import yaml
    
    # Load configuration
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize orchestrator
    orchestrator = PowerBIWorkflowOrchestrator(config)
    
    # Example usage
    requirements = """
    Create a sales performance dashboard that shows:
    1. Total sales and revenue trends over time
    2. Sales performance by product category and region
    3. Top performing sales representatives
    4. Key performance indicators with targets
    5. Interactive filters for date range and region
    """
    
    data_source_configs = [
        {
            "name": "SalesDB",
            "type": "sql_server",
            "server": "localhost",
            "database": "SalesDB",
            "username": "user",
            "password": "password"
        }
    ]
    
    try:
        # Execute workflow
        result = await orchestrator.execute_powerbi_development(
            requirements=requirements,
            data_source_configs=data_source_configs,
            workflow_type="main_powerbi_development",
            output_path="./powerbi_projects"
        )
        
        print("Workflow Result:")
        print(json.dumps(result, indent=2))
        
        # If successful, demonstrate deployment
        if result["status"] == "success":
            project_details = result.get("project_details", {})
            project_path = project_details.get("project_path")
            
            if project_path:
                deployment_result = await orchestrator.deploy_project(
                    project_path=project_path,
                    target_environment="development"
                )
                
                print("\nDeployment Result:")
                print(json.dumps(deployment_result, indent=2))
    
    finally:
        await orchestrator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
