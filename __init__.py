"""
AI-Powered Power BI Development System

An intelligent, agentic workflow system for automated Power BI report
and data model development using specialized AI agents.
"""

__version__ = "1.0.0"
__author__ = "AI Development Team"
__description__ = "AI-Powered Power BI Development with Agentic Workflows"

# Quick imports for common use cases
from ai_agents import (
    AgentType, 
    create_agent_team, 
    setup_communication_bus,
    AGENT_REGISTRY
)

from powerbi_generator import (
    PBIPGenerator,
    create_pbip_generator,
    SUPPORTED_DATA_SOURCES
)

from workflow_orchestrator import PowerBIWorkflowOrchestrator

# Convenience function for quick setup
def create_powerbi_development_system(config: dict) -> PowerBIWorkflowOrchestrator:
    """
    Quick setup function for the complete Power BI development system.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Fully configured PowerBIWorkflowOrchestrator
        
    Example:
        >>> import yaml
        >>> with open("config.yaml") as f:
        ...     config = yaml.safe_load(f)
        >>> system = create_powerbi_development_system(config)
        >>> result = await system.execute_powerbi_development(
        ...     requirements="Create sales dashboard...",
        ...     data_source_configs=[...])
    """
    return PowerBIWorkflowOrchestrator(config)

__all__ = [
    'PowerBIWorkflowOrchestrator',
    'AgentType',
    'PBIPGenerator',
    'create_powerbi_development_system',
    'create_agent_team',
    'create_pbip_generator',
    'AGENT_REGISTRY',
    'SUPPORTED_DATA_SOURCES'
]
