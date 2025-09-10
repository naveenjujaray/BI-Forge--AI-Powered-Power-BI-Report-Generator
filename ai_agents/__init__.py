"""
AI Agents Package for Power BI Development

This package contains specialized AI agents that work together to automate
Power BI report and data model development using agentic workflows.
"""

__version__ = "1.0.0"
__author__ = "AI Development Team"
__description__ = "Agentic AI system for automated Power BI development"

# Core framework imports
from .agent_framework import (
    AgentType,
    TaskStatus,
    AgentMessage,
    AgentTask,
    AgentState,
    BaseAIAgent,
    AgentCommunicationBus,
    WorkflowEngine
)

# Specialized agent imports
from .specialized_agents import (
    DataAnalystAgent,
    ReportDesignerAgent,
    DAXDeveloperAgent,
    SchemaArchitectAgent
)

from .specialized_agents_continued import (
    QualityAssessorAgent,
    DeploymentManagerAgent,
    OrchestratorAgent
)

# Agent registry for easy instantiation
AGENT_REGISTRY = {
    AgentType.DATA_ANALYST: DataAnalystAgent,
    AgentType.REPORT_DESIGNER: ReportDesignerAgent,
    AgentType.DAX_DEVELOPER: DAXDeveloperAgent,
    AgentType.SCHEMA_ARCHITECT: SchemaArchitectAgent,
    AgentType.QUALITY_ASSESSOR: QualityAssessorAgent,
    AgentType.DEPLOYMENT_MANAGER: DeploymentManagerAgent,
    AgentType.ORCHESTRATOR: OrchestratorAgent
}

# Convenience functions
def create_agent(agent_type: AgentType, name: str, config: dict, **kwargs) -> BaseAIAgent:
    """
    Factory function to create agents by type.
    
    Args:
        agent_type: The type of agent to create
        name: Name for the agent instance
        config: Configuration dictionary
        **kwargs: Additional arguments passed to agent constructor
    
    Returns:
        Instantiated agent of the specified type
    
    Example:
        >>> config = {"openai": {"api_key": "sk-..."}}
        >>> agent = create_agent(AgentType.DATA_ANALYST, "DataAnalyst1", config)
    """
    if agent_type not in AGENT_REGISTRY:
        raise ValueError(f"Unknown agent type: {agent_type}")
    
    agent_class = AGENT_REGISTRY[agent_type]
    return agent_class(name=name, config=config, **kwargs)

def create_agent_team(config: dict, team_prefix: str = "Agent") -> dict:
    """
    Create a complete team of all agent types.
    
    Args:
        config: Configuration dictionary for all agents
        team_prefix: Prefix for agent names
    
    Returns:
        Dictionary mapping agent type to agent instance
    
    Example:
        >>> config = load_config("config.yaml")
        >>> team = create_agent_team(config, "PowerBI")
        >>> data_analyst = team[AgentType.DATA_ANALYST]
    """
    team = {}
    
    for agent_type in AgentType:
        agent_name = f"{team_prefix}{agent_type.value.replace('_', '').title()}"
        team[agent_type] = create_agent(agent_type, agent_name, config)
    
    return team

def setup_communication_bus(agents: dict = None) -> AgentCommunicationBus:
    """
    Set up communication bus and register agents.
    
    Args:
        agents: Dictionary of agents to register (optional)
    
    Returns:
        Configured communication bus
    
    Example:
        >>> team = create_agent_team(config)
        >>> bus = setup_communication_bus(team)
    """
    bus = AgentCommunicationBus()
    
    if agents:
        for agent in agents.values():
            bus.register_agent(agent)
            
        # Set up standard event subscriptions
        _setup_standard_subscriptions(bus, agents)
    
    return bus

def _setup_standard_subscriptions(bus: AgentCommunicationBus, agents: dict):
    """Set up standard event subscriptions between agents."""
    # Data quality events
    bus.subscribe_to_event("data_quality_issue", "DataAnalyst")
    bus.subscribe_to_event("data_quality_issue", "QualityAssessor")
    
    # Schema change events
    bus.subscribe_to_event("schema_updated", "SchemaArchitect")
    bus.subscribe_to_event("schema_updated", "DataAnalyst")
    bus.subscribe_to_event("schema_updated", "DAXDeveloper")
    
    # Deployment events
    bus.subscribe_to_event("deployment_completed", "DeploymentManager")
    bus.subscribe_to_event("deployment_completed", "QualityAssessor")
    
    # Report design events
    bus.subscribe_to_event("design_updated", "ReportDesigner")
    bus.subscribe_to_event("design_updated", "QualityAssessor")

# Package metadata
__all__ = [
    # Enums
    'AgentType',
    'TaskStatus',
    
    # Data classes
    'AgentMessage',
    'AgentTask', 
    'AgentState',
    
    # Core classes
    'BaseAIAgent',
    'AgentCommunicationBus',
    'WorkflowEngine',
    
    # Specialized agents
    'DataAnalystAgent',
    'ReportDesignerAgent', 
    'DAXDeveloperAgent',
    'SchemaArchitectAgent',
    'QualityAssessorAgent',
    'DeploymentManagerAgent',
    'OrchestratorAgent',
    
    # Utility functions
    'create_agent',
    'create_agent_team',
    'setup_communication_bus',
    
    # Registry
    'AGENT_REGISTRY'
]

# Package-level constants
DEFAULT_AGENT_CONFIG = {
    "timeout_seconds": 300,
    "max_retries": 3,
    "enable_logging": True,
    "log_level": "INFO"
}

SUPPORTED_WORKFLOWS = [
    "main_powerbi_development",
    "quick_powerbi_development", 
    "data_analysis_only",
    "quality_assessment_only",
    "deployment_only"
]

# Validation function
def validate_config(config: dict) -> bool:
    """
    Validate configuration dictionary for agent creation.
    
    Args:
        config: Configuration to validate
        
    Returns:
        True if valid, raises ValueError if invalid
    """
    required_keys = ["openai"]
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
    
    # Validate OpenAI config
    openai_config = config["openai"]
    if "api_key" not in openai_config:
        raise ValueError("Missing OpenAI API key in configuration")
    
    return True

# Import error handling
try:
    from langchain_openai import ChatOpenAI, AzureChatOpenAI
    from langchain_core.agents import AgentExecutor
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    import warnings
    warnings.warn(
        "LangChain not available. Some agent functionality may be limited.",
        ImportWarning
    )

try:
    from microsoft.fabric.core import FabricClient
    FABRIC_AVAILABLE = True
except ImportError:
    FABRIC_AVAILABLE = False
    import warnings
    warnings.warn(
        "Microsoft Fabric SDK not available. Fabric-specific features will be disabled.",
        ImportWarning
    )

# Package information
PACKAGE_INFO = {
    "name": "ai_agents",
    "version": __version__,
    "description": __description__,
    "author": __author__,
    "langchain_available": LANGCHAIN_AVAILABLE,
    "fabric_available": FABRIC_AVAILABLE,
    "supported_workflows": SUPPORTED_WORKFLOWS
}

def get_package_info() -> dict:
    """Get package information and capabilities."""
    return PACKAGE_INFO.copy()
