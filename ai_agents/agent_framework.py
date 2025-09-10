import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import uuid

from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import BaseTool
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.memory import ConversationBufferMemory
from langgraph import StateGraph, END
from langgraph.graph import Graph
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class AgentType(Enum):
    """Types of agents in the system."""
    DATA_ANALYST = "data_analyst"
    REPORT_DESIGNER = "report_designer"
    DAX_DEVELOPER = "dax_developer"
    SCHEMA_ARCHITECT = "schema_architect"
    QUALITY_ASSESSOR = "quality_assessor"
    DEPLOYMENT_MANAGER = "deployment_manager"
    ORCHESTRATOR = "orchestrator"

class TaskStatus(Enum):
    """Status of agent tasks."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class AgentMessage:
    """Message structure for agent communication."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ""
    receiver: str = ""
    message_type: str = "info"
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    priority: int = 1  # 1-5, where 5 is highest priority

@dataclass
class AgentTask:
    """Task structure for agents."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_type: AgentType = AgentType.DATA_ANALYST
    task_type: str = ""
    description: str = ""
    input_data: Dict[str, Any] = field(default_factory=dict)
    output_data: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    priority: int = 1
    dependencies: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

class AgentState(BaseModel):
    """State management for agents."""
    messages: List[AgentMessage] = Field(default_factory=list)
    tasks: List[AgentTask] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    current_step: str = "initialization"
    iteration_count: int = 0
    max_iterations: int = 10

class BaseAIAgent:
    """Base class for all AI agents in the system."""
    
    def __init__(self, 
                 agent_type: AgentType,
                 name: str,
                 config: Dict[str, Any],
                 llm: Optional[Union[ChatOpenAI, AzureChatOpenAI]] = None):
        self.agent_type = agent_type
        self.name = name
        self.config = config
        self.llm = llm or self._initialize_llm()
        self.tools: List[BaseTool] = []
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.message_queue: List[AgentMessage] = []
        self.task_queue: List[AgentTask] = []
        self.is_running = False
        
        # Initialize agent-specific tools
        self._initialize_tools()
        
        # Create the agent
        self.agent = self._create_agent()
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            memory=self.memory,
            verbose=True,
            max_iterations=10
        )
        
        logger.info(f"Initialized {self.agent_type.value} agent: {self.name}")
    
    def _initialize_llm(self) -> Union[ChatOpenAI, AzureChatOpenAI]:
        """Initialize the language model."""
        openai_config = self.config.get('openai', {})
        
        if openai_config.get('use_azure', False):
            return AzureChatOpenAI(
                azure_deployment=openai_config.get('model', 'gpt-4'),
                azure_endpoint=openai_config.get('azure_endpoint'),
                api_version=openai_config.get('azure_version', '2023-12-01-preview'),
                api_key=openai_config.get('api_key'),
                temperature=0.2
            )
        else:
            return ChatOpenAI(
                api_key=openai_config.get('api_key'),
                model=openai_config.get('model', 'gpt-4'),
                temperature=0.2
            )
    
    def _initialize_tools(self):
        """Initialize agent-specific tools. Override in subclasses."""
        pass
    
    def _create_agent(self):
        """Create the LangChain agent."""
        system_prompt = self._get_system_prompt()
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
        
        return create_openai_tools_agent(self.llm, self.tools, prompt)
    
    def _get_system_prompt(self) -> str:
        """Get the system prompt for this agent. Override in subclasses."""
        return f"You are a {self.agent_type.value} agent specialized in Power BI development."
    
    async def process_message(self, message: AgentMessage) -> Optional[AgentMessage]:
        """Process an incoming message."""
        try:
            self.message_queue.append(message)
            
            # Process the message based on its type
            if message.message_type == "task_request":
                return await self._handle_task_request(message)
            elif message.message_type == "data_update":
                return await self._handle_data_update(message)
            elif message.message_type == "query":
                return await self._handle_query(message)
            else:
                return await self._handle_generic_message(message)
                
        except Exception as e:
            logger.error(f"Error processing message in {self.name}: {str(e)}")
            return AgentMessage(
                sender=self.name,
                receiver=message.sender,
                message_type="error",
                content={"error": str(e), "original_message_id": message.id}
            )
    
    async def _handle_task_request(self, message: AgentMessage) -> AgentMessage:
        """Handle task request messages."""
        task_data = message.content.get('task', {})
        
        task = AgentTask(
            agent_type=self.agent_type,
            task_type=task_data.get('type', 'generic'),
            description=task_data.get('description', ''),
            input_data=task_data.get('input_data', {}),
            priority=task_data.get('priority', 1)
        )
        
        self.task_queue.append(task)
        
        # Execute the task
        result = await self.execute_task(task)
        
        return AgentMessage(
            sender=self.name,
            receiver=message.sender,
            message_type="task_result",
            content={
                "task_id": task.id,
                "result": result,
                "status": task.status.value
            }
        )
    
    async def _handle_data_update(self, message: AgentMessage) -> AgentMessage:
        """Handle data update messages."""
        # Default implementation - override in subclasses
        return AgentMessage(
            sender=self.name,
            receiver=message.sender,
            message_type="ack",
            content={"message": "Data update received"}
        )
    
    async def _handle_query(self, message: AgentMessage) -> AgentMessage:
        """Handle query messages."""
        query = message.content.get('query', '')
        
        try:
            response = await self.agent_executor.ainvoke({
                "input": query
            })
            
            return AgentMessage(
                sender=self.name,
                receiver=message.sender,
                message_type="query_response",
                content={
                    "query": query,
                    "response": response.get('output', ''),
                    "success": True
                }
            )
        except Exception as e:
            return AgentMessage(
                sender=self.name,
                receiver=message.sender,
                message_type="query_response",
                content={
                    "query": query,
                    "error": str(e),
                    "success": False
                }
            )
    
    async def _handle_generic_message(self, message: AgentMessage) -> AgentMessage:
        """Handle generic messages."""
        return AgentMessage(
            sender=self.name,
            receiver=message.sender,
            message_type="ack",
            content={"message": "Message received"}
        )
    
    async def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a task. Override in subclasses."""
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        
        try:
            # Default implementation
            result = {"message": f"Task {task.id} executed by {self.name}"}
            
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.output_data = result
            
            return result
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            task.completed_at = datetime.now()
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the agent."""
        return {
            "name": self.name,
            "type": self.agent_type.value,
            "is_running": self.is_running,
            "message_queue_size": len(self.message_queue),
            "task_queue_size": len(self.task_queue),
            "completed_tasks": len([t for t in self.task_queue if t.status == TaskStatus.COMPLETED]),
            "failed_tasks": len([t for t in self.task_queue if t.status == TaskStatus.FAILED])
        }

class AgentCommunicationBus:
    """Communication bus for agent-to-agent messaging."""
    
    def __init__(self):
        self.agents: Dict[str, BaseAIAgent] = {}
        self.message_history: List[AgentMessage] = []
        self.subscribers: Dict[str, List[str]] = {}  # event_type -> [agent_names]
    
    def register_agent(self, agent: BaseAIAgent):
        """Register an agent with the communication bus."""
        self.agents[agent.name] = agent
        logger.info(f"Registered agent: {agent.name}")
    
    def subscribe_to_event(self, agent_name: str, event_type: str):
        """Subscribe an agent to a specific event type."""
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        
        if agent_name not in self.subscribers[event_type]:
            self.subscribers[event_type].append(agent_name)
    
    async def send_message(self, message: AgentMessage) -> Optional[AgentMessage]:
        """Send a message to a specific agent."""
        if message.receiver not in self.agents:
            logger.error(f"Agent {message.receiver} not found")
            return None
        
        self.message_history.append(message)
        
        try:
            response = await self.agents[message.receiver].process_message(message)
            if response:
                self.message_history.append(response)
            return response
        except Exception as e:
            logger.error(f"Error sending message to {message.receiver}: {str(e)}")
            return None
    
    async def broadcast_event(self, event_type: str, content: Dict[str, Any], sender: str = "system"):
        """Broadcast an event to all subscribed agents."""
        if event_type not in self.subscribers:
            return
        
        for agent_name in self.subscribers[event_type]:
            if agent_name != sender:  # Don't send to sender
                message = AgentMessage(
                    sender=sender,
                    receiver=agent_name,
                    message_type=event_type,
                    content=content
                )
                await self.send_message(message)
    
    def get_agent_status(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """Get the status of a specific agent."""
        if agent_name in self.agents:
            return self.agents[agent_name].get_status()
        return None
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get the status of the entire agent system."""
        return {
            "total_agents": len(self.agents),
            "total_messages": len(self.message_history),
            "agents": {name: agent.get_status() for name, agent in self.agents.items()}
        }

class WorkflowEngine:
    """Engine for managing multi-agent workflows."""
    
    def __init__(self, communication_bus: AgentCommunicationBus):
        self.communication_bus = communication_bus
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.active_workflows: Dict[str, AgentState] = {}
    
    def define_workflow(self, workflow_id: str, workflow_definition: Dict[str, Any]):
        """Define a new workflow."""
        self.workflows[workflow_id] = workflow_definition
        logger.info(f"Defined workflow: {workflow_id}")
    
    async def execute_workflow(self, workflow_id: str, initial_input: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workflow."""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        workflow_def = self.workflows[workflow_id]
        state = AgentState(
            context=initial_input,
            current_step=workflow_def.get('start_step', 'start')
        )
        
        execution_id = str(uuid.uuid4())
        self.active_workflows[execution_id] = state
        
        try:
            result = await self._execute_workflow_steps(workflow_def, state)
            return {
                "execution_id": execution_id,
                "status": "completed",
                "result": result,
                "final_state": state.dict()
            }
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            return {
                "execution_id": execution_id,
                "status": "failed",
                "error": str(e),
                "final_state": state.dict()
            }
        finally:
            if execution_id in self.active_workflows:
                del self.active_workflows[execution_id]
    
    async def _execute_workflow_steps(self, workflow_def: Dict[str, Any], state: AgentState) -> Dict[str, Any]:
        """Execute the steps of a workflow."""
        steps = workflow_def.get('steps', {})
        current_step = state.current_step
        
        while current_step != 'end' and state.iteration_count < state.max_iterations:
            if current_step not in steps:
                raise ValueError(f"Step {current_step} not found in workflow")
            
            step_def = steps[current_step]
            state.iteration_count += 1
            
            # Execute the step
            step_result = await self._execute_step(step_def, state)
            
            # Update state
            state.context.update(step_result.get('context_updates', {}))
            
            # Determine next step
            next_step = step_result.get('next_step')
            if not next_step:
                next_step = step_def.get('next_step', 'end')
            
            state.current_step = next_step
            current_step = next_step
        
        return state.context
    
    async def _execute_step(self, step_def: Dict[str, Any], state: AgentState) -> Dict[str, Any]:
        """Execute a single workflow step."""
        step_type = step_def.get('type', 'agent_task')
        
        if step_type == 'agent_task':
            return await self._execute_agent_task_step(step_def, state)
        elif step_type == 'parallel':
            return await self._execute_parallel_step(step_def, state)
        elif step_type == 'condition':
            return await self._execute_condition_step(step_def, state)
        else:
            raise ValueError(f"Unknown step type: {step_type}")
    
    async def _execute_agent_task_step(self, step_def: Dict[str, Any], state: AgentState) -> Dict[str, Any]:
        """Execute an agent task step."""
        agent_name = step_def.get('agent')
        task_definition = step_def.get('task', {})
        
        # Send task to agent
        message = AgentMessage(
            sender="workflow_engine",
            receiver=agent_name,
            message_type="task_request",
            content={
                "task": {
                    "type": task_definition.get('type', 'generic'),
                    "description": task_definition.get('description', ''),
                    "input_data": {**state.context, **task_definition.get('input_data', {})},
                    "priority": task_definition.get('priority', 1)
                }
            }
        )
        
        response = await self.communication_bus.send_message(message)
        
        if response and response.message_type == "task_result":
            result = response.content.get('result', {})
            return {
                "context_updates": result,
                "next_step": step_def.get('next_step')
            }
        else:
            raise Exception(f"Task execution failed for agent {agent_name}")
    
    async def _execute_parallel_step(self, step_def: Dict[str, Any], state: AgentState) -> Dict[str, Any]:
        """Execute parallel tasks."""
        parallel_tasks = step_def.get('tasks', [])
        results = []
        
        # Execute all tasks in parallel
        tasks = []
        for task_def in parallel_tasks:
            task = self._execute_step(task_def, state)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results
        combined_context = {}
        for result in results:
            if isinstance(result, dict) and 'context_updates' in result:
                combined_context.update(result['context_updates'])
        
        return {
            "context_updates": combined_context,
            "next_step": step_def.get('next_step')
        }
    
    async def _execute_condition_step(self, step_def: Dict[str, Any], state: AgentState) -> Dict[str, Any]:
        """Execute a conditional step."""
        condition = step_def.get('condition', {})
        condition_type = condition.get('type', 'simple')
        
        if condition_type == 'simple':
            field = condition.get('field')
            operator = condition.get('operator', 'equals')
            value = condition.get('value')
            
            field_value = state.context.get(field)
            
            if operator == 'equals':
                condition_result = field_value == value
            elif operator == 'not_equals':
                condition_result = field_value != value
            elif operator == 'greater_than':
                condition_result = field_value > value
            elif operator == 'less_than':
                condition_result = field_value < value
            else:
                condition_result = False
            
            if condition_result:
                next_step = step_def.get('true_step', 'end')
            else:
                next_step = step_def.get('false_step', 'end')
            
            return {
                "context_updates": {},
                "next_step": next_step
            }
        
        return {
            "context_updates": {},
            "next_step": step_def.get('next_step', 'end')
        }
