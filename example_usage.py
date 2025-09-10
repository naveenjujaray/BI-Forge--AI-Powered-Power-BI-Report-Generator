import asyncio
import json
from workflow_orchestrator import PowerBIWorkflowOrchestrator

async def run_sales_dashboard_example():
    """Example: Create a sales dashboard."""
    
    # Configuration (in practice, load from config.yaml)
    config = {
        "openai": {
            "api_key": "your-openai-api-key",
            "model": "gpt-4",
            "temperature": 0.2
        },
        "fabric": {
            "tenant_id": "your-tenant-id",
            "client_id": "your-client-id",
            "client_secret": "your-client-secret",
            "workspace_id": "your-workspace-id"
        },
        "quality_threshold": 75
    }
    
    # Initialize orchestrator
    orchestrator = PowerBIWorkflowOrchestrator(config)
    
    # Define requirements
    requirements = """
    Create a comprehensive sales performance dashboard with the following features:
    
    1. Executive Summary Page:
       - Total Revenue, Units Sold, Average Order Value cards
       - Revenue trend line chart (monthly)
       - Top 5 products by revenue (bar chart)
       - Geographic revenue distribution (map)
    
    2. Product Analysis Page:
       - Product category performance matrix
       - Product lifecycle analysis
       - Inventory turnover metrics
       - Price vs. margin scatter plot
    
    3. Customer Analysis Page:
       - Customer segmentation analysis
       - Customer acquisition vs. retention metrics
       - Customer lifetime value distribution
       - Cohort analysis visualization
    
    Key Requirements:
    - Interactive date range filters on all pages
    - Drill-through capabilities from summary to detail
    - Mobile-responsive design
    - Automated data refresh daily at 6 AM
    - Performance targets and KPI indicators
    """
    
    # Define data sources
    data_source_configs = [
        {
            "name": "SalesDB",
            "type": "sql_server",
            "server": "sql-server.company.com",
            "database": "SalesDatamart",
            "description": "Main sales transaction database"
        },
        {
            "name": "CustomerDB",
            "type": "sql_server", 
            "server": "crm-server.company.com",
            "database": "CustomerCRM",
            "description": "Customer relationship management system"
        },
        {
            "name": "ProductDB",
            "type": "sql_server",
            "server": "erp-server.company.com", 
            "database": "ProductCatalog",
            "description": "Product master data and inventory"
        }
    ]
    
    try:
        print("🚀 Starting Power BI Development Workflow...")
        
        # Execute main workflow
        result = await orchestrator.execute_powerbi_development(
            requirements=requirements,
            data_source_configs=data_source_configs,
            workflow_type="main_powerbi_development",
            output_path="./sales_dashboard_project"
        )
        
        print("\n📋 Workflow Execution Result:")
        print("=" * 50)
        print(f"Status: {result['status']}")
        print(f"Workflow ID: {result.get('workflow_id', 'N/A')}")
        
        if result['status'] == 'success':
            project_details = result.get('project_details', {})
            quality_metrics = result.get('quality_metrics', {})
            
            print(f"\n📁 Project Details:")
            print(f"  Project Path: {project_details.get('project_path', 'N/A')}")
            print(f"  PBIP File: {project_details.get('pbip_file', 'N/A')}")
            print(f"  Files Created: {len(project_details.get('files_created', []))}")
            
            print(f"\n📊 Quality Metrics:")
            print(f"  Overall Score: {quality_metrics.get('overall_score', 'N/A')}")
            print(f"  Issues Found: {len(quality_metrics.get('issues', []))}")
            
            # Display recommendations
            recommendations = result.get('recommendations', [])
            if recommendations:
                print(f"\n💡 Recommendations:")
                for i, rec in enumerate(recommendations[:5], 1):
                    print(f"  {i}. [{rec.get('category', 'General')}] {rec.get('recommendation', '')}")
            
            # Display next steps
            next_steps = result.get('next_steps', [])
            if next_steps:
                print(f"\n➡️ Next Steps:")
                for i, step in enumerate(next_steps, 1):
                    print(f"  {i}. {step}")
            
            # Deploy to development environment
            print(f"\n🚀 Deploying to development environment...")
            deployment_result = await orchestrator.deploy_project(
                project_path=project_details.get('project_path', ''),
                target_environment="development"
            )
            
            print(f"\n📦 Deployment Result:")
            print(f"  Status: {deployment_result.get('status', 'N/A')}")
            if deployment_result.get('status') == 'success':
                print(f"  Message: {deployment_result.get('message', 'Deployed successfully')}")
        
        else:
            print(f"❌ Workflow failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        
    finally:
        await orchestrator.shutdown()
        print("\n✅ Orchestrator shutdown complete")

async def run_quick_dashboard_example():
    """Example: Create a quick dashboard for rapid prototyping."""
    
    config = {
        "openai": {
            "api_key": "your-openai-api-key",
            "model": "gpt-4",
            "temperature": 0.2
        },
        "quality_threshold": 60  # Lower threshold for quick development
    }
    
    orchestrator = PowerBIWorkflowOrchestrator(config)
    
    requirements = """
    Quick sales overview dashboard:
    - Revenue by month (line chart)
    - Sales by region (bar chart)  
    - Top 10 customers (table)
    - Basic KPI cards (total sales, orders, customers)
    """
    
    data_source_configs = [
        {
            "name": "SalesCSV",
            "type": "csv",
            "file_path": "./data/sales_data.csv",
            "description": "Sales transaction data"
        }
    ]
    
    try:
        print("⚡ Starting Quick Power BI Development...")
        
        result = await orchestrator.execute_powerbi_development(
            requirements=requirements,
            data_source_configs=data_source_configs,
            workflow_type="quick_powerbi_development", 
            output_path="./quick_dashboard"
        )
        
        print(f"\n✅ Quick Development Result: {result['status']}")
        if result['status'] == 'success':
            print(f"📁 Project created at: {result['project_details'].get('project_path')}")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        
    finally:
        await orchestrator.shutdown()

if __name__ == "__main__":
    print("Power BI Agentic Workflow Examples")
    print("=" * 40)
    
    choice = input("\nChoose example:\n1. Full Sales Dashboard\n2. Quick Dashboard\nEnter choice (1-2): ")
    
    if choice == "1":
        asyncio.run(run_sales_dashboard_example())
    elif choice == "2": 
        asyncio.run(run_quick_dashboard_example())
    else:
        print("Invalid choice. Please run again and select 1 or 2.")
