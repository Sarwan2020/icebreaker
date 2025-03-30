import streamlit as st
from langchain.chat_models import ChatOpenAI
from langchain.agents import initialize_agent, AgentType
from langchain.memory import ConversationBufferMemory
from langchain.tools import Tool
from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, MessagesPlaceholder
from typing import Dict, List, Any, Optional

from config import config


class Assistant:

    def __init__(self,metrics_service):
        # Initialize the chat model
        self.chat_model = ChatOpenAI(
            temperature=0.7,
            model_name="gpt-4o",
            api_key=''
        )
        
        # Initialize memory
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        
        # Initialize services
        self.metrics_service = metrics_service
        self.duckdb_repo = metrics_service.repo
        
        # Initialize tools
        self.tools = self._create_tools()
        
        # Initialize agent
        self.agent = self._create_agent()
    
    def _create_tools(self) -> List[Tool]:
        """Create tools from DuckDBRepository and MetricsService methods."""
        
        # DuckDBRepository tools
        duckdb_tools = [
            Tool(
                name="get_all_tables",
                func=self.duckdb_repo.get_all_tables,
                description="Get a list of all tables in the database."
            ),
            Tool(
                name="get_active_files",
                func=self.duckdb_repo.get_active_files,
                description="Get all active files in the database."
            ),
            Tool(
                name="get_all_files",
                func=self.duckdb_repo.get_all_files,
                description="Get all files (active and inactive) in the database."
            ),
            Tool(
                name="get_history",
                func=self.duckdb_repo.get_history,
                description="Get the history of operations on tables."
            ),
            Tool(
                name="get_snapshot_details",
                func=self.duckdb_repo.get_snapshot_details,
                description="Get details of all snapshots for tables."
            ),
            Tool(
                name="get_partition_details",
                func=self.duckdb_repo.get_partition_details,
                description="Get partition details for tables."
            ),
            Tool(
                name="metadata_log_entries",
                func=self.duckdb_repo.metadata_log_entries,
                description="Get metadata log entries for tables."
            )
        ]
        
        # MetricsService tools
        metrics_tools = [
            Tool(
                name="list_tables",
                func=self.metrics_service.list_tables,
                description="List all tables in the database."
            ),
            Tool(
                name="get_active_files_count",
                func=self.metrics_service.get_active_files_count,
                description="Get count of active files from the repository."
            ),
            Tool(
                name="get_all_files_count",
                func=self.metrics_service.get_all_files_count,
                description="Get count of all files from the repository."
            ),
            Tool(
                name="get_namespace_statistics",
                func=self.metrics_service.get_namespace_statistics,
                description="Get namespace statistics in a structured format."
            ),
            Tool(
                name="get_all_table_metrics",
                func=self.metrics_service.get_all_table_metrics,
                description="Get metrics for all tables."
            ),
            Tool(
                name="get_table_snapshot_details",
                func=lambda table_name: self.metrics_service.get_table_snapshot_details(table_name),
                description="Get snapshot details for a specific table. Input should be the table name."
            ),
            Tool(
                name="get_table_growth_analysis",
                func=lambda table_name: self.metrics_service.get_table_growth_analysis(table_name),
                description="Get growth analysis for a specific table. Input should be the table name."
            ),
            Tool(
                name="get_partition_details_for_table",
                func=lambda table_name: self.metrics_service.get_partition_details(table_name),
                description="Get partition details for a specific table. Input should be the table name."
            ),
            Tool(
                name="get_partition_file_size",
                func=lambda table_name: self.metrics_service.get_partition_file_size(table_name),
                description="Get file sizes for partitions in a specific table. Input should be the table name."
            ),
            Tool(
                name="get_column_level_metrics",
                func=lambda table_name: self.metrics_service.get_column_level_metrics(table_name),
                description="Get column-level metrics for a specific table. Input should be the table name."
            ),
            Tool(
                name="run_compaction_job",
                func=lambda table_name: self.metrics_service.run_compaction_job(table_name),
                description="Run a compaction job on a specific table. Input should be the table name."
            ),
            Tool(
                name="time_travel_to_snapshot",
                func=lambda input_str: self._parse_and_time_travel(input_str),
                description="Time travel to a specific snapshot of a table. Input should be in format 'table_name,snapshot_id'."
            ),
            Tool(
                name="run_vacuum",
                func=lambda table_name: self.metrics_service.run_vacuum(table_name),
                description="Run a vacuum operation on a specific table. Input should be the table name."
            ),
            Tool(
                name="copy_metrics_to_db",
                func=self.metrics_service.copy_metrics_to_db,
                description="Copy metrics to the database."
            )
        ]
        
        return duckdb_tools + metrics_tools
    
    def _parse_and_time_travel(self, input_str: str):
        """Parse input string and call time_travel_to_snapshot."""
        try:
            table_name, snapshot_id = input_str.split(',')
            return self.metrics_service.time_travel_to_snapshot(table_name.strip(), snapshot_id.strip())
        except Exception as e:
            return f"Error in time travel: {str(e)}. Input should be in format 'table_name,snapshot_id'"
    
    def _create_agent(self):
        """Create an agent with the tools."""
        return initialize_agent(
            tools=self.tools,
            llm=self.chat_model,
            agent=AgentType.OPENAI_FUNCTIONS,
            verbose=True,
            memory=self.memory,
            handle_parsing_errors=True,
            max_iterations=3,
            early_stopping_method="generate",
            prompt=ChatPromptTemplate(
                messages=[
                    HumanMessagePromptTemplate.from_template(template="{input}"),
                    MessagesPlaceholder(variable_name="agent_scratchpad")
                ]
            ),
            agent_kwargs={
                "system_message": """You are an Apache Iceberg expert chatbot. 
                Your role is to answer all questions related to Apache Iceberg,
                including optimization techniques and best practices.
                
                IMPORTANT INSTRUCTIONS FOR USING TOOLS:
                1. Always review ALL available tools before formulating your response
                2. When answering questions, use MULTIPLE tools if they can provide complementary information
                3. Combine and analyze results from different tools to provide comprehensive answers
                4. For complex questions, break down your analysis step by step using relevant tools
                5. Always explain which tools you're using and why
                
                Do not append namespace to table names in your tool calls.
                For example:
                - When asked about table health, combine GetPartitionFileCount, GetPartitionSize, and GetFileSizeDeviation
                - When analyzing growth patterns, use both GetAddedRecordsOverTime and GetOperationCountsOverTime
                
                Be friendly and helpful while providing accurate technical information based on the tools' results.""",
                "format_instructions": """To use a tool, please use the following format:

Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action

Thought: I know what to do next
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action

Thought: I now know the final answer
Final Answer: the final answer to the original input question"""
            }
        )
    
    def ask(self, query: str) -> str:
        """Ask a question to the assistant."""
        try:
            response = self.agent.run(query)
            return response
        except Exception as e:
            return f"Error: {str(e)}"