#!/usr/bin/env python
"""
Debug script for SQL Migration Agents.
This script provides detailed output of agent execution to debug issues.
"""

import os
import sys
import json
import sqlparse
from dotenv import load_dotenv
from loguru import logger

# Set up enhanced logging
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add("logs/debug.log", level="DEBUG", rotation="10 MB")

# Load environment variables
load_dotenv()

# Import after environment is loaded
from src.agents.agent_manager import AgentManager
from src.agents.tasks import get_analysis_task_template, get_migration_task_template
from src.utils.file_utils import read_sql_file
from src.config.llm import get_config_list

def main():
    """Run a debug session with the agent manager"""
    # Print the configuration being used
    config_list = get_config_list()
    print(f"Using LLM config: {json.dumps(config_list, indent=2, default=str)}")
    
    # Initialize agent manager with verbose logging
    agent_manager = AgentManager()
    
    # Example SQL file
    sql_file = "data/input/CustomerRFM.sql"
    if not os.path.exists(sql_file):
        print(f"File not found: {sql_file}")
        sql_file = input("Enter path to a SQL file: ")
        if not os.path.exists(sql_file):
            print(f"File not found: {sql_file}")
            return
    
    # Read and format SQL
    sql_code = read_sql_file(sql_file)
    formatted_sql = sqlparse.format(sql_code, reindent=True, keyword_case='upper')
    print(f"Loaded SQL file: {sql_file} ({len(sql_code)} chars)")
    
    # Create the task
    task_message = get_analysis_task_template().format(
        sql_code=formatted_sql,
        context="Calculates customer RFM segments based on purchase history"
    )
    print("\n==== Starting agent execution with task ====")
    print(f"Task message snippet: {task_message[:100]}...")
    
    # Execute the task
    print("\n==== Executing task ====")
    # Capture the returned result which should contain the conversation
    result_data = agent_manager.execute_task(task_message)
    
    # Extract the conversation messages from the returned data
    # Use an empty list as a fallback if the key is missing or None
    conversation_messages = result_data.get('full_conversation', []) if result_data else []

    # Print message count based on the captured messages
    message_count = len(conversation_messages)
    print(f"\n==== Execution complete: {message_count} messages generated ====")
    
    if message_count == 0:
        # Check the potentially unreliable groupchat messages as a last resort for debugging info
        backup_messages = agent_manager.groupchat.messages
        if backup_messages:
             print(f"WARNING: Main result capture failed, but found {len(backup_messages)} messages directly in groupchat object (might be unreliable).")
             # Optionally use backup_messages if conversation_messages is empty
             # conversation_messages = backup_messages 
        else:
             print("WARNING: No messages were generated or captured during execution!")
             # Decide if you want to return or continue with empty results
             # return # Example: exit if no messages

    # Ensure conversation_messages is used from here on
    if conversation_messages: # Proceed only if we have messages
        # Analyze the conversation using the captured messages
        print("\n==== Analyzing messages by agent ====")
        agent_message_counts = {}
        for message in conversation_messages: # Use captured messages
            # Ensure message is a dictionary and has expected keys
            if isinstance(message, dict):
                sender = message.get("name") or message.get("sender", "Unknown") # Adapt based on actual message structure
                content = message.get("content", "")
                content_length = len(content) if content else 0
                
                if sender not in agent_message_counts:
                    agent_message_counts[sender] = {"count": 0, "total_chars": 0}
                agent_message_counts[sender]["count"] += 1
                agent_message_counts[sender]["total_chars"] += content_length
            else:
                logger.warning(f"Skipping non-dictionary message in analysis: {message}")

        
        for agent, stats in agent_message_counts.items():
            print(f"{agent}: {stats['count']} messages, {stats['total_chars']} total chars")
        
        # Get and display results (assuming get_results_for_analysis uses the now potentially updated/correct agent_manager.groupchat state or is adapted)
        # NOTE: We might need to adapt get_results_for_analysis if it also incorrectly relies on the groupchat object state after completion.
        # For now, let's assume it might work, or we address it next.
        print("\n==== Extracting analysis results ====")
        # Pass the reliable conversation history to the analysis function
        analysis_results = agent_manager.get_results_for_analysis(conversation_messages) # MODIFIED to accept messages

        # Print summary statistics for each result type
        print(f"Business Analysis: {len(analysis_results.get('business_analysis', ''))} chars")
        print(f"Technical Analysis: {len(analysis_results.get('technical_analysis', ''))} chars")
        print(f"Azure Recommendations: {len(analysis_results.get('azure_recommendations', ''))} chars")

        # Define output directory
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)

        # Save analysis results to files
        output_paths = {
            "business_analysis": os.path.join(output_dir, "business_analysis.txt"),
            "technical_analysis": os.path.join(output_dir, "technical_analysis.txt"),
            "azure_recommendations": os.path.join(output_dir, "azure_recommendations.txt"),
        }

        for key, content in analysis_results.items(): # Use results from the modified call
            if key in output_paths:
                try:
                    # Use 'w' mode to overwrite file if it exists
                    with open(output_paths[key], "w", encoding='utf-8') as f:
                        f.write(content if content else "No content generated for this section.")
                    print(f"Saved {key} to {output_paths[key]}")
                except Exception as e:
                    logger.error(f"Error saving {key} to {output_paths[key]}: {e}")
            # Optional: Log if a result key is found but not configured for saving
            # else:
            #     logger.warning(f"Result key '{key}' not configured for saving to output.")

        # Save debug output using the captured messages
        os.makedirs("logs", exist_ok=True)
        try:
            with open("logs/debug_conversation.json", "w") as f:
                json.dump(conversation_messages, f, indent=2, default=str) # Use captured messages
            print("\nFull conversation saved to logs/debug_conversation.json")
        except Exception as e:
             logger.error(f"Error saving debug conversation: {e}")
        
        # Print first and last message to verify content
        if message_count > 0:
            print("\n==== First message ====")
            first_message = conversation_messages[0]
            if isinstance(first_message, dict):
                 print(f"From: {first_message.get('name') or first_message.get('sender', 'Unknown')}")
                 print(f"Content: {first_message.get('content', '')[:100]}...")
            
            print("\n==== Last message ====")
            last_message = conversation_messages[-1]
            if isinstance(last_message, dict):
                 print(f"From: {last_message.get('name') or last_message.get('sender', 'Unknown')}")
                 print(f"Content: {last_message.get('content', '')[:100]}...")
    else:
         print("\nNo messages captured, skipping analysis and saving.")

    print("\n==== Debug session complete ====")

if __name__ == "__main__":
    main() 