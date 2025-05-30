"""
Utility functions for SQL Migration Agents.
"""

from typing import Dict, List, Any

def extract_response(messages: List[Dict[str, Any]], agent_name: str) -> str:
    """
    Extract the most recent response from a specific agent.
    
    Args:
        messages: List of messages from the group chat
        agent_name: Name of the agent to extract response from
        
    Returns:
        The most recent message from the agent as a string
    """
    for message in reversed(messages):
        # Check both "name" and "sender" keys for the agent identifier
        # Prioritize "name" if it exists, otherwise use "sender"
        current_agent_name = message.get("name") or message.get("sender")
        if current_agent_name == agent_name:
            # Ensure content exists and is a string
            content = message.get("content", "")
            return content if isinstance(content, str) else ""
    return "" # Return empty string if no message from the agent is found

def extract_code_blocks(text: str) -> List[str]:
    """
    Extract code blocks from markdown text.
    
    Args:
        text: Markdown text containing code blocks
        
    Returns:
        List of extracted code block contents
    """
    code_blocks = []
    lines = text.split('\n')
    in_code_block = False
    current_block = []
    
    for line in lines:
        if line.strip().startswith("```") and not in_code_block:
            in_code_block = True
            # Remove the language specifier if present
            if len(line.strip()) > 3:
                continue
        elif line.strip() == "```" and in_code_block:
            in_code_block = False
            code_blocks.append("\n".join(current_block))
            current_block = []
        elif in_code_block:
            current_block.append(line)
            
    return code_blocks 