"""
Utility functions for SQL Migration Agents.
"""

from typing import Dict, List, Any, Optional
import re
from loguru import logger

def extract_response(messages: List[Dict[str, Any]], agent_name: str) -> str:
    """
    Extract the most recent response from a specific agent.
    
    Args:
        messages: List of messages from the group chat
        agent_name: Name of the agent to extract response from
        
    Returns:
        The most recent message from the agent as a string
    """
    if not messages:
        logger.warning("No messages provided to extract_response")
        return ""
    
    # Log debug info about message structure
    if messages and len(messages) > 0:
        sample_msg = messages[-1]
        logger.debug(f"Sample message structure: {list(sample_msg.keys())}")
    
    for message in reversed(messages):
        if not isinstance(message, dict):
            logger.warning(f"Skipping non-dict message: {type(message)}")
            continue
            
        # Check multiple possible keys for agent identification
        # Priority: name > sender > role
        current_agent_name = None
        
        # Try 'name' field first
        if "name" in message and message["name"]:
            current_agent_name = message["name"]
        # Try 'sender' field
        elif "sender" in message and message["sender"]:
            current_agent_name = message["sender"]
        # Try 'role' field (some autogen versions use this)
        elif "role" in message and message["role"]:
            current_agent_name = message["role"]
        
        # Match agent name (case-insensitive)
        if current_agent_name and current_agent_name.lower() == agent_name.lower():
            # Extract content
            content = message.get("content", "")
            
            # Handle different content types
            if isinstance(content, str):
                return content
            elif isinstance(content, dict):
                # Some autogen versions wrap content in a dict
                return str(content.get("text", content.get("message", str(content))))
            elif isinstance(content, list):
                # Handle list of content items
                return "\n".join([str(item) for item in content])
            else:
                logger.warning(f"Unexpected content type: {type(content)}")
                return str(content)
    
    logger.debug(f"No message found from agent: {agent_name}")
    return ""

def extract_code_blocks(text: str, language: Optional[str] = None) -> List[str]:
    """
    Extract code blocks from markdown text.
    
    Args:
        text: Markdown text containing code blocks
        language: Optional language filter (e.g., 'python')
        
    Returns:
        List of extracted code block contents
    """
    if not text:
        return []
    
    code_blocks = []
    
    # Method 1: Extract using regex for markdown code blocks
    # Pattern matches ```language\n...\n```
    pattern = r'```(?:(\w+)\n)?(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    
    for lang, code in matches:
        # If language filter is specified, only include matching blocks
        if language and lang.lower() != language.lower():
            continue
        
        # Clean up the code
        code = code.strip()
        if code:
            code_blocks.append(code)
    
    # Method 2: Fallback line-by-line parsing (if regex fails)
    if not code_blocks:
        lines = text.split('\n')
        in_code_block = False
        current_block = []
        current_lang = ""
        
        for line in lines:
            # Check for code block start
            if line.strip().startswith("```"):
                if not in_code_block:
                    in_code_block = True
                    # Extract language specifier if present
                    lang_match = re.match(r"```(\w+)", line.strip())
                    if lang_match:
                        current_lang = lang_match.group(1)
                else:
                    # End of code block
                    in_code_block = False
                    if current_block and (not language or current_lang.lower() == language.lower()):
                        code_blocks.append("\n".join(current_block))
                    current_block = []
                    current_lang = ""
            elif in_code_block:
                current_block.append(line)
        
        # Handle unclosed code block
        if in_code_block and current_block:
            if not language or current_lang.lower() == language.lower():
                code_blocks.append("\n".join(current_block))
    
    logger.debug(f"Extracted {len(code_blocks)} code blocks")
    return code_blocks

def extract_structured_code(text: str) -> Dict[str, str]:
    """
    Extract code blocks with specific structure markers for migration layers.
    
    Args:
        text: Text containing structured code blocks
        
    Returns:
        Dictionary mapping layer names to code content
    """
    structured_code = {
        "bronze": "",
        "stage1": "",
        "stage2": "",
        "silver": "",
        "gold": "",
        "tests": ""
    }
    
    if not text:
        return structured_code
    
    # Define markers for different sections
    markers = {
        "bronze": [r"# BRONZE LAYER START", r"# Bronze Layer", r"# BRONZE"],
        "stage1": [r"# STAGE 1: BASE DATA START", r"# Stage 1", r"# BASE DATA"],
        "stage2": [r"# STAGE 2: ADVANCED ANALYTICS START", r"# Stage 2", r"# ADVANCED ANALYTICS"],
        "silver": [r"# SILVER LAYER START", r"# Silver Layer", r"# SILVER"],
        "gold": [r"# GOLD LAYER START", r"# Gold Layer", r"# GOLD"],
        "tests": [r"# TESTS START", r"# Test Cases", r"# TESTING"]
    }
    
    # Extract all code blocks first
    all_blocks = extract_code_blocks(text, language="python")
    
    # Categorize blocks based on markers
    for block in all_blocks:
        block_upper = block.upper()
        categorized = False
        
        for layer, marker_list in markers.items():
            for marker in marker_list:
                if marker.upper() in block_upper:
                    if structured_code[layer]:
                        # Append if already has content
                        structured_code[layer] += "\n\n" + block
                    else:
                        structured_code[layer] = block
                    categorized = True
                    break
            
            if categorized:
                break
        
        # If not categorized, try to infer from content
        if not categorized:
            if "bronze_df" in block or "raw_data" in block.lower():
                structured_code["bronze"] += "\n\n" + block if structured_code["bronze"] else block
            elif "customer_base" in block.lower() or "transaction_summary" in block.lower():
                structured_code["stage1"] += "\n\n" + block if structured_code["stage1"] else block
            elif "rfm" in block.lower() or "customer_metrics" in block.lower():
                structured_code["stage2"] += "\n\n" + block if structured_code["stage2"] else block
            elif "gold_df" in block or "final_df" in block.lower():
                structured_code["gold"] += "\n\n" + block if structured_code["gold"] else block
            elif "test_" in block or "assert" in block:
                structured_code["tests"] += "\n\n" + block if structured_code["tests"] else block
    
    return structured_code

def validate_message_format(message: Dict[str, Any]) -> bool:
    """
    Validate that a message has the expected format.
    
    Args:
        message: Message dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(message, dict):
        return False
    
    # Check for required fields (at least one identifier and content)
    has_identifier = any(key in message for key in ["name", "sender", "role"])
    has_content = "content" in message
    
    return has_identifier and has_content