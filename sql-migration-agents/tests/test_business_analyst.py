import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import os

from src.agents.business_analyst import BusinessAnalystAgent
from src.core.base_agent import AgentResponse


@pytest.fixture
def mock_openai_client():
    """Mock the OpenAI client"""
    mock_client = MagicMock()
    
    # Mock completion response
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Mocked OpenAI response"
    
    # Configure the mock client to return the mock response
    mock_client.chat.completions.create.return_value = mock_response
    
    return mock_client


@pytest.fixture
def business_analyst_agent(mock_openai_client):
    """Create a BusinessAnalystAgent with a mocked OpenAI client"""
    agent = BusinessAnalystAgent()
    agent.client = mock_openai_client
    return agent


@pytest.mark.asyncio
async def test_analyze_sql_procedure(business_analyst_agent):
    """Test the analyze_sql_procedure method"""
    # Define test inputs
    sql_code = """
    CREATE PROCEDURE test_procedure AS
    BEGIN
        SELECT * FROM test_table;
    END
    """
    business_context = "This is a test procedure"
    
    # Call the method under test
    response = await business_analyst_agent.analyze_sql_procedure(
        sql_code=sql_code,
        business_context=business_context
    )
    
    # Assert response
    assert isinstance(response, AgentResponse)
    assert response.response == "Mocked OpenAI response"
    assert response.success == True
    
    # Assert the mocked client was called with the expected arguments
    business_analyst_agent.client.chat.completions.create.assert_called_once()
    args, kwargs = business_analyst_agent.client.chat.completions.create.call_args
    
    # Check that the model name was set
    assert kwargs["model"] == business_analyst_agent.openai_model
    
    # Check that the message includes the SQL code
    messages = kwargs["messages"]
    user_message = [m for m in messages if m["role"] == "user"][0]
    assert sql_code in user_message["content"]
    assert business_context in user_message["content"]


@pytest.mark.asyncio
async def test_create_business_requirements(business_analyst_agent):
    """Test the create_business_requirements method"""
    # Define test inputs
    analysis_results = {"analysis": "This is a test analysis"}
    
    # Call the method under test
    response = await business_analyst_agent.create_business_requirements(analysis_results)
    
    # Assert response
    assert isinstance(response, AgentResponse)
    assert response.response == "Mocked OpenAI response"
    assert response.success == True
    
    # Assert the mocked client was called with the expected arguments
    business_analyst_agent.client.chat.completions.create.assert_called_once()
    args, kwargs = business_analyst_agent.client.chat.completions.create.call_args
    
    # Check that the message includes the analysis results
    messages = kwargs["messages"]
    user_message = [m for m in messages if m["role"] == "user"][0]
    assert str(analysis_results) in user_message["content"] 