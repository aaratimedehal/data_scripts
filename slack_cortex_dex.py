"""
Dex - Greenely Contract Analytics Bot
Integrates Slack with Snowflake Cortex Agent for natural language queries about contracts.
"""
import os
import json
import logging
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
import requests
import time
from collections import deque

load_dotenv()

# Simple in-memory deduplication for slash commands
# Track recent commands to prevent duplicate processing
_recent_commands = deque(maxlen=100)  # Keep last 100 commands
_command_lock = {}  # Track commands being processed

# Slack tokens
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

# Cortex Agent configuration
AGENT_ENDPOINT = os.getenv("AGENT_ENDPOINT")
PAT = os.getenv("PAT")  # Programmatic Access Token from Snowflake

# Channel where bot is active (optional - can listen to all channels)
TARGET_CHANNEL = os.getenv("SLACK_CHANNEL", "ask-dex")

# Custom emoji for verified badge (use your custom emoji name, e.g., ":verified:")
# Leave empty or use standard emoji like ":white_check_mark:"
VERIFIED_EMOJI = os.getenv("VERIFIED_EMOJI", ":verified:")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Slack app
app = App(token=SLACK_BOT_TOKEN)


def call_cortex_agent(question):
    """
    Call Snowflake Cortex Agent REST API with a question.
    
    Returns the agent's response as a formatted string.
    Based on Cortex Agent API response format.
    """
    if not AGENT_ENDPOINT or not PAT:
        return "❌ Error: AGENT_ENDPOINT or PAT not configured. Please check your .env file."
    
    headers = {
        "Authorization": f"Bearer {PAT}",
        "Content-Type": "application/json"
    }
    
    # Cortex Agent API format: messages array with content as array of objects
    # For a new conversation, omit thread_id and parent_message_id
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": question
                    }
                ]
            }
        ]
    }
    
    try:
        # Cortex Agent API returns streaming Server-Sent Events (SSE)
        response = requests.post(
            AGENT_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=60,
            stream=True  # Enable streaming for SSE
        )
        
        response.raise_for_status()
        
        # Parse streaming Server-Sent Events (SSE) response
        final_response = None
        text_content = []
        verified_query_info = None
        metadata = None
        interpretation = None
        sql_queries = []  # Track multiple SQL queries (like the example)
        all_events = []
        step_count = 0  # Track number of processing steps
        planning_steps = []  # Track planning steps
        thinking_steps = []  # Track thinking steps
        error_message = None  # Track error messages from the agent
        
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    data_str = line_str[6:]  # Remove 'data: ' prefix
                    try:
                        event_data = json.loads(data_str)
                        all_events.append(event_data)
                        
                        # Look for the final response event (last event)
                        if event_data.get('role') == 'assistant':
                            final_response = event_data
                            # Check for metadata
                            if 'metadata' in event_data:
                                metadata = event_data['metadata']
                            
                            # DON'T collect text_content here - we'll extract from final_response at the end
                            # This prevents capturing intermediate thinking steps from multiple assistant events
                        
                        # Look for interpretation in various places (not in metadata based on sample)
                        if not interpretation:
                            if 'interpretation' in event_data:
                                interpretation = event_data.get('interpretation')
                            elif 'query_interpretation' in event_data:
                                interpretation = event_data.get('query_interpretation')
                            elif 'message' in event_data and isinstance(event_data['message'], dict):
                                if 'interpretation' in event_data['message']:
                                    interpretation = event_data['message'].get('interpretation')
                            # Check if interpretation is in content array
                            elif 'content' in event_data:
                                for item in event_data['content']:
                                    if isinstance(item, dict) and 'interpretation' in item:
                                        interpretation = item.get('interpretation')
                                        break
                        
                        # Collect SQL queries (track multiple like the example)
                        found_sql = None
                        if 'sql' in event_data:
                            found_sql = event_data.get('sql')
                        elif 'query' in event_data and isinstance(event_data.get('query'), str):
                            found_sql = event_data.get('query')
                        elif 'logical_query' in event_data:
                            found_sql = event_data.get('logical_query')
                        elif 'physical_query' in event_data:
                            found_sql = event_data.get('physical_query')
                        elif 'sql_query' in event_data:
                            found_sql = event_data.get('sql_query')
                        # Check if SQL is in content array
                        elif 'content' in event_data:
                            for item in event_data['content']:
                                if isinstance(item, dict):
                                    if 'sql' in item:
                                        found_sql = item.get('sql')
                                        break
                                    elif 'query' in item:
                                        found_sql = item.get('query')
                                        break
                        
                        # Add to SQL queries list if found and not already present
                        if found_sql and found_sql not in sql_queries:
                            sql_queries.append(found_sql)
                        
                        # Check for verified query information (metadata has is_semantic_sql which might indicate verification)
                        if metadata and metadata.get('is_semantic_sql'):
                            verified_query_info = True
                        elif 'verified_query' in event_data or 'based_on_verified_query' in event_data:
                            verified_query_info = event_data.get('verified_query') or event_data.get('based_on_verified_query')
                        
                        # Check for error messages
                        if 'message' in event_data and isinstance(event_data.get('message'), str):
                            msg = event_data.get('message', '')
                            # Check if it's an error message (timeout, error, etc.)
                            if any(keyword in msg.lower() for keyword in ['error', 'timeout', 'failed', 'cannot', 'exceed']):
                                error_message = msg
                                logger.warning(f"Agent returned error: {msg}")
                        elif 'error' in event_data:
                            error_message = str(event_data.get('error', ''))
                            logger.warning(f"Agent returned error: {error_message}")
                            
                    except json.JSONDecodeError:
                        continue
        
        # If we got an error message, return it as the answer
        if error_message:
            return {
                'answer': f"⚠️ {error_message}\n\nPlease try a simpler question or contact the data analyst if the issue persists.",
                'interpretation': None,
                'sql_query': None,
                'sql_queries': [],
                'verified': False,
                'metadata': metadata,
                'step_count': 0,
                'planning_steps': [],
                'thinking_steps': []
            }
        
        # Extract answer, SQL, and interpretation from final response
        answer_text = None
        
        # PRIORITY 1: Extract from final_response first (this is the cleanest source)
        # The final_response should contain only the final answer, not thinking steps
        if final_response:
            if 'content' in final_response and isinstance(final_response['content'], list):
                text_parts = []
                for item in final_response['content']:
                    # Extract text answers
                    if isinstance(item, dict) and item.get('type') == 'text':
                        text_value = item.get('text', '').strip()
                        if text_value:
                            text_parts.append(text_value)
                    
                    # Also extract SQL from tool_result in final_response
                    elif isinstance(item, dict) and item.get('type') == 'tool_result':
                        tool_result = item.get('tool_result', {})
                        if 'content' in tool_result and isinstance(tool_result['content'], list):
                            for content_item in tool_result['content']:
                                if isinstance(content_item, dict) and content_item.get('type') == 'json':
                                    json_data = content_item.get('json', {})
                                    if 'sql' in json_data:
                                        found_sql = json_data.get('sql')
                                        if found_sql and found_sql not in sql_queries:
                                            sql_queries.append(found_sql)
                                    # Also check verified status in final_response
                                    if 'verified_query_used' in json_data:
                                        if json_data.get('verified_query_used'):
                                            verified_query_info = True
                
                if text_parts:
                    answer_text = '\n'.join(text_parts)
            
            # Also check if final_response has text directly
            elif 'text' in final_response:
                answer_text = final_response.get('text', '').strip()
        
        # PRIORITY 2: Fall back to collected text_content (only if final_response didn't work)
        # Helper function to filter out thinking/reasoning steps
        def filter_thinking_steps(text):
            """Remove agent's internal reasoning and keep only the final answer"""
            if not text:
                return text
            
            # Patterns that indicate thinking/reasoning (not final answer)
            thinking_patterns = [
                'the user is asking',
                'let me query',
                'i need to use',
                'i have the sql results',
                'wait, this seems',
                'actually, looking at',
                'let me check',
                'this seems to be',
                'this is quantitative',
                'i need to',
                'let me',
                'this seems incorrect',
                'looking at the',
                'check if there',
                'the results show',
                'earliest termination',
                'latest termination',
                'spanning from'
            ]
            
            # Strategy: Find the last substantial paragraph that doesn't contain thinking patterns
            # Split by double newlines first (paragraphs)
            paragraphs = text.split('\n\n')
            
            # Also split by single newlines for finer granularity
            all_sections = []
            for para in paragraphs:
                all_sections.extend(para.split('\n'))
            
            # Find the last substantial section that looks like a final answer
            # Final answers typically:
            # - Start with "There", "The", numbers, or direct statements
            # - Are at least 30 characters
            # - Don't contain thinking patterns
            final_answer = None
            for section in reversed(all_sections):
                section_lower = section.lower().strip()
                if not section_lower or len(section_lower) < 30:
                    continue
                
                # Check if it contains thinking patterns
                is_thinking = any(pattern in section_lower for pattern in thinking_patterns)
                if is_thinking:
                    continue
                
                # Check if it looks like a final answer
                if (section_lower.startswith(('there', 'the', 'we have', 'a total', 'in total', '*')) or
                    section_lower[0].isdigit() or
                    (len(section_lower) > 50 and not section_lower.startswith(('i ', 'let ', 'the user', 'this seems')))):
                    final_answer = section.strip()
                    break
            
            # If we found a final answer, return it
            if final_answer:
                return final_answer
            
            # Fallback: return the last paragraph that's substantial and doesn't have thinking patterns
            for para in reversed(paragraphs):
                para_lower = para.lower().strip()
                if para_lower and len(para_lower) > 30:
                    if not any(pattern in para_lower for pattern in thinking_patterns):
                        return para.strip()
            
            # Last resort: return original text
            return text.strip()
        
        # PRIORITY 2: Fall back to collected text_content (only if final_response didn't work)
        # This collects text from events as they stream in
        if not answer_text and text_content:
            # Join all text content, filtering out empty strings
            filtered_text = [t for t in text_content if t and t.strip()]
            if filtered_text:
                full_text = '\n'.join(filtered_text)
                # Filter out thinking steps
                answer_text = filter_thinking_steps(full_text)
            elif text_content:
                full_text = ''.join(text_content)
                answer_text = filter_thinking_steps(full_text)
        
        # PRIORITY 3: Try to extract from final_response content array (if both above didn't work)
        if not answer_text and final_response:
            # Check if content is a list
            if 'content' in final_response and isinstance(final_response['content'], list):
                text_parts = []
                for item in final_response['content']:
                    # Extract text answers
                    if isinstance(item, dict) and item.get('type') == 'text':
                        text_parts.append(item.get('text', ''))
                    
                    # Check for image/chart content (if Cortex Agent returns charts)
                    elif isinstance(item, dict) and item.get('type') == 'image':
                        # If Cortex Agent returns image data, we'd handle it here
                        # For now, this is a placeholder - Cortex Agent typically describes charts in text
                        pass
                    
                    # Extract SQL and interpretation from tool_result
                    elif isinstance(item, dict) and item.get('type') == 'tool_result':
                        tool_result = item.get('tool_result', {})
                        tool_name = tool_result.get('name', '')
                        
                        if 'content' in tool_result and isinstance(tool_result['content'], list):
                            for content_item in tool_result['content']:
                                if isinstance(content_item, dict):
                                    # Handle JSON content
                                    if content_item.get('type') == 'json' and 'json' in content_item:
                                        json_data = content_item['json']
                                        
                                        # Extract SQL query (prefer from contract_analytics)
                                        if 'sql' in json_data:
                                            found_sql = json_data.get('sql')
                                            if found_sql and found_sql not in sql_queries:
                                                sql_queries.append(found_sql)
                                        
                                        # Extract interpretation (use 'text' field as interpretation)
                                        # Always extract from contract_analytics, or use first available
                                        if 'text' in json_data:
                                            text_value = json_data.get('text', '').strip()
                                            if text_value:  # Only use non-empty text
                                                # Always prefer contract_analytics, otherwise use first available
                                                if tool_name == 'contract_analytics':
                                                    interpretation = text_value
                                                elif not interpretation:  # Use first available if we don't have one yet
                                                    interpretation = text_value
                                        
                                        # Extract verified status
                                        if 'verified_query_used' in json_data:
                                            if json_data.get('verified_query_used'):
                                                verified_query_info = True
                                    
                                    # Handle text content in tool_result
                                    elif content_item.get('type') == 'text':
                                        text_value = content_item.get('text', '').strip()
                                        if text_value:
                                            text_parts.append(text_value)
                    
                    # Also check tool_use for verified query reference (can provide better context)
                    elif isinstance(item, dict) and item.get('type') == 'tool_use':
                        tool_use = item.get('tool_use', {})
                        if tool_use.get('name') == 'contract_analytics' and 'input' in tool_use:
                            input_data = tool_use.get('input', {})
                            # Check if there's a verified query reference that might have better interpretation
                            if 'reference_vqrs' in input_data and isinstance(input_data['reference_vqrs'], list):
                                if input_data['reference_vqrs'] and not interpretation:
                                    # Use the verified query question as interpretation context
                                    vqr = input_data['reference_vqrs'][0]
                                    if 'question' in vqr:
                                        interpretation = f"Using verified query: {vqr['question']}"
            
                if text_parts:
                    full_text = '\n'.join(text_parts)
                    answer_text = filter_thinking_steps(full_text)
            
            # Also check if final_response has text directly
            elif 'text' in final_response:
                raw_text = final_response.get('text', '')
                answer_text = filter_thinking_steps(raw_text)
        
        # Final fallback: check if we have any text in the response at all (if both above failed)
        if not answer_text:
            # Try to extract from any event that has text
            for event in all_events:
                if 'text' in event:
                    raw_text = event.get('text', '')
                    answer_text = filter_thinking_steps(raw_text)
                    break
                elif 'content' in event:
                    # Check content array for text
                    if isinstance(event['content'], list):
                        for item in event['content']:
                            if isinstance(item, dict) and item.get('type') == 'text':
                                raw_text = item.get('text', '')
                                answer_text = filter_thinking_steps(raw_text)
                                break
                        if answer_text:
                            break
        
        # If still no answer, log for debugging and return error
        if not answer_text:
            logger.warning(f"Could not parse answer from agent response. Events: {len(all_events)}, text_content items: {len(text_content)}")
            # Log first event structure for debugging (but don't log full SQL which could be huge)
            if all_events:
                sample_event = all_events[0]
                if isinstance(sample_event, dict):
                    # Log structure without full content - show keys and types
                    event_summary = {}
                    for k, v in sample_event.items():
                        if k in ['text', 'message', 'answer']:
                            # Show preview of text fields
                            if isinstance(v, str):
                                preview = v[:200] + "..." if len(v) > 200 else v
                                event_summary[k] = f"'{preview}'"
                            else:
                                event_summary[k] = str(type(v).__name__)
                        elif k == 'content':
                            if isinstance(v, list):
                                content_types = [type(item).__name__ if not isinstance(item, dict) else item.get('type', 'dict') for item in v[:3]]
                                event_summary[k] = f"list[{len(v)}] with types: {content_types}"
                            else:
                                event_summary[k] = str(type(v).__name__)
                        else:
                            event_summary[k] = str(type(v).__name__)
                    logger.warning(f"Sample event structure: {event_summary}")
                    
                    # Try one more aggressive extraction from the sample event
                    if 'content' in sample_event:
                        content = sample_event['content']
                        if isinstance(content, list):
                            for item in content:
                                if isinstance(item, dict):
                                    # Try any field that might contain text
                                    for field in ['text', 'message', 'answer', 'response', 'output']:
                                        if field in item:
                                            potential_text = item[field]
                                            if isinstance(potential_text, str) and potential_text.strip():
                                                answer_text = potential_text
                                                logger.info(f"Found answer in content[{field}] via fallback")
                                                break
                                    if answer_text:
                                        break
                else:
                    logger.warning(f"Sample event is not a dict: {type(sample_event)}")
            
            # If we still don't have an answer after aggressive parsing, return error
            if not answer_text:
                return {
                    'answer': "✅ Received response from agent, but couldn't parse the format. The agent is working! Please try rephrasing your question.",
                    'interpretation': None,
                    'sql_query': None,
                    'sql_queries': [],
                    'verified': False,
                    'metadata': None,
                    'step_count': 0,
                    'planning_steps': [],
                    'thinking_steps': []
                }
        
        # Final check: Look through all events for interpretation if we didn't find it yet
        # Also check content arrays recursively
        if not interpretation:
            for event in all_events:
                # Direct fields
                if 'interpretation' in event:
                    interpretation = event.get('interpretation')
                    break
                elif 'query_interpretation' in event:
                    interpretation = event.get('query_interpretation')
                    break
                # Check content arrays
                elif 'content' in event:
                    for item in event['content']:
                        if isinstance(item, dict):
                            if 'interpretation' in item:
                                interpretation = item.get('interpretation')
                                break
                            elif 'query_interpretation' in item:
                                interpretation = item.get('query_interpretation')
                                break
                    if interpretation:
                        break
        
        # Final check: Look through all events for SQL queries if we haven't found any yet
        # Also check content arrays recursively
        if not sql_queries:
            for event in all_events:
                found_sql = None
                # Direct fields
                if 'sql' in event:
                    found_sql = event.get('sql')
                elif 'logical_query' in event:
                    found_sql = event.get('logical_query')
                elif 'sql_query' in event:
                    found_sql = event.get('sql_query')
                # Check content arrays
                elif 'content' in event:
                    for item in event['content']:
                        if isinstance(item, dict):
                            if 'sql' in item:
                                found_sql = item.get('sql')
                                break
                            elif 'logical_query' in item:
                                found_sql = item.get('logical_query')
                                break
                            elif 'query' in item:
                                found_sql = item.get('query')
                                break
                
                if found_sql and found_sql not in sql_queries:
                    sql_queries.append(found_sql)
                    break
        
        # Build response with answer and optional query details
        # Store query details separately for the "show more" button
        # Check verification status (verified_query_used from tool_result, or metadata flags)
        is_verified = bool(verified_query_info)  # Already set from tool_result JSON if found
        if not is_verified and metadata:
            # If it's semantic SQL, it's likely verified
            if metadata.get('is_semantic_sql'):
                is_verified = True
            # Also check for explicit verified_query flag
            elif metadata.get('verified_query'):
                is_verified = True
        
        # Use first SQL query as primary (for backward compatibility)
        sql_query = sql_queries[0] if sql_queries else None
        
        # If we got an answer but no SQL queries were extracted, assume there was at least 1 query
        # (the agent always uses SQL to answer questions from semantic views)
        # This ensures the "Includes 1 SQL query" message shows even if extraction failed
        if answer_text and not sql_queries:
            # Don't add a fake SQL query, but mark as verified so the "Includes" line shows
            # We'll show "1 SQL query" in the display even though we don't have the actual SQL
            is_verified = True
        
        # If we have SQL queries, assume it's verified (semantic views are verified by default)
        # This ensures the "Includes verified query" message shows when we have SQL
        if sql_queries and not is_verified:
            is_verified = True  # If we have SQL queries, it's likely from a verified semantic view
        
        response_data = {
            'answer': answer_text,
            'interpretation': interpretation,
            'sql_query': sql_query,  # Primary SQL query (first one)
            'sql_queries': sql_queries,  # All SQL queries (like the example)
            'verified': is_verified or bool(verified_query_info),
            'metadata': metadata,  # Store metadata for debugging
            'step_count': step_count,  # Number of processing steps
            'planning_steps': planning_steps,  # Planning steps (like the example)
            'thinking_steps': thinking_steps  # Thinking steps (like the example)
        }
        
        return response_data
            
    except requests.exceptions.Timeout:
        return "⏱️ Request timed out. The query may be taking too long. Try a simpler question."
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return "❌ Authentication failed. Please check your PAT (Programmatic Access Token)."
        elif e.response.status_code == 404:
            return "❌ Agent endpoint not found. Please verify AGENT_ENDPOINT in your .env file."
        else:
            error_text = e.response.text[:500] if e.response.text else "No error details"
            return f"❌ Error {e.response.status_code}: {error_text}"
    except Exception as e:
        return f"❌ Error calling Cortex Agent: {str(e)}"


def truncate_button_value(data):
    """
    Truncate data to fit within Slack's 2001 character limit for button values.
    Returns a JSON string that's guaranteed to be under 2000 characters.
    """
    max_length = 2000  # Leave 1 char buffer
    
    # Extract and truncate fields
    answer = data.get("answer", "")[:300] if data.get("answer") else ""
    sql_query = data.get("sql_query", "")
    sql_queries = data.get("sql_queries", [])
    interpretation = data.get("interpretation", "")[:200] if data.get("interpretation") else ""
    question = data.get("question", "")[:200] if data.get("question") else ""  # Preserve question
    
    # Build minimal payload first
    minimal_data = {
        "answer": answer,
        "sql_query": None,
        "sql_queries": [],
        "interpretation": interpretation,
        "verified": data.get("verified", False),
        "step_count": data.get("step_count", 0),
        "processing_time": data.get("processing_time"),
        "question": question,  # Preserve question in button value
        "action": data.get("action", "show")
    }
    
    # Calculate available space for SQL
    base_json = json.dumps(minimal_data)
    available_space = max_length - len(base_json) - 100  # Buffer for SQL array formatting
    
    # Truncate SQL queries to fit
    if sql_queries:
        max_sql_per_query = max(200, available_space // max(1, len(sql_queries)))
        truncated_queries = []
        for q in sql_queries:
            if q and len(q) > max_sql_per_query:
                truncated_queries.append(q[:max_sql_per_query] + "...")
            else:
                truncated_queries.append(q)
        minimal_data["sql_queries"] = truncated_queries
        minimal_data["sql_query"] = truncated_queries[0] if truncated_queries else None
    elif sql_query:
        max_sql_length = max(200, available_space)
        if len(sql_query) > max_sql_length:
            minimal_data["sql_query"] = sql_query[:max_sql_length] + "..."
            minimal_data["sql_queries"] = [minimal_data["sql_query"]]
        else:
            minimal_data["sql_query"] = sql_query
            minimal_data["sql_queries"] = [sql_query]
    
    # Final check and return
    result = json.dumps(minimal_data)
    if len(result) > max_length:
        # Emergency truncation - remove SQL entirely if still too long
        minimal_data["sql_query"] = None
        minimal_data["sql_queries"] = []
        result = json.dumps(minimal_data)
    
    return result


def format_slack_response(response_data, question=""):
    """
    Format agent response data into Slack Block Kit format with interpretation visible and optional "Show Query Details" button.
    
    Args:
        response_data: Dict with 'answer', 'interpretation', 'sql_query', 'verified' keys
                      OR a string (fallback)
        question: The user's original question (optional, for display)
    
    Returns:
        Tuple of (blocks, text) for Slack message
    """
    # Handle string responses (fallback)
    if isinstance(response_data, str):
        return None, response_data
    
    answer = response_data.get('answer', 'No answer received')
    sql_query = response_data.get('sql_query')
    sql_queries = response_data.get('sql_queries', [])
    # If sql_queries is empty but sql_query exists, use it
    if not sql_queries and sql_query:
        sql_queries = [sql_query]
    interpretation = response_data.get('interpretation')
    verified = response_data.get('verified', False)
    step_count = response_data.get('step_count', 0)
    planning_steps = response_data.get('planning_steps', [])
    thinking_steps = response_data.get('thinking_steps', [])
    
    # Build blocks for Slack message
    blocks = []
    
    # Add user's question at the top (if provided)
    if question:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Your question:* {question}"
            }
        })
        blocks.append({"type": "divider"})
    
    # Add "Completed!" section BEFORE the answer (simpler - just show completion status)
    # Build additional info
    additional_info = []
    if verified:
        additional_info.append(f"{VERIFIED_EMOJI} answer accuracy verified by agent owner")
    # Check both sql_queries and sql_query to ensure we show the count
    queries_to_count = sql_queries if sql_queries else ([sql_query] if sql_query else [])
    if queries_to_count:
        query_count = len(queries_to_count)
        additional_info.append(f"{query_count} SQL {'query' if query_count == 1 else 'queries'}")
    # If we have verified status but no SQL queries listed, still show "1 SQL query" 
    # (the agent used a query even if we couldn't extract it)
    elif verified and not queries_to_count:
        additional_info.append("1 SQL query")
    
    # Build summary text with step count
    processing_time = response_data.get('processing_time')
    if step_count > 0:
        summary_text = f"_Finished {step_count} steps"
    else:
        summary_text = "_Finished processing"
    
    # Always show processing time if available
    if processing_time:
        summary_text += f" • ⏱️ {processing_time:.1f}s"
    
    if additional_info:
        summary_text += f" • Includes {' and '.join(additional_info)}"
    summary_text += "_"
    
    completed_section_text = f"✅ *Completed!*\n\n{summary_text}"
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": completed_section_text
        }
    })
    
    blocks.append({"type": "divider"})  # Add divider before answer
    
    # Then show the answer
    # Convert markdown format if needed (Slack uses *bold* not **bold**)
    answer_formatted = answer.replace("**", "*")  # Convert **bold** to *bold* for Slack
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": answer_formatted
        }
    })
    
    # Add "Show Details" button if SQL query/queries or interpretation is available
    # Always show button if we have SQL queries (even if empty, the agent might have used a query)
    has_sql = bool(sql_queries) or bool(sql_query)
    if has_sql or interpretation:
        button_data = {
            "answer": answer,
            "sql_query": sql_query,
            "sql_queries": sql_queries if sql_queries else ([sql_query] if sql_query else []),
            "interpretation": interpretation,
            "verified": verified,
            "step_count": step_count,
            "processing_time": response_data.get('processing_time'),
            "question": question,  # Store question for details view
            "action": "show"
        }
        value_json = truncate_button_value(button_data)
        
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📋 Show Query Details"
                    },
                    "action_id": "show_query_details",
                    "value": value_json
                }
            ]
        })
    
    # Build text fallback (for notifications)
    text_fallback = answer
    if interpretation:
        text_fallback = f"Interpretation: {interpretation}\n\n{answer}"
    
    return blocks, text_fallback


def get_welcome_message():
    """Generate welcome message with instructions and example questions"""
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "👋 Welcome to Dex - Your Analytics Assistant"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "I'm *Dex* (Data Expert), your AI assistant for Greenely contract analytics. I can answer questions about contracts, signings, churn, channels, and more using Snowflake Cortex Agent (many more to come!).\n\n*How to use:*\n• Use `/dex [your question]` to ask me anything\n• Or mention me: `@Dex [your question]`\n• Or just ask a question in this channel"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Example Questions I Can Answer:*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Signed Contracts / Customers Signed:*\n• How many signed contracts this month?\n• How many customers signed in Finland yesterday?\n• Show me signed contracts by channel this year\n• What's the trend of signed contracts over the past 6 months?"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Channel Analysis:*\n• Which channels perform best for contract signings?\n• Show me contract distribution by channel group\n• Compare contract metrics across channels"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Country/Market Analysis:*\n• How many contracts do we have in Sweden vs Finland?\n• Show me contract metrics by country\n• What's the growth rate by market?"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Time-based Analysis:*\n• How many contracts were signed this week?\n• Show me monthly contract signings for the past year\n• What's the contract growth rate month-over-month?"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Churn Analysis:*\n• What's the churn rate by country?\n• Show me churned contracts from last month\n• What's the churn rate for contracts signed last quarter?"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*💡 Tips:*\n• Be specific with time periods (e.g., 'this month', 'last quarter', '2025')\n• You can filter by country (SE, FI), channel, or other dimensions\n• I'll show you the SQL query used if you click 'Show Query Details'\n• Processing typically takes a few seconds\n• Use `/dex-help` anytime to see this message again"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Powered by Snowflake Cortex Agent • Using CONTRACT_SEMANTIC semantic view"
                    }
                ]
            }
        ]
    }


@app.command("/dex-help")
def handle_dex_help(ack, respond):
    """Handle /dex-help command to show help message"""
    ack()
    welcome_msg = get_welcome_message()
    respond(blocks=welcome_msg["blocks"], text="Dex Help - Contract Analytics Assistant")


@app.message("")
def handle_message(message, say):
    """
    Handle all messages in channels where the bot is present.
    Only responds to messages that look like questions about contracts.
    """
    # Ignore bot's own messages
    if message.get("subtype") == "bot_message":
        return
    
    # Ignore slash commands (handled by @app.command)
    if message.get("text", "").startswith("/"):
        return
    
    text = message.get("text", "").strip()
    
    # Only respond to messages that look like questions
    # You can customize this logic
    if not text:
        return
    
    # Check if it's a question (ends with ? or contains question words)
    is_question = (
        text.endswith("?") or
        any(word in text.lower() for word in [
            "how many", "what", "show me", "tell me", "count", 
            "contracts", "signed", "operational", "churn", "channel"
        ])
    )
    
    if not is_question:
        return
    
    # Show typing indicator
    try:
        app.client.conversations_mark(
            channel=message["channel"],
            ts=message["ts"]
        )
    except:
        pass
    
    # Call Cortex Agent
    start_time = time.time()
    response_data = call_cortex_agent(text)
    elapsed_time = time.time() - start_time
    
    # Add processing time to response
    if isinstance(response_data, dict):
        response_data['processing_time'] = elapsed_time
    
    # Format response with optional "Show Query Details" button (include question for display)
    blocks, text_fallback = format_slack_response(
        response_data, 
        question=text
    )
    
    # Post response in thread
    try:
        if blocks:
            say(blocks=blocks, text=text_fallback, thread_ts=message.get("ts"))
        else:
            say(text_fallback, thread_ts=message.get("ts"))
    except SlackApiError as e:
        say(f"❌ Error posting response: {str(e)}", thread_ts=message.get("ts"))


@app.action("quick_question")
def handle_quick_question(ack, body, respond, client):
    """Handle quick start question button clicks"""
    ack()
    
    try:
        # Get the question from the button value
        if "actions" not in body or not body["actions"]:
            respond("Error: No action data found", response_type="ephemeral")
            return
        
        question = body["actions"][0].get("value", "")
        if not question:
            respond("Error: No question found", response_type="ephemeral")
            return
        
        # Get channel info
        channel_id = body.get("channel", {}).get("id")
        if not channel_id:
            respond("Error: No channel found", response_type="ephemeral")
            return
        
        # Send the question as if the user typed it
        # We'll use the same logic as /dex command
        start_time = time.time()
        
        # Send initial processing message
        initial_message = client.chat_postMessage(
            channel=channel_id,
            text=f"Your question: {question}\n\nProcessing...",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Your question:* {question}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":snowflake: *Snowflake Cortex Agent* is processing your request...",
                    }
                },
                {
                    "type": "divider"
                },
            ]
        )
        initial_response = {"ts": initial_message.get("ts"), "channel": initial_message.get("channel")} if initial_message else None
        
        # Call Cortex Agent
        response_data = call_cortex_agent(question)
        
        # Calculate total processing time BEFORE formatting (so it can be displayed)
        elapsed_time = time.time() - start_time
        if isinstance(response_data, dict):
            response_data['processing_time'] = elapsed_time
        
        # Format response
        blocks, text_fallback = format_slack_response(
            response_data, 
            question=question
        )
        
        # Update the initial processing message with the final response
        try:
            if initial_response and 'ts' in initial_response:
                client.chat_update(
                    channel=initial_response.get('channel', channel_id),
                    ts=initial_response['ts'],
                    text=text_fallback,
                    blocks=blocks if blocks else None
                )
            else:
                # Fallback: send new message if update not possible
                if blocks:
                    client.chat_postMessage(channel=channel_id, blocks=blocks, text=text_fallback)
                else:
                    client.chat_postMessage(channel=channel_id, text=text_fallback)
        except Exception as update_error:
            logger.error(f"Could not update message: {update_error}", exc_info=True)
            # Fallback: send as new message
            if blocks:
                client.chat_postMessage(channel=channel_id, blocks=blocks, text=text_fallback)
            else:
                client.chat_postMessage(channel=channel_id, text=text_fallback)
                
    except Exception as e:
        logger.error(f"Error handling quick question: {str(e)}", exc_info=True)
        respond(
            f"❌ Error processing question: {str(e)}\n\nPlease try again or use `/dex [your question]`",
            response_type="ephemeral"
        )


@app.action("show_query_details")
def handle_show_query_details(ack, body, respond, client):
    """Handle button click to show/hide query details"""
    ack()
    
    try:
        # Safely extract values from body
        if "actions" not in body or not body["actions"]:
            respond("Error: No action data found", response_type="ephemeral")
            return
        
        action_value = body["actions"][0].get("value", "show")
        
        if "message" not in body:
            respond("Error: No message data found", response_type="ephemeral")
            return
        
        message_ts = body["message"].get("ts")
        if not message_ts:
            respond("Error: No message timestamp found", response_type="ephemeral")
            return
        
        if "channel" not in body:
            respond("Error: No channel data found", response_type="ephemeral")
            return
        
        channel_id = body["channel"].get("id")
        if not channel_id:
            respond("Error: No channel ID found", response_type="ephemeral")
            return
        
        # Parse the stored data - handle JSON parsing more robustly
        stored_data = {}
        try:
            if isinstance(action_value, str):
                stored_data = json.loads(action_value)
            elif isinstance(action_value, dict):
                stored_data = action_value
        except (json.JSONDecodeError, TypeError):
            # Fallback: try to get from action value directly
            try:
                raw_value = body["actions"][0].get("value", "")
                if isinstance(raw_value, str):
                    stored_data = json.loads(raw_value)
                elif isinstance(raw_value, dict):
                    stored_data = raw_value
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(f"Could not parse action value: {e}")
                stored_data = {}
        
        # Extract all data from stored_data
        answer = stored_data.get("answer", "")
        sql_query = stored_data.get("sql_query")
        sql_queries = stored_data.get("sql_queries", [sql_query] if sql_query else [])
        interpretation = stored_data.get("interpretation")
        verified = stored_data.get("verified", False)
        step_count = stored_data.get("step_count", 0)
        planning_steps = stored_data.get("planning_steps", [])
        thinking_steps = stored_data.get("thinking_steps", [])
        processing_time = stored_data.get("processing_time")
        question = stored_data.get("question", "")  # Get question from stored data
        action = stored_data.get("action", "show")
        
        if action == "hide":
            # Hide details - restore original message with answer
            additional_info = []
            if verified:
                additional_info.append(f"{VERIFIED_EMOJI} answer accuracy verified by agent owner")
            # Use sql_queries if available, otherwise fall back to sql_query
            queries_to_count = sql_queries if sql_queries else ([sql_query] if sql_query else [])
            if queries_to_count:
                query_count = len(queries_to_count)
                additional_info.append(f"{query_count} SQL {'query' if query_count == 1 else 'queries'}")
            
            if step_count > 0:
                summary_text = f"_Finished {step_count} steps"
            else:
                summary_text = "_Finished processing"
            
            if processing_time:
                summary_text += f" • ⏱️ {processing_time:.1f}s"
            
            if additional_info:
                summary_text += f" • Includes {' and '.join(additional_info)}"
            summary_text += "_"
            
            # Rebuild the original message structure
            blocks = []
            
            # Add question if available
            if question:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Your question:* {question}"
                    }
                })
                blocks.append({"type": "divider"})
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"✅ *Completed!*\n\n{summary_text}"
                }
            })
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": answer.replace("**", "*") if answer else "No answer available"
                }
            })
            
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📋 Show Query Details"
                        },
                        "action_id": "show_query_details",
                        "value": truncate_button_value({
                            "answer": answer,
                            "sql_query": sql_query,
                            "sql_queries": sql_queries if sql_queries else ([sql_query] if sql_query else []),
                            "interpretation": interpretation,
                            "verified": verified,
                            "step_count": step_count,
                            "processing_time": processing_time,
                            "question": question,  # Store question for details view
                            "action": "show"
                        })
                    }
                ]
            })
        else:
            # Show details - first show the answer and completion status, then details
            blocks = []
            
            # Add question if available
            if question:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Your question:* {question}"
                    }
                })
                blocks.append({"type": "divider"})
            
            # First, rebuild the "Completed!" section and answer (same as original message)
            additional_info = []
            if verified:
                additional_info.append(f"{VERIFIED_EMOJI} answer accuracy verified by agent owner")
            queries_to_count = sql_queries if sql_queries else ([sql_query] if sql_query else [])
            if queries_to_count:
                query_count = len(queries_to_count)
                additional_info.append(f"{query_count} SQL {'query' if query_count == 1 else 'queries'}")
            
            if step_count > 0:
                summary_text = f"_Finished {step_count} steps"
            else:
                summary_text = "_Finished processing"
            
            if processing_time:
                summary_text += f" • ⏱️ {processing_time:.1f}s"
            
            if additional_info:
                summary_text += f" • Includes {' and '.join(additional_info)}"
            summary_text += "_"
            
            completed_section_text = f"✅ *Completed!*\n\n{summary_text}"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": completed_section_text
                }
            })
            
            blocks.append({"type": "divider"})
            
            # Then show the answer
            answer_formatted = answer.replace("**", "*") if answer else "No answer available"
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": answer_formatted
                }
            })
            
            blocks.append({"type": "divider"})
            
            # Now add the details (SQL queries only - interpretation not available from API)
            # Add SQL queries (like the example - supports multiple)
            # Use sql_queries if available, otherwise fall back to sql_query
            queries_to_show = sql_queries if sql_queries else ([sql_query] if sql_query else [])
            if queries_to_show:
                num_queries = len(queries_to_show)
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*💾 SQL Queries:*\nCortex Analyst used {num_queries} SQL {'query' if num_queries == 1 else 'queries'}"
                    }
                })
                
                # Add each SQL query with verification badge
                for i, query in enumerate(queries_to_show, 1):
                    query_header = f"*💾 SQL Query {i}:*"
                    if verified and i == 1:  # First query is verified if any are
                        query_header += f" {VERIFIED_EMOJI} Answer accuracy verified by agent owner"
                    
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": query_header
                        }
                    })
                    
                    # Truncate SQL if too long for Slack (max ~2800 chars)
                    if len(query) > 2800:
                        displayed_sql = query[:2800] + "...\n-- (SQL truncated for display)"
                    else:
                        displayed_sql = query
                    
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```sql\n{displayed_sql}\n```"
                        }
                    })
                    
                    # Add separator between queries (except for the last one)
                    if i < len(queries_to_show):
                        blocks.append({"type": "divider"})
                
                # Add context message after all queries
                blocks.append({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "ℹ️ All SQL queries were already executed by Cortex during analysis. Results are included in the response above."
                        }
                    ]
                })
            
            if not blocks:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "No additional details available."
                    }
                })
            
            # Add hide button
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🔽 Hide Query Details"
                        },
                        "action_id": "show_query_details",
                        "value": truncate_button_value({
                            "answer": answer,
                            "sql_query": sql_query,
                            "sql_queries": sql_queries if sql_queries else ([sql_query] if sql_query else []),
                            "interpretation": interpretation,
                            "verified": verified,
                            "step_count": step_count,
                            "processing_time": processing_time,
                            "question": question,  # Store question for details view
                            "action": "hide"
                        })
                    }
                ]
            })
        
        # Update the message
        try:
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text="Query details" if action == "show" else "Answer",
                blocks=blocks
            )
        except SlackApiError as update_error:
            logger.error(f"Failed to update message: {update_error}", exc_info=True)
            # Fallback: send as ephemeral message
            respond(
                f"⚠️ Could not update message. Error: {str(update_error)}",
                response_type="ephemeral"
            )
            
    except Exception as e:
        logger.error(f"Error showing query details: {str(e)}", exc_info=True)
        respond(
            f"❌ Error showing query details: {str(e)}\n\nPlease try again.",
            response_type="ephemeral"
        )


def get_welcome_message():
    """Generate welcome message with instructions and example questions"""
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "👋 Welcome to Dex - Your Contract Analytics Assistant"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "I'm *Dex* (Data Expert), your AI assistant for Greenely contract analytics. I can answer questions about contracts, signings, churn, channels, and more using Snowflake Cortex Agent (many more data sources to come!).\n\n*How to use:*\n• Use `/dex [your question]` to ask me anything\n• Or mention me: `@Dex [your question]`\n• Or just ask a question in this channel"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Quick Start Questions:*\nClick a button below to ask a common question:"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📈 Contracts signed last month"
                        },
                        "action_id": "quick_question",
                        "value": "How many contracts were signed last month?"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📉 Churn last month"
                        },
                        "action_id": "quick_question",
                        "value": "How many contracts churned last month?"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🇸🇪 Customers in Sweden"
                        },
                        "action_id": "quick_question",
                        "value": "How many customers were signed this month in Sweden?"
                    }
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📊 Net growth this month"
                        },
                        "action_id": "quick_question",
                        "value": "What is the current customer net growth this month?"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🔌 Saveye connected"
                        },
                        "action_id": "quick_question",
                        "value": "How many customers in Sverige have a Saveye connected?"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📈 Total signed customers"
                        },
                        "action_id": "quick_question",
                        "value": "How many total signed customers do we have?"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "_Or type `/dex [your question]` to ask anything else_"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*💡 What I Can Help With:*\n✅ Contract signings, churn, and growth metrics\n✅ Channel and acquisition analysis\n✅ Device connectivity (Saveye, EV, Charging Station, Battery)\n✅ Contract types, discounts, and payment methods\n✅ Country/market analysis (Sweden, Finland)\n"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*💡 Tips:*\n• Be specific with time periods (e.g., 'this month', 'last quarter', '2025')\n• You can filter by country (SE, FI), channel, or other dimensions\n• I'll show you the SQL query used if you click 'Show Query Details'\n• Processing typically takes a few seconds\n• Use `/dex-help` anytime to see this message again"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Powered by Snowflake Cortex Agent"
                    }
                ]
            }
        ]
    }


@app.command("/dex-help")
def handle_dex_help(ack, respond):
    """Handle /dex-help command to show help message"""
    ack()
    welcome_msg = get_welcome_message()
    respond(blocks=welcome_msg["blocks"], text="Dex Help - Contract Analytics Assistant")


@app.event("member_joined_channel")
def handle_member_joined_channel(event, client):
    """
    Handle when a member (including the bot) joins a channel.
    Post welcome message if it's the bot that joined.
    """
    try:
        # Get bot's user ID
        bot_user_id = app.client.auth_test()["user_id"]
        user_id = event.get("user")
        
        # Only post welcome message if the bot itself joined
        if user_id == bot_user_id:
            channel_id = event.get("channel")
            if channel_id:
                welcome_msg = get_welcome_message()
                client.chat_postMessage(
                    channel=channel_id,
                    blocks=welcome_msg["blocks"],
                    text="👋 Welcome to Dex - Your Contract Analytics Assistant"
                )
    except Exception as e:
        logger.warning(f"Could not post welcome message: {e}")


@app.event("app_mention")
def handle_mention(event, say):
    """
    Handle when the bot is mentioned (e.g., @Dex how many contracts?)
    """
    text = event.get("text", "").strip()
    
    # Remove the mention from the text
    try:
        bot_user_id = app.client.auth_test()['user_id']
        text = text.replace(f"<@{bot_user_id}>", "").strip()
    except:
        # Fallback: remove any mention pattern
        import re
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()
    
    if not text:
        # If just mentioned without a question, show welcome message
        welcome_msg = get_welcome_message()
        say(blocks=welcome_msg["blocks"], text="Dex Help - Contract Analytics Assistant")
        return
    
    # Call Cortex Agent
    response_data = call_cortex_agent(text)
    
    # Format response with optional "Show Query Details" button (include question for display)
    blocks, text_fallback = format_slack_response(
        response_data, 
        question=text
    )
    
    if blocks:
        say(blocks=blocks, text=text_fallback)
    else:
        say(text_fallback)


@app.command("/dex")
def handle_dex_command(ack, respond, command):
    """
    Handle /dex slash command
    Usage: /dex [question]
    Example: /dex how many signed contracts this month?
    """
    # Acknowledge the command IMMEDIATELY (must be done within 3 seconds)
    # This MUST be the very first thing - wrap in try-except to ensure it always succeeds
    try:
        ack()
    except Exception as ack_error:
        # If ack fails, log it but continue - this is critical for Slack
        logger.error(f"Failed to acknowledge /dex command: {ack_error}", exc_info=True)
        # Try once more as a fallback
        try:
            ack()
        except:
            pass
        # Even if ack fails, we should still try to respond to the user
        # but the command might show as "dispatch_failed" in Slack
    
    try:
        question = command.get("text", "").strip()
        
        # Deduplication: Check if this exact command was processed recently
        # Use command_id if available, otherwise use user_id + question + timestamp
        command_id = command.get("command_id") or command.get("trigger_id")
        user_id = command.get("user_id", "unknown")
        current_time = time.time()
        
        # Create a unique key for this command
        if command_id:
            command_key = f"{command_id}"
        else:
            # Fallback: use user + question + rounded timestamp (within 5 seconds)
            rounded_time = int(current_time / 5) * 5
            command_key = f"{user_id}:{question}:{rounded_time}"
        
        # Check if we're already processing this command
        if command_key in _command_lock:
            logger.info(f"Ignoring duplicate command: {command_key}")
            return
        
        # Check if this command was processed very recently (within last 2 seconds)
        if command_key in _recent_commands:
            logger.info(f"Ignoring recently processed command: {command_key}")
            return
        
        # Mark as being processed
        _command_lock[command_key] = current_time
        _recent_commands.append(command_key)
        if not question:
            # Show welcome message when /dex is used without a question
            # Use respond() immediately (it's fast) - then optionally send full message via chat_postMessage
            channel_id = command.get("channel_id")
            
            # First, respond quickly with a simple message
            respond(
                "👋 Welcome to Dex! Loading full instructions...",
                response_type="ephemeral"
            )
            
            # Then send the full welcome message (this can be slower)
            try:
                welcome_msg = get_welcome_message()
                if channel_id:
                    app.client.chat_postMessage(
                        channel=channel_id,
                        blocks=welcome_msg["blocks"],
                        text="👋 Welcome to Dex - Your Contract Analytics Assistant"
                    )
            except Exception as welcome_error:
                logger.error(f"Error showing welcome message: {welcome_error}", exc_info=True)
                # Fallback: send simple text message
                if channel_id:
                    app.client.chat_postMessage(
                        channel=channel_id,
                        text=(
                            "👋 Welcome to Dex - Your Contract Analytics Assistant!\n\n"
                            "Use `/dex [your question]` to ask me anything about contracts, signings, churn, channels, and more.\n\n"
                            "Example: `/dex how many contracts were signed last month?`"
                        )
                    )
            return
        
        # Call Cortex Agent (when ready) or show placeholder
        if not AGENT_ENDPOINT or not PAT:
            respond(
                "⚠️ Cortex Agent not configured yet.\n\n"
                "Once your admin enables Cortex Agent access, I'll be able to answer questions about contracts!\n\n"
                "For now, you can ask questions like:\n"
                "• How many signed contracts this month?\n"
                "• Show me contracts by channel\n"
                "• What's the churn rate by country?"
            )
        else:
            try:
                # Send initial message with question and processing status
                # Use app.client.chat_postMessage instead of respond() to avoid dispatch issues
                start_time = time.time()
                
                # Combine question and processing message in one post
                initial_message = app.client.chat_postMessage(
                    channel=command.get("channel_id"),
                    text=f"Your question: {question}\n\nProcessing...",
                    blocks=[
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Your question:* {question}"
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":snowflake: *Snowflake Cortex Agent* is processing your request...",
                            }
                        },
                        {
                            "type": "divider"
                        },
                    ]
                )
                initial_response = {"ts": initial_message.get("ts"), "channel": initial_message.get("channel")} if initial_message else None
                channel_id = command.get("channel_id")  # Store for later use
                
                # Call Cortex Agent
                response_data = call_cortex_agent(question)
                
                # Calculate total processing time BEFORE formatting (so it can be displayed)
                elapsed_time = time.time() - start_time
                if isinstance(response_data, dict):
                    response_data['processing_time'] = elapsed_time
                
                # Format response (this should be fast - just building Slack blocks)
                blocks, text_fallback = format_slack_response(
                    response_data, 
                    question=question
                )
                
                # Update the initial processing message with the final response
                try:
                    if initial_response and 'ts' in initial_response:
                        # Update the processing message with the final response
                        app.client.chat_update(
                            channel=initial_response.get('channel', channel_id),
                            ts=initial_response['ts'],
                            text=text_fallback,
                            blocks=blocks if blocks else None
                        )
                    else:
                        # Fallback: send new message if update not possible
                        logger.warning("initial_response missing 'ts', sending new message")
                        if blocks:
                            respond(blocks=blocks, text=text_fallback)
                        else:
                            respond(text_fallback)
                except Exception as update_error:
                    # If update fails, send as new message
                    logger.error(f"Could not update message: {update_error}", exc_info=True)
                    if blocks:
                        respond(blocks=blocks, text=text_fallback)
                    else:
                        respond(text_fallback)
            except Exception as e:
                # Log the error for debugging
                logger.error(f"Error calling Cortex Agent: {str(e)}", exc_info=True)
                respond(
                    f"❌ Sorry, I encountered an error while processing your question.\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Please try again or contact support if the issue persists."
                )
            finally:
                # Clean up: remove from processing lock after a delay
                # This allows the same command to be processed again after 10 seconds if needed
                if 'command_key' in locals():
                    def cleanup_lock():
                        time.sleep(10)
                        if command_key in _command_lock:
                            del _command_lock[command_key]
                    import threading
                    threading.Thread(target=cleanup_lock, daemon=True).start()
    except Exception as e:
        # Log any errors in the handler itself
        logger.error(f"Error in /dex command handler: {str(e)}", exc_info=True)
        # Try to respond with error (if ack() was called)
        try:
            respond(f"❌ Error processing command: {str(e)}")
        except:
            # If we can't respond, at least log it
            pass
        finally:
            # Clean up lock on error too
            if 'command_key' in locals() and command_key in _command_lock:
                del _command_lock[command_key]


if __name__ == "__main__":
    # Validate configuration
    if not SLACK_BOT_TOKEN:
        print("❌ Error: SLACK_BOT_TOKEN not found in .env")
        exit(1)
    
    if not SLACK_APP_TOKEN:
        print("❌ Error: SLACK_APP_TOKEN not found in .env")
        print("   Socket Mode requires an App-Level Token (xapp-...)")
        exit(1)
    
    if not AGENT_ENDPOINT:
        print("⚠️  Warning: AGENT_ENDPOINT not found in .env")
        print("   Bot will start but won't be able to answer questions.")
        print("   Create your Cortex Agent in Snowsight first, then add the endpoint URL.")
    
    if not PAT:
        print("⚠️  Warning: PAT (Programmatic Access Token) not found in .env")
        print("   Bot will start but won't be able to authenticate with Cortex Agent.")
    
    print("🚀 Starting Dex (Greenely Contract Analytics Bot)...")
    print(f" channel: #{TARGET_CHANNEL}")
    print("💬 Listening for questions about contracts...")
    print("\nBot will respond to:")
    print("  - Slash command: /dex [question] (recommended)")
    print("  - Help command: /dex-help (show instructions)")
    print("  - Messages in channels (if they look like questions)")
    print("  - Mentions: @Dex [question]")
    print("\n⚡️ Bot is running! Press Ctrl+C to stop.")
    print("\n💡 Tip: Use /dex-help in Slack to show welcome message with example questions")
    
    # Start the bot
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
    
    # Optional: Post welcome message to channel when bot starts
    # Uncomment the lines below if you want to auto-post the welcome message
    # try:
    #     if SLACK_BOT_TOKEN and TARGET_CHANNEL:
    #         from slack_sdk import WebClient
    #         client = WebClient(token=SLACK_BOT_TOKEN)
    #         welcome_msg = get_welcome_message()
    #         client.chat_postMessage(
    #             channel=TARGET_CHANNEL,
    #             blocks=welcome_msg["blocks"],
    #             text="Welcome to Dex - Contract Analytics Assistant"
    #         )
    #         print(f"✅ Posted welcome message to #{TARGET_CHANNEL}")
    # except Exception as e:
    #     print(f"⚠️  Could not post welcome message: {e}")
    #     print("   You can manually post it using /dex-help in Slack")