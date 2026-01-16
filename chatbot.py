"""
OCE Chatbot - Gemini 3.0 Flash with Function Calling

Provides a conversational interface to NYC procurement data using the same
tools as the MCP server.
"""

import os
from typing import Optional
from google import genai
from google.genai import types

# Import tool functions from MCP server
from mcp_server import (
    search_vendors,
    get_vendor_profile,
    search_contracts,
    get_contract_details,
    search_solicitations,
    get_solicitation_details,
    search_transactions,
    get_vendor_spending,
    get_spending_by_year,
    get_datasets_info
)


# Gemini client (initialized lazily)
_client = None


def get_client():
    """Get or create Gemini client."""
    global _client
    if _client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        _client = genai.Client(api_key=api_key)
    return _client


# Tool function registry (get_chart_data added after its definition)
TOOL_FUNCTIONS = {
    "search_vendors": search_vendors,
    "get_vendor_profile": get_vendor_profile,
    "search_contracts": search_contracts,
    "get_contract_details": get_contract_details,
    "search_solicitations": search_solicitations,
    "get_solicitation_details": get_solicitation_details,
    "search_transactions": search_transactions,
    "get_vendor_spending": get_vendor_spending,
    "get_spending_by_year": get_spending_by_year,
    "get_datasets_info": get_datasets_info,
}


def get_tools():
    """Define tool declarations for Gemini function calling."""
    return [
        types.Tool(function_declarations=[
            types.FunctionDeclaration(
                name="search_vendors",
                description="Search NYC vendors by name. Returns list of matching vendors with IDs.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "query": types.Schema(type="STRING", description="Vendor name to search for"),
                        "limit": types.Schema(type="INTEGER", description="Max results (default 20)")
                    },
                    required=["query"]
                )
            ),
            types.FunctionDeclaration(
                name="get_vendor_profile",
                description="Get detailed profile for a vendor including contract history.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "vendor_id": types.Schema(type="STRING", description="PASSPort Supplier ID")
                    },
                    required=["vendor_id"]
                )
            ),
            types.FunctionDeclaration(
                name="search_contracts",
                description="Search NYC contracts with optional filters for vendor, agency, status.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "query": types.Schema(type="STRING", description="Search in contract ID or title"),
                        "vendor": types.Schema(type="STRING", description="Filter by vendor name"),
                        "agency": types.Schema(type="STRING", description="Filter by agency name"),
                        "status": types.Schema(type="STRING", description="Filter by status (Registered, Active)"),
                        "limit": types.Schema(type="INTEGER", description="Max results (default 20)")
                    }
                )
            ),
            types.FunctionDeclaration(
                name="get_contract_details",
                description="Get detailed information for a specific contract.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "contract_id": types.Schema(type="STRING", description="Contract ID (e.g., CT1-...)")
                    },
                    required=["contract_id"]
                )
            ),
            types.FunctionDeclaration(
                name="search_solicitations",
                description="Search NYC solicitations (RFPs, bids) with optional filters.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "query": types.Schema(type="STRING", description="Search in EPIN or name"),
                        "status": types.Schema(type="STRING", description="Filter by status"),
                        "agency": types.Schema(type="STRING", description="Filter by agency"),
                        "limit": types.Schema(type="INTEGER", description="Max results")
                    }
                )
            ),
            types.FunctionDeclaration(
                name="get_solicitation_details",
                description="Get detailed information for a specific solicitation.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "epin": types.Schema(type="STRING", description="Solicitation EPIN")
                    },
                    required=["epin"]
                )
            ),
            types.FunctionDeclaration(
                name="search_transactions",
                description="Search NYC spending transactions from Checkbook NYC (147M+ records).",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "query": types.Schema(type="STRING", description="Search by payee, agency, or contract ID"),
                        "limit": types.Schema(type="INTEGER", description="Max results (default 50)")
                    },
                    required=["query"]
                )
            ),
            types.FunctionDeclaration(
                name="get_vendor_spending",
                description="Get spending history for a vendor from Checkbook NYC.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "vendor_name": types.Schema(type="STRING", description="Vendor/payee name"),
                        "fiscal_year": types.Schema(type="INTEGER", description="Optional fiscal year filter")
                    },
                    required=["vendor_name"]
                )
            ),
            types.FunctionDeclaration(
                name="get_spending_by_year",
                description="Get aggregate spending statistics for a fiscal year.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "fiscal_year": types.Schema(type="INTEGER", description="Fiscal year (e.g., 2024)")
                    },
                    required=["fiscal_year"]
                )
            ),
            types.FunctionDeclaration(
                name="get_datasets_info",
                description="Get information about available datasets and data freshness.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={}
                )
            ),
            types.FunctionDeclaration(
                name="get_chart_data",
                description="Get data formatted for creating a chart visualization. Use this when the user asks for a chart, graph, or visual representation of spending, contracts, or other data. Returns data that can be rendered as a Chart.js chart.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={
                        "chart_type": types.Schema(type="STRING", description="Type of chart: 'bar', 'pie', 'line', or 'doughnut'"),
                        "data_type": types.Schema(type="STRING", description="What to chart: 'spending_by_agency', 'spending_by_year', 'contracts_by_agency', 'contracts_by_status'"),
                        "fiscal_year": types.Schema(type="INTEGER", description="Optional: fiscal year to filter for spending data"),
                        "limit": types.Schema(type="INTEGER", description="Number of items to show (default 10)")
                    },
                    required=["chart_type", "data_type"]
                )
            ),
        ])
    ]


def get_chart_data(chart_type: str, data_type: str, fiscal_year: int = 2024, limit: int = 10) -> str:
    """
    Get chart data for visualization.
    Returns a JSON string with Chart.js compatible configuration.
    """
    import json
    from mcp_server import query_db, get_spending_connection, get_spending_files
    
    chart_config = {
        "type": chart_type,
        "data": {"labels": [], "datasets": [{"data": [], "backgroundColor": []}]},
        "options": {"responsive": True}
    }
    
    # Color palette
    colors = [
        "#667eea", "#764ba2", "#f093fb", "#f5576c", "#4facfe",
        "#00f2fe", "#43e97b", "#38f9d7", "#fa709a", "#fee140",
        "#30cfd0", "#330867", "#a8edea", "#fed6e3", "#5ee7df"
    ]
    
    try:
        if data_type == "spending_by_agency":
            con = get_spending_connection()
            files = get_spending_files(fiscal_year)
            result = con.execute(f"""
                SELECT agency, SUM(TRY_CAST(check_amount AS DOUBLE)) as total
                FROM read_parquet({files}, union_by_name=true)
                GROUP BY agency
                ORDER BY total DESC
                LIMIT {limit}
            """).fetchall()
            con.close()
            
            labels = [r[0][:30] for r in result]  # Truncate long names
            data = [round(r[1] / 1e9, 2) for r in result]  # Convert to billions
            
            chart_config["data"]["labels"] = labels
            chart_config["data"]["datasets"][0]["data"] = data
            chart_config["data"]["datasets"][0]["label"] = f"FY{fiscal_year} Spending ($B)"
            chart_config["data"]["datasets"][0]["backgroundColor"] = colors[:len(data)]
            chart_config["options"]["plugins"] = {"title": {"display": True, "text": f"Top {limit} Agencies by Spending - FY{fiscal_year}"}}
            
        elif data_type == "spending_by_year":
            con = get_spending_connection()
            totals = []
            for fy in range(2020, 2025):
                try:
                    files = get_spending_files(fy)
                    result = con.execute(f"""
                        SELECT SUM(TRY_CAST(check_amount AS DOUBLE)) as total
                        FROM read_parquet({files}, union_by_name=true)
                    """).fetchone()
                    totals.append((fy, round(result[0] / 1e9, 2) if result[0] else 0))
                except:
                    totals.append((fy, 0))
            con.close()
            
            chart_config["data"]["labels"] = [f"FY{t[0]}" for t in totals]
            chart_config["data"]["datasets"][0]["data"] = [t[1] for t in totals]
            chart_config["data"]["datasets"][0]["label"] = "Total Spending ($B)"
            chart_config["data"]["datasets"][0]["backgroundColor"] = colors[:len(totals)]
            chart_config["options"]["plugins"] = {"title": {"display": True, "text": "NYC Spending by Fiscal Year"}}
            
        elif data_type == "contracts_by_agency":
            result = query_db("""
                SELECT agency, COUNT(*) as count
                FROM contracts
                GROUP BY agency
                ORDER BY count DESC
                LIMIT ?
            """, (limit,))
            
            labels = [r['agency'][:25] for r in result]
            data = [r['count'] for r in result]
            
            chart_config["data"]["labels"] = labels
            chart_config["data"]["datasets"][0]["data"] = data
            chart_config["data"]["datasets"][0]["label"] = "Number of Contracts"
            chart_config["data"]["datasets"][0]["backgroundColor"] = colors[:len(data)]
            chart_config["options"]["plugins"] = {"title": {"display": True, "text": f"Top {limit} Agencies by Contract Count"}}
            
        elif data_type == "contracts_by_status":
            result = query_db("""
                SELECT status, COUNT(*) as count
                FROM contracts
                GROUP BY status
                ORDER BY count DESC
            """)
            
            labels = [r['status'] or 'Unknown' for r in result]
            data = [r['count'] for r in result]
            
            chart_config["data"]["labels"] = labels
            chart_config["data"]["datasets"][0]["data"] = data
            chart_config["data"]["datasets"][0]["backgroundColor"] = colors[:len(data)]
            chart_config["options"]["plugins"] = {"title": {"display": True, "text": "Contracts by Status"}}
        
        else:
            return f"Unknown data type: {data_type}"
        
        # Return as special format that frontend will recognize
        return f"__CHART__{json.dumps(chart_config)}__CHART__"
        
    except Exception as e:
        return f"Error generating chart data: {str(e)}"


# Add get_chart_data to the registry
TOOL_FUNCTIONS["get_chart_data"] = get_chart_data


def execute_function(name: str, args: dict) -> str:
    """Execute a tool function and return the result."""
    func = TOOL_FUNCTIONS.get(name)
    if not func:
        return f"Unknown function: {name}"
    
    try:
        return func(**args)
    except Exception as e:
        return f"Error executing {name}: {str(e)}"


SYSTEM_PROMPT = """You are a helpful assistant for NYC Contract Explorer (OCE), a platform for exploring NYC procurement data.

You have access to tools that let you:
- Search vendors, contracts, and solicitations from the PASSPort system
- Query 147M+ spending transactions from Checkbook NYC
- Get detailed profiles and statistics
- CREATE CHARTS using the get_chart_data tool

When answering questions:
- Use the appropriate tools to find accurate data
- Format monetary values as currency ($X,XXX.XX)
- Be concise but informative

**IMPORTANT - CHARTS**: When users ask for a chart, graph, or visualization:
1. Use get_chart_data tool with the appropriate parameters
2. The tool returns data wrapped in __CHART__...__CHART__ markers
3. You MUST include this __CHART__...__CHART__ data EXACTLY as returned in your response
4. Add a brief description AFTER the chart data

Example response format when chart data is returned:
__CHART__{"type":"bar",...}__CHART__

This chart shows the top agencies by spending in FY2024.

Available chart types:
- spending_by_agency: Top agencies by spending (bar or pie)
- spending_by_year: Spending trends over years (bar or line)  
- contracts_by_agency: Contract counts by agency (bar or pie)
- contracts_by_status: Contract status breakdown (pie or doughnut)

You are embedded on oce.wegov.nyc and help users explore NYC government procurement data."""


def chat(message: str, history: list = None) -> str:
    """
    Process a chat message and return a response.
    
    Args:
        message: User's message
        history: Optional conversation history
        
    Returns:
        Assistant's response
    """
    client = get_client()
    tools = get_tools()
    
    # Build conversation contents
    contents = []
    
    # Add history if provided
    if history:
        for msg in history:
            contents.append(types.Content(
                role=msg["role"],
                parts=[types.Part(text=msg["content"])]
            ))
    
    # Add current message
    contents.append(types.Content(
        role="user",
        parts=[types.Part(text=message)]
    ))
    
    # Generate response
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=contents,
        config=types.GenerateContentConfig(
            tools=tools,
            system_instruction=SYSTEM_PROMPT
        )
    )
    
    # Check for function calls
    max_iterations = 5  # Prevent infinite loops
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        
        # Get the response content
        if not response.candidates or not response.candidates[0].content.parts:
            return "I couldn't generate a response. Please try again."
        
        part = response.candidates[0].content.parts[0]
        
        # Check if it's a function call
        if hasattr(part, 'function_call') and part.function_call:
            fc = part.function_call
            
            # Execute the function
            result = execute_function(fc.name, dict(fc.args))
            
            # Add function call and result to contents
            contents.append(response.candidates[0].content)
            contents.append(types.Content(
                role="user",
                parts=[types.Part(function_response=types.FunctionResponse(
                    name=fc.name,
                    response={"result": result}
                ))]
            ))
            
            # Get next response
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    tools=tools,
                    system_instruction=SYSTEM_PROMPT
                )
            )
        else:
            # Text response - we're done
            return part.text
    
    return "I'm having trouble processing this request. Please try a simpler question."
