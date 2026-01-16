"""
OCE MCP Server - Serves MCP over Streamable HTTP with full OAuth flow.

Implements OAuth 2.1 authorization code flow for Claude Online compatibility:
- /.well-known/oauth-protected-resource - Resource metadata
- /.well-known/oauth-authorization-server - Server metadata  
- /register - Dynamic Client Registration
- /authorize - Authorization endpoint (auto-approves)
- /token - Token endpoint (issues dummy tokens)
- /mcp/mcp - Streamable HTTP MCP endpoint
"""
import asyncio
import uuid
import time
import urllib.parse
from contextlib import asynccontextmanager
from mcp_server import mcp
from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

# Disable internal DNS rebinding protection
if mcp.settings.transport_security:
    mcp.settings.transport_security.enable_dns_rebinding_protection = False

# In-memory storage for authorization codes (simple implementation)
AUTH_CODES = {}


@asynccontextmanager
async def lifespan(app):
    """Manage the lifecycle of the Streamable HTTP session manager."""
    async with mcp.session_manager.run():
        yield


# Get the Streamable HTTP app
http_app = mcp.streamable_http_app()


# OAuth Discovery Endpoints
async def oauth_protected_resource(request: Request) -> JSONResponse:
    """Returns OAuth Protected Resource Metadata per RFC 9728."""
    return JSONResponse({
        "resource": "https://oce.wegov.nyc/mcp/mcp",
        "authorization_servers": ["https://oce.wegov.nyc"],
    })


async def oauth_authorization_server(request: Request) -> JSONResponse:
    """Returns OAuth Authorization Server Metadata per RFC 8414."""
    return JSONResponse({
        "issuer": "https://oce.wegov.nyc",
        "authorization_endpoint": "https://oce.wegov.nyc/authorize",
        "token_endpoint": "https://oce.wegov.nyc/token",
        "registration_endpoint": "https://oce.wegov.nyc/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": ["claudeai", "mcp"],
    })


async def oauth_register(request: Request) -> JSONResponse:
    """Dynamic Client Registration endpoint (RFC 7591)."""
    try:
        body = await request.json()
    except:
        body = {}
    
    client_id = f"oce-client-{uuid.uuid4().hex[:16]}"
    
    return JSONResponse(
        status_code=201,
        content={
            "client_id": client_id,
            "client_id_issued_at": int(time.time()),
            "redirect_uris": body.get("redirect_uris", []),
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        }
    )


async def oauth_authorize(request: Request) -> RedirectResponse:
    """
    OAuth Authorization endpoint.
    Auto-approves all requests and redirects back with an authorization code.
    This is a "public" server that doesn't require actual user authentication.
    """
    # Extract OAuth parameters
    client_id = request.query_params.get("client_id", "")
    redirect_uri = request.query_params.get("redirect_uri", "")
    state = request.query_params.get("state", "")
    code_challenge = request.query_params.get("code_challenge", "")
    code_challenge_method = request.query_params.get("code_challenge_method", "")
    scope = request.query_params.get("scope", "")
    
    # Generate authorization code
    auth_code = uuid.uuid4().hex
    
    # Store the code with its parameters (for token exchange)
    AUTH_CODES[auth_code] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "scope": scope,
        "created_at": time.time(),
    }
    
    # Build redirect URL with authorization code
    redirect_params = {
        "code": auth_code,
        "state": state,
    }
    
    redirect_url = f"{redirect_uri}?{urllib.parse.urlencode(redirect_params)}"
    
    return RedirectResponse(url=redirect_url, status_code=302)


async def oauth_token(request: Request) -> JSONResponse:
    """
    OAuth Token endpoint.
    Exchanges authorization code for access token.
    Issues a dummy token since this server doesn't require auth.
    """
    try:
        # Handle both form and JSON body
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            body = await request.json()
        else:
            form = await request.form()
            body = dict(form)
    except:
        body = {}
    
    grant_type = body.get("grant_type", "")
    code = body.get("code", "")
    client_id = body.get("client_id", "")
    redirect_uri = body.get("redirect_uri", "")
    code_verifier = body.get("code_verifier", "")
    
    # Validate the authorization code
    if code not in AUTH_CODES:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_grant", "error_description": "Invalid authorization code"}
        )
    
    stored = AUTH_CODES.pop(code)  # Use code only once
    
    # Check if code is expired (5 minutes)
    if time.time() - stored["created_at"] > 300:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_grant", "error_description": "Authorization code expired"}
        )
    
    # Generate access token
    access_token = f"oce-token-{uuid.uuid4().hex}"
    
    return JSONResponse({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": stored.get("scope", "mcp"),
    })


# Configure CORS
middleware = [
    Middleware(
        CORSMiddleware, 
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
]

# Build the app with all OAuth routes
app = Starlette(
    routes=[
        # OAuth discovery endpoints
        Route("/.well-known/oauth-protected-resource", endpoint=oauth_protected_resource, methods=["GET"]),
        Route("/.well-known/oauth-protected-resource/{path:path}", endpoint=oauth_protected_resource, methods=["GET"]),
        Route("/.well-known/oauth-authorization-server", endpoint=oauth_authorization_server, methods=["GET"]),
        # OAuth flow endpoints
        Route("/register", endpoint=oauth_register, methods=["POST"]),
        Route("/authorize", endpoint=oauth_authorize, methods=["GET"]),
        Route("/token", endpoint=oauth_token, methods=["POST"]),
        # MCP endpoint
        Mount("/mcp", app=http_app),
    ],
    middleware=middleware,
    lifespan=lifespan
)
