# NYC Contracting Explorer

A comprehensive platform for exploring New York City's procurement data, including vendor information, contracts, solicitations, and spending analysis. Features an AI-powered chatbot and MCP (Model Context Protocol) server for use with Claude Desktop.

## Features

- 🔍 **Contract Search** - Search 55,000+ NYC contracts by agency, vendor, or keyword
- 🏢 **Vendor Directory** - Browse 34,000+ registered vendors with certification data
- 📋 **Solicitation Tracker** - Monitor open and closed procurement opportunities
- 💰 **Spending Analysis** - Analyze 147M+ transactions from Checkbook NYC
- 🤖 **AI Chatbot** - Natural language queries powered by Google Gemini
- 🔌 **MCP Server** - Claude Desktop integration for AI-assisted analysis

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/wegovnyc/nyc-contracting-explorer.git
cd nyc-contracting-explorer

# Install dependencies
pip install -r requirements.txt

# Run the web application
python app.py
```

Visit `http://localhost:5000` to access the explorer.

## Claude Desktop Integration

Add this to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
    "mcpServers": {
        "nyc-contracts": {
            "command": "npx",
            "args": [
                "-y",
                "mcp-remote",
                "https://oce.wegov.nyc/mcp/mcp"
            ]
        }
    }
}
```

Restart Claude Desktop, then ask questions like:
- "How many contracts did Parks have in 2023?"
- "Show me vendor certification statistics"
- "What are the top IT vendors for the city?"

## MCP Tools Available

### Statistics & Aggregation
| Tool | Description |
|------|-------------|
| `get_database_overview` | Complete summary of all procurement data |
| `get_contract_stats` | Count contracts by agency and/or fiscal year |
| `get_agency_contracts` | Agency summary with top vendors |
| `get_vendor_stats` | Vendor breakdown by certification type |
| `get_solicitation_stats` | Solicitation trends by status and method |
| `get_yearly_trends` | Year-over-year procurement trends |

### Search Tools
| Tool | Description |
|------|-------------|
| `search_vendors` | Find vendors by name |
| `search_contracts` | Search contracts by agency, vendor, status |
| `search_solicitations` | Find solicitations by status or agency |

### Spending Data
| Tool | Description |
|------|-------------|
| `get_spending_by_year` | Checkbook NYC spending by fiscal year |
| `get_vendor_spending` | Spending history for specific vendor |

## Data Sources

- **PASSPort (MOCS)** - Vendor registrations, contracts, solicitations
- **Checkbook NYC** - City spending transactions
- **City Record Online** - Public notices and procurement announcements

## Project Structure

```
nyc-contracting-explorer/
├── app.py              # Flask web application
├── mcp_server.py       # MCP server for Claude Desktop
├── serve_sse.py        # Streamable HTTP server with OAuth
├── chatbot.py          # Gemini-powered AI chatbot
├── build_database.py   # Database construction scripts
├── daily_update.py     # Daily data refresh
├── templates/          # Jinja2 HTML templates
├── static/             # CSS, JS, images
└── requirements.txt    # Python dependencies
```

## Deployment

See [Deployment Guide](docs/deployment.md) for production setup with nginx and systemd.

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting PRs.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- **Live Demo**: [oce.wegov.nyc](https://oce.wegov.nyc)
- **WeGov NYC**: [wegov.nyc](https://wegov.nyc)
