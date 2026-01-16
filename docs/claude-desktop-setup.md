---
layout: default
title: Claude Desktop Setup
---

# Claude Desktop Setup Guide

Connect Claude Desktop to the NYC Contracting Explorer for AI-powered procurement analysis.

## Quick Setup (2 minutes)

### Step 1: Find your config file

**Mac:**

```
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Windows:**

```
%APPDATA%\Claude\claude_desktop_config.json
```

> **Tip:** If the file doesn't exist, create it.

---

### Step 2: Add this configuration

Copy and paste this into the file:

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

Save the file.

---

### Step 3: Restart Claude Desktop

Completely quit Claude Desktop and reopen it.

---

### Step 4: Try it out!

Ask Claude things like:

- *"How many contracts did Parks have in 2023?"*
- *"Give me an overview of NYC procurement data"*
- *"What vendors does the Department of Education use?"*
- *"Show me vendor certification statistics"*

---

## Available Tools

### Statistics & Aggregation

| Tool | What it does |
| ---- | ------------ |
| `get_database_overview` | Complete summary of all NYC procurement data |
| `get_contract_stats` | Count contracts by agency and/or fiscal year |
| `get_agency_contracts` | Summary for specific agency with top vendors |
| `get_vendor_stats` | Vendor breakdown by certification type |
| `get_solicitation_stats` | Solicitation trends by status and method |
| `get_yearly_trends` | Year-over-year procurement trends |

### Search Tools

| Tool | What it does |
| ---- | ------------ |
| `search_vendors` | Find NYC vendors by name |
| `search_contracts` | Search contracts by agency, vendor, status |
| `search_solicitations` | Find open/past solicitations |

### Spending Data

| Tool | What it does |
| ---- | ------------ |
| `get_spending_by_year` | Checkbook NYC spending by fiscal year |
| `get_vendor_spending` | Spending history for specific vendor |

---

## Data Available

- **55,000+** NYC contracts
- **34,000+** registered vendors
- **9,600+** solicitations
- **147M+** spending transactions (via Checkbook NYC)

---

## Troubleshooting

### "Could Not Load App Settings" error

Make sure your JSON is valid. Check for:
- Missing commas
- Unclosed brackets
- Extra trailing commas

### Connection timeout

The MCP server may take a few seconds to connect on first use. Try again after a moment.

### Tools not appearing

Completely quit Claude Desktop (not just close the window) and reopen it.

---

## Need Help?

- [Live Demo (oce.wegov.nyc)](https://oce.wegov.nyc)
- [GitHub Repository](https://github.com/wegovnyc/nyc-contracting-explorer)
- [WeGov NYC](https://wegov.nyc)
