# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an **unofficial** MCP (Model Context Protocol) server that wraps the MLIT (Ministry of Land, Infrastructure, Transport and Tourism) Data Platform API. It enables LLMs to search and retrieve Japanese government infrastructure data through natural language queries.

**Important**: This is NOT an official MLIT project and has NO endorsement from the Japanese government.

## Architecture

### Core Components

**MCP Server Layer** (`src/server.py`)
- Defines MCP tools using the `mcp` SDK
- Each tool corresponds to an API operation (search, get_data, normalize_codes, etc.)
- Tools validate inputs using Pydantic schemas and delegate to `MLITClient`
- The server runs via stdio transport (`stdio_server`)

**API Client Layer** (`src/client.py`)
- `MLITClient`: Async GraphQL client for MLIT Data Platform API
- Handles authentication via API key header
- Implements retry logic with exponential backoff for transient errors (429, 5xx)
- Token-bucket rate limiter (default: 4 RPS)
- Caches prefecture/municipality data to reduce API calls
- Uses `aiohttp` for async HTTP operations

**Schema Layer** (`src/schemas.py`)
- Pydantic models for input validation
- Key schemas: `SearchBase`, `SearchByRect`, `SearchByPoint`, `SearchByAttr`, `GetAllDataInput`
- Validators ensure constraints (e.g., `size` capped at 500)

**Configuration** (`src/config.py`)
- Loads `.env` file via `python-dotenv`
- Required: `MLIT_API_KEY`
- Optional: `MLIT_BASE_URL` (defaults to `https://www.mlit-data.jp/api/v1/`)
- Settings validation via Pydantic

**Utilities** (`src/utils.py`)
- JSON logger with structured logging
- `RateLimiter`: Token-bucket async rate limiter
- `Timer`: Context manager for performance tracking
- Request ID generation for tracing

### GraphQL Query Building

The client builds GraphQL queries dynamically based on tool parameters:
- **Minimal mode** (`minimal=True`): Returns only `id, title, lat, lon, dataset_id`
- **Basic mode**: Adds `year, catalog_id`
- **Detail mode** (default): Includes `theme, metadata, hasThumbnail`

Spatial queries use GraphQL `location_rectangle` and `location_point_distance` fields.

### Data Flow

1. MCP tool receives request from LLM
2. Input validated against Pydantic schema
3. `MLITClient` builds GraphQL query
4. Rate limiter enforces request throttling
5. HTTP request sent with API key authentication
6. Response parsed and returned to MCP tool
7. Tool formats result for LLM consumption

## Development Commands

### Setup

```bash
# Install dependencies
uv pip install -e .

# Or with pip
pip install -e .

# Create .env file from example
cp .env.example .env
# Edit .env and add your MLIT_API_KEY
```

### Running the MCP Server

The MCP server is designed to run via Claude Desktop or Claude Code, not standalone. Configuration:

**For Claude Code** (`.mcp.json`):
```json
{
  "mcpServers": {
    "mlit-dpf-mcp": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/mlit-dpf-mcp", "run", "python", "-m", "src.server"],
      "env": {
        "MLIT_API_KEY": "your_api_key_here",
        "MLIT_BASE_URL": "https://www.mlit-data.jp/api/v1/"
      }
    }
  }
}
```

**For Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "mlit-dpf-mcp": {
      "command": "/path/to/.venv/Scripts/python.exe",  // or bin/python on Unix
      "args": ["/path/to/src/server.py"],
      "env": {
        "MLIT_API_KEY": "your_api_key_here",
        "MLIT_BASE_URL": "https://www.mlit-data.jp/api/v1/",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

### Testing the Server Manually

```bash
# Set environment variables
export MLIT_API_KEY=your_api_key_here
export MLIT_BASE_URL=https://www.mlit-data.jp/api/v1/

# Run server (will communicate via stdio)
python -m src.server
```

### Sample Scripts

**Data Visualization** (requires `folium`):
```bash
# Install visualization dependency
uv pip install folium

# Generate interactive map of Tokai region dams
python plot_tokai_dams.py

# Open in browser
xdg-open tokai_dams_map.html  # Linux
# or start tokai_dams_map.html  # Windows
```

**Data Fetching**:
```bash
# Fetch dam data for Tokai region
python fetch_tokai_dams.py

# Get and plot dams in one step
python fetch_and_plot_dams.py
```

## Debugging

### Enable Detailed Logging

Set environment variables:

```bash
# Log full GraphQL queries
export MLIT_DEBUG_QUERY=1

# Log full HTTP responses
export MLIT_DEBUG_RESP=1

# Control response body truncation (default: 4000 chars)
export MLIT_LOG_BODY_LIMIT=10000

# Change log level (default: INFO)
export LOG_LEVEL=DEBUG
```

### Common Issues

**API Key Errors**: Verify `MLIT_API_KEY` is set correctly in `.env` or MCP config. No quotes needed in `.env` files.

**Rate Limiting**: If hitting 429 errors, the client will retry automatically with exponential backoff. Adjust `rps` in `config.py` if needed.

**Timeout Errors**: Default timeout is 30s. Set `MLIT_TIMEOUT_S` environment variable to increase.

**Import Errors**: The `src/server.py` has a robust import header that adds project root to `sys.path`. This ensures modules can be imported whether running as script or module.

## Key MCP Tools

- `search`: Keyword search with sorting and pagination
- `search_by_location_rectangle`: Spatial search with bounding box
- `search_by_location_point_distance`: Spatial search with center point + radius
- `search_by_attribute`: Search by metadata attributes (catalog_id, dataset_id, prefecture_code, etc.)
- `get_data`: Retrieve detailed data by dataset_id + data_id
- `get_all_data`: Bulk retrieval with automatic pagination (up to 1000 items per batch)
- `normalize_codes`: Convert prefecture/municipality names to standard codes
- `get_file_download_urls`: Get time-limited download URLs (60s expiry)
- `get_thumbnail_urls`: Get thumbnail image URLs (60s expiry)

## Code Conventions

- **Async/await**: All API calls are async; use `asyncio` patterns
- **Logging**: Use structured JSON logging via `logger` from `utils.py`
- **Error Handling**: Transient errors (429, 5xx) trigger retries; client errors (400, 401, 404) fail immediately
- **Validation**: All tool inputs validated via Pydantic before API calls
- **Normalization**: Use `normalize_codes` tool to convert Japanese place names to official codes before searching

## API Constraints

- Maximum 500 items per single search request (`size` parameter)
- Maximum 1000 items per `get_all_data` batch
- Download URLs expire after 60 seconds
- Default rate limit: 4 requests/second
- Coordinates must use WGS84 (world geodetic system)

## Important Files Not to Modify

- `uv.lock`: Dependency lock file managed by `uv`
- `tokai_dams.json`: Sample data used for demo visualization
- `tokai_dams_map.html`: Generated output (can be regenerated)

## MLIT Data Platform Resources

- Official API Documentation: https://www.mlit-data.jp/api_docs/
- Official GitHub (basis for this fork): https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp
- Terms of Use: https://www.mlit-data.jp/assets/policy/国土交通データプラットフォーム利用規約.pdf
