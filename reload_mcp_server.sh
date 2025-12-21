#!/bin/bash
# Alternative: Reload MCP server by modifying and restoring config file

CONFIG_FILE="$HOME/.cursor/mcp.json"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "⚠ Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "Reloading MCP server by temporarily modifying config..."

# Backup original
cp "$CONFIG_FILE" "${CONFIG_FILE}.bak"

# Add a timestamp field to trigger reload
python3 << PYTHON
import json
import time

config_file = "$CONFIG_FILE"

try:
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Add a timestamp field to trigger reload
    if 'mcpServers' in config and 'mcpkit-data' in config['mcpServers']:
        if 'env' not in config['mcpServers']['mcpkit-data']:
            config['mcpServers']['mcpkit-data']['env'] = {}
        config['mcpServers']['mcpkit-data']['env']['_RELOAD_TIMESTAMP'] = str(int(time.time()))
    
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print("✓ Config file modified")
except Exception as e:
    print(f"✗ Error: {e}")
    exit(1)
PYTHON

if [ $? -eq 0 ]; then
    echo "✓ MCP server should reload automatically"
    echo "If not, restart Cursor manually."
else
    echo "✗ Failed to modify config file"
    # Restore backup
    mv "${CONFIG_FILE}.bak" "$CONFIG_FILE" 2>/dev/null
fi

