"""Repository/file system tools for MCP server."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import repo_ops
from mcpkit.server_models import DirectoryListResponse, FileInfo, FileReadResponse, SearchMatch, SearchResponse
from mcpkit.server_utils import _to_int


def register_repo_tools(mcp: FastMCP):
    """Register repository/file system tools with MCP server."""
    
    @mcp.tool()
    def fs_read(
        path: Annotated[str, Field(description="File path (must be within allowed roots)")],
        offset: Annotated[int, Field(description="Byte offset to start reading from. Default: 0")] = 0,
    ) -> FileReadResponse:
        """Read file content (with offset support)."""
        offset = _to_int(offset, 0)
        result = repo_ops.fs_read(path, offset)
        return FileReadResponse(**result)

    @mcp.tool()
    def fs_list_dir(
        path: Annotated[str, Field(description="Directory path (must be within allowed roots)")],
    ) -> DirectoryListResponse:
        """List directory contents."""
        result = repo_ops.fs_list_dir(path)
        # Ensure size and mtime are set for files, None for directories
        files = [FileInfo(
            name=f["name"],
            path=f["path"],
            size=f.get("size"),
            mtime=f.get("mtime")
        ) for f in result["files"]]
        directories = [FileInfo(
            name=d["name"],
            path=d["path"],
            size=None,
            mtime=None
        ) for d in result["directories"]]
        return DirectoryListResponse(
            path=result["path"],
            files=files,
            directories=directories,
            file_count=result["file_count"],
            directory_count=result["directory_count"],
        )

    @mcp.tool()
    def rg_search(
        root: Annotated[str, Field(description="Root directory to search (must be within allowed roots)")],
        pattern: Annotated[str, Field(description="Regex pattern to search for")],
    ) -> SearchResponse:
        """Search using ripgrep (rg)."""
        result = repo_ops.rg_search(root, pattern)
        matches = [SearchMatch(**m) for m in result["matches"]]
        return SearchResponse(
            root=result["root"],
            pattern=result["pattern"],
            matches=matches,
            match_count=result["match_count"]
        )

