"""Repository operations (file read, search)."""

from .guards import GuardError, cap_bytes, check_path_allowed, get_max_output_bytes
from .proc import run_cmd


def fs_read(path: str, offset: int = 0) -> dict:
    """
    Read file content (with offset support).
    Returns dict with content and metadata.
    """
    resolved_path = check_path_allowed(path)
    
    if not resolved_path.exists():
        raise GuardError(f"File not found: {path}")
    
    if not resolved_path.is_file():
        raise GuardError(f"Path is not a file: {path}")
    
    try:
        with open(resolved_path, "rb") as f:
            if offset > 0:
                f.seek(offset)
            content = f.read()
        
        # Cap bytes
        max_bytes = get_max_output_bytes()
        if len(content) > max_bytes:
            content = content[:max_bytes]
            truncated = True
        else:
            truncated = False
        
        return {
            "path": str(resolved_path),
            "content": content.decode("utf-8", errors="replace"),
            "size": len(content),
            "truncated": truncated,
        }
    except Exception as e:
        raise GuardError(f"File read failed: {e}")


def rg_search(root: str, pattern: str) -> dict:
    """
    Search using ripgrep (rg).
    Returns dict with matches.
    """
    resolved_root = check_path_allowed(root)
    
    if not resolved_root.exists():
        raise GuardError(f"Root path not found: {root}")
    
    # Run rg --json
    cmd = ["rg", "--json", pattern, str(resolved_root)]
    
    result = run_cmd(cmd, max_output_bytes=get_max_output_bytes())
    
    if result["returncode"] != 0 and result["returncode"] != 1:  # 1 = no matches
        raise GuardError(f"rg search failed: {result['stderr']}")
    
    # Parse JSON lines
    matches = []
    for line in result["stdout"].splitlines():
        if line.strip():
            try:
                import json
                match_data = json.loads(line)
                if match_data.get("type") == "match":
                    matches.append({
                        "path": match_data.get("data", {}).get("path", {}).get("text", ""),
                        "line_number": match_data.get("data", {}).get("line_number", 0),
                        "text": match_data.get("data", {}).get("lines", {}).get("text", "").strip(),
                    })
            except Exception:
                continue
    
    return {
        "root": str(resolved_root),
        "pattern": pattern,
        "matches": matches,
        "match_count": len(matches),
    }


def fs_list_dir(path: str) -> dict:
    """
    List directory contents.
    
    Args:
        path: Directory path (must be within allowed roots)
    
    Returns:
        dict with files and directories
    """
    resolved_path = check_path_allowed(path)
    
    if not resolved_path.exists():
        raise GuardError(f"Path not found: {path}")
    
    if not resolved_path.is_dir():
        raise GuardError(f"Path is not a directory: {path}")
    
    try:
        files = []
        directories = []
        
        for item in sorted(resolved_path.iterdir()):
            # Skip hidden files/dirs
            if item.name.startswith("."):
                continue
            
            item_info = {
                "name": item.name,
                "path": str(item),
            }
            
            if item.is_file():
                try:
                    stat = item.stat()
                    item_info["size"] = stat.st_size
                    item_info["mtime"] = stat.st_mtime
                except Exception:
                    pass
                files.append(item_info)
            elif item.is_dir():
                directories.append(item_info)
        
        return {
            "path": str(resolved_path),
            "files": files,
            "directories": directories,
            "file_count": len(files),
            "directory_count": len(directories),
        }
    except Exception as e:
        raise GuardError(f"Directory listing failed: {e}")

