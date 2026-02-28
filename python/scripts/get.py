#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Memory get script - retrieve content from memU database or workspace files."""

import asyncio
import argparse
import os
import sys
import io
import json
import re

# Force UTF-8 encoding for Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from memu.app.service import MemoryService
from memu.app.settings import DatabaseConfig, LLMConfig, MetadataStoreConfig


def get_db_dsn() -> str:
    data_dir = os.getenv("MEMU_DATA_DIR")
    if not data_dir:
        base = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        data_dir = os.path.join(base, "data")
    os.makedirs(data_dir, exist_ok=True)
    return f"sqlite:///{os.path.join(data_dir, 'memu.db')}"


def get_db_path() -> str:
    """Get database path for direct SQLite access."""
    data_dir = os.getenv("MEMU_DATA_DIR")
    if not data_dir:
        data_dir = os.path.expanduser("~/.openclaw/memUdata")
    return os.path.join(data_dir, "memu.db")


def _expand_short_path(short: str) -> str | None:
    """Expand short path notation (ws:, ext:, conv:) to full path."""
    data_dir = os.getenv("MEMU_DATA_DIR", "")
    workspace_dir = os.getenv(
        "MEMU_WORKSPACE_DIR", os.path.expanduser("~/.openclaw/workspace")
    )
    extra_paths_json = os.getenv("MEMU_EXTRA_PATHS", "[]")
    try:
        extra_paths: list[str] = (
            json.loads(extra_paths_json) if extra_paths_json else []
        )
    except Exception:
        extra_paths = []

    # Handle Windows paths
    short = short.replace("/", os.sep).replace("\\", os.sep)

    if short.startswith("ws:"):
        rel = short[3:]
        rel = rel.replace("/", os.sep).replace("\\", os.sep)
        return os.path.join(workspace_dir, rel) if rel else workspace_dir

    m = re.match(r"^ext(\d+):(.*)$", short)
    if m:
        idx, rel = int(m.group(1)), m.group(2)
        rel = rel.replace("/", os.sep).replace("\\", os.sep)
        if 0 <= idx < len(extra_paths):
            return os.path.join(extra_paths[idx], rel) if rel else extra_paths[idx]

    m = re.match(r"^conv:([a-f0-9-]+):p(\d+)$", short)
    if m:
        prefix, part = m.group(1), int(m.group(2))
        conv_dir = os.path.join(data_dir, "conversations") if data_dir else ""
        if conv_dir and os.path.isdir(conv_dir):
            for f in os.listdir(conv_dir):
                if f.startswith(prefix) and f.endswith(f".part{part:03d}.json"):
                    return os.path.join(conv_dir, f)

    m = re.match(r"^conv:([a-f0-9-]+)$", short)
    if m:
        prefix = m.group(1)
        conv_dir = os.path.join(data_dir, "conversations") if data_dir else ""
        if conv_dir and os.path.isdir(conv_dir):
            for f in os.listdir(conv_dir):
                if f.startswith(prefix) and f.endswith(".json") and ".part" not in f:
                    return os.path.join(conv_dir, f)

    return None


def _convert_json_conversation_to_markdown(data: dict) -> str:
    """Convert memU JSON conversation format to Virtual Markdown."""
    lines = []
    messages = data.get("messages", [])
    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        lines.append(f"### {role.capitalize()}\n\n{content}\n")
    return "\n".join(lines)


def _get_resource_content_sqlite(path_or_id: str) -> str | None:
    """Get resource content directly from SQLite database."""
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return None
    
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    try:
        # Handle category paths
        if path_or_id.startswith("category/"):
            cat_id = path_or_id.split("/", 1)[1]
            cur.execute("""
                SELECT id, name, description, summary
                FROM memu_memory_categories
                WHERE id = ? OR name = ?
            """, (cat_id, cat_id))
            row = cur.fetchone()
            if row:
                return (
                    f"# Category\n\n"
                    f"- id: {row['id']}\n"
                    f"- name: {row['name']}\n"
                    f"- description: {row['description'] or ''}\n\n"
                    f"## Summary\n\n{row['summary'] or ''}\n"
                )
        
        # Handle item paths
        elif path_or_id.startswith("item/"):
            item_id = path_or_id.split("/", 1)[1]
            cur.execute("""
                SELECT id, memory_type, summary, created_at, updated_at
                FROM memu_memory_items
                WHERE id = ?
            """, (item_id,))
            row = cur.fetchone()
            if row:
                return (
                    f"# Memory Item\n\n"
                    f"- id: {row['id']}\n"
                    f"- memory_type: {row['memory_type']}\n"
                    f"- created_at: {row['created_at']}\n"
                    f"- updated_at: {row['updated_at']}\n\n"
                    f"## Summary\n\n{row['summary'] or ''}\n"
                )
        
        # Handle resource paths
        elif path_or_id.startswith("resource/"):
            res_id = path_or_id.split("/", 1)[1]
            cur.execute("""
                SELECT id, url, local_path, caption, modality
                FROM memu_resources
                WHERE id = ?
            """, (res_id,))
            row = cur.fetchone()
            if row:
                content = f"# Resource\n\n- id: {row['id']}\n- url: {row['url']}\n- modality: {row['modality'] or 'unknown'}\n"
                if row['local_path']:
                    local_path = row['local_path']
                    if os.path.exists(local_path):
                        with open(local_path, 'r', encoding='utf-8') as f:
                            return f.read()
                return content + f"\n## Caption\n\n{row['caption'] or ''}\n"
    
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()
    
    return None


async def get_resource_content(path_or_id: str) -> str:
    """Get resource content from memU SDK or fallback to SQLite."""
    is_memu_uri = path_or_id.startswith("memu://")
    if is_memu_uri:
        path_or_id = path_or_id.replace("memu://", "", 1)

    # Expand short path notation
    if path_or_id.startswith(("conv:", "ws:", "ext")):
        expanded = _expand_short_path(path_or_id)
        if expanded:
            path_or_id = expanded

    user_id = os.getenv("MEMU_USER_ID") or "default"

    # Try memU SDK first
    try:
        dummy_llm = LLMConfig(
            provider="openai",
            base_url="http://localhost",
            api_key="none",
            chat_model="none",
        )
        db_config = DatabaseConfig(
            metadata_store=MetadataStoreConfig(provider="sqlite", dsn=get_db_dsn())
        )
        service = MemoryService(
            llm_profiles={"default": dummy_llm, "embedding": dummy_llm},
            database_config=db_config,
        )

        if path_or_id.startswith("category/"):
            category_key = path_or_id.split("/", 1)[1]
            categories = service.database.memory_category_repo.list_categories(
                where={"user_id": user_id}
            )
            target = categories.get(category_key)
            if target is None:
                for cat in categories.values():
                    if cat.name == category_key:
                        target = cat
                        break
            if target is not None:
                return (
                    f"# Category\n\n"
                    f"- id: {target.id}\n"
                    f"- name: {target.name}\n"
                    f"- description: {target.description or ''}\n\n"
                    f"## Summary\n\n{target.summary or ''}\n"
                )

        if path_or_id.startswith("item/"):
            item_key = path_or_id.split("/", 1)[1]
            item = service.database.memory_item_repo.get_item(item_key)
            if item is not None:
                return (
                    f"# Memory Item\n\n"
                    f"- id: {item.id}\n"
                    f"- memory_type: {item.memory_type}\n"
                    f"- resource_id: {item.resource_id}\n"
                    f"- created_at: {item.created_at}\n"
                    f"- updated_at: {item.updated_at}\n\n"
                    f"## Summary\n\n{item.summary or ''}\n"
                )

        if path_or_id.startswith("resource/"):
            resource_key = path_or_id.split("/", 1)[1]
            resources = service.database.resource_repo.list_resources(
                where={"user_id": user_id}
            )
            target = resources.get(resource_key)
            if target is not None:
                return (
                    f"# Resource\n\n"
                    f"- id: {target.id}\n"
                    f"- url: {target.url}\n"
                    f"- modality: {target.modality}\n"
                    f"- local_path: {target.local_path}\n\n"
                    f"## Caption\n\n{target.caption or ''}\n"
                )

        # Try to find by URL
        resources = service.database.resource_repo.list_resources(
            where={"user_id": user_id}
        )
        target = None
        for res in resources.values():
            if res.url == path_or_id or res.id == path_or_id:
                target = res
                break

        if target:
            if target.local_path:
                local_path = target.local_path
                candidates: list[str] = []
                if os.path.isabs(local_path):
                    candidates.append(local_path)
                else:
                    data_dir = os.getenv("MEMU_DATA_DIR")
                    if data_dir:
                        candidates.append(os.path.join(data_dir, local_path))
                    candidates.append(local_path)

                for p in candidates:
                    if p and os.path.exists(p):
                        if p.endswith(".json"):
                            # Convert memU JSON conversation to Virtual Markdown
                            try:
                                with open(p, "r", encoding="utf-8") as f:
                                    data = json.load(f)
                                    return _convert_json_conversation_to_markdown(data)
                            except Exception:
                                pass
                        with open(p, "r", encoding="utf-8") as f:
                            return f.read()
    except Exception as e:
        # Fall back to SQLite direct access
        pass

    # Fallback to SQLite direct access
    sqlite_result = _get_resource_content_sqlite(path_or_id)
    if sqlite_result:
        return sqlite_result

    # Try as file path
    if os.path.exists(path_or_id):
        with open(path_or_id, "r", encoding="utf-8") as f:
            return f.read()

    raise FileNotFoundError(f"Resource not found: {path_or_id}")


def _resolve_file_path(path_str: str) -> str:
    """Resolve and validate file path within workspace."""
    workspace_dir = os.getenv("MEMU_WORKSPACE_DIR")
    if not workspace_dir:
        workspace_dir = os.path.expanduser("~/.openclaw/workspace")

    workspace_real = os.path.realpath(workspace_dir)

    if os.path.isabs(path_str):
        candidate = os.path.realpath(path_str)
    else:
        candidate = os.path.realpath(
            os.path.normpath(os.path.join(workspace_dir, path_str))
        )

    if os.path.commonpath([workspace_real, candidate]) != workspace_real:
        data_dir = os.getenv("MEMU_DATA_DIR")
        if data_dir:
            data_real = os.path.realpath(data_dir)
            if os.path.commonpath([data_real, candidate]) == data_real:
                return candidate

        raise ValueError(f"Path escapes workspace: {path_str}")
    return candidate


def _slice_lines(text: str, from_line: int, lines_count: int | None) -> str:
    """Slice text by lines (1-indexed from_line)."""
    all_lines = text.splitlines(keepends=True)
    if from_line < 0:
        from_line = 0
    else:
        from_line = max(0, from_line - 1)  # Convert to 0-based

    end = None
    if lines_count is not None:
        end = from_line + lines_count

    return "".join(all_lines[from_line:end])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to file or memu:// resource")
    parser.add_argument("--from", dest="from_line", type=int, default=1)
    parser.add_argument("--lines", type=int, default=None)
    parser.add_argument("--offset", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    from_val = max(1, args.from_line)
    if args.offset is not None:
        # --offset is 0-based, pass directly (add 1 to make it 1-based for _slice_lines)
        from_val = max(0, args.offset) + 1
    # from_val is now 1-based

    lines_val = args.lines
    if args.limit is not None:
        lines_val = args.limit

    if lines_val is not None and lines_val < 0:
        lines_val = 0

    try:
        content = asyncio.run(get_resource_content(args.path))
        sliced = _slice_lines(content, from_val, lines_val)
        print(json.dumps({"path": args.path, "text": sliced}, ensure_ascii=False))
    except Exception as e:
        print(
            json.dumps(
                {"path": args.path, "text": "", "error": str(e)}, ensure_ascii=False
            )
        )