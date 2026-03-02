#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""memU Dashboard - Web management UI for the memU memory database.

Features:
- Overview statistics
- Memory items browser with pagination, type filter, and inline delete
- Resource list with cascade delete
- Resource detail with associated memory items
- Keyword search with direct delete from results
- Category browser
- File tracking state viewer
"""

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = os.environ.get(
    "MEMU_DATA_DIR", os.path.expanduser("~/.openclaw/memUdata")
)
DB_PATH = os.path.join(DATA_DIR, "memu.db")
STATE_FILE = os.path.join(DATA_DIR, "docs_ingest_state.json")
PORT = int(os.environ.get("MEMU_DASHBOARD_PORT", "8377"))
# PID of the parent OpenClaw Node.js process (set by index.ts at spawn time).
PARENT_PID = int(os.getenv("MEMU_PARENT_PID", "0") or "0")

app = FastAPI(title="memU Dashboard", docs_url=None, redoc_url=None)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def query_all(sql: str, params: tuple = ()) -> list[dict]:
    with get_db() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict | None:
    with get_db() as conn:
        r = conn.execute(sql, params).fetchone()
        return dict(r) if r else None


def execute(sql: str, params: tuple = ()) -> int:
    with get_db() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

PAGE_SIZE = 50

BADGE_COLORS = {
    "profile": ("1f3a5f", "58a6ff"),
    "event": ("3d2c1f", "d29922"),
    "knowledge": ("1f3d2c", "3fb950"),
    "behavior": ("3d1f3a", "bc8cff"),
    "skill": ("1f3d3d", "39d2c0"),
    "tool": ("3d3d1f", "d2c839"),
}


def _badge(mt: str) -> str:
    bg, fg = BADGE_COLORS.get(mt, ("30363d", "c9d1d9"))
    return f'<span class="badge" style="background:#{bg};color:#{fg};">{_esc(mt)}</span>'


def _esc(text) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _short(text, maxlen: int = 80) -> str:
    if not text:
        return ""
    t = str(text).replace("\n", " ").strip()
    return t[:maxlen] + "..." if len(t) > maxlen else t


def _pagination(page: int, total: int, base_url: str) -> str:
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages <= 1:
        return ""
    parts = []
    if page > 1:
        parts.append(f'<a href="{base_url}page={page - 1}">&laquo; 上一页</a>')
    start = max(1, min(page - 3, total_pages - 6))
    end = min(total_pages, start + 6)
    for p in range(start, end + 1):
        if p == page:
            parts.append(f'<span class="current">{p}</span>')
        else:
            parts.append(f'<a href="{base_url}page={p}">{p}</a>')
    if page < total_pages:
        parts.append(f'<a href="{base_url}page={page + 1}">下一页 &raquo;</a>')
    parts.append(f'<span class="page-info">(共 {total} 条)</span>')
    return '<div class="pagination">' + "".join(parts) + "</div>"


# ---------------------------------------------------------------------------
# HTML shell
# ---------------------------------------------------------------------------

CSS = """
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,'Segoe UI','Noto Sans SC',sans-serif; background:#0d1117; color:#c9d1d9; display:flex; min-height:100vh; }

.sidebar { width:220px; background:#161b22; border-right:1px solid #30363d; padding:20px 0; flex-shrink:0; position:fixed; height:100vh; overflow-y:auto; }
.sidebar h1 { font-size:18px; color:#58a6ff; padding:0 20px 20px; border-bottom:1px solid #30363d; margin-bottom:8px; }
.sidebar a { display:block; padding:10px 20px; color:#8b949e; text-decoration:none; font-size:14px; transition:all .15s; }
.sidebar a:hover { color:#c9d1d9; background:#1c2128; }
.sidebar a.active { color:#58a6ff; background:#1c2128; border-left:3px solid #58a6ff; padding-left:17px; }

.main { margin-left:220px; flex:1; padding:32px 40px; max-width:1200px; }
.main h2 { font-size:22px; color:#e6edf3; margin-bottom:20px; border-bottom:1px solid #30363d; padding-bottom:12px; }

.stats { display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:16px; margin-bottom:28px; }
.stat-card { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:20px; }
.stat-card .label { font-size:12px; color:#8b949e; text-transform:uppercase; letter-spacing:.5px; }
.stat-card .value { font-size:28px; font-weight:600; color:#58a6ff; margin-top:6px; }
.stat-card .sub { font-size:12px; color:#8b949e; margin-top:4px; }

table { width:100%; border-collapse:collapse; background:#161b22; border:1px solid #30363d; border-radius:8px; overflow:hidden; margin-bottom:20px; }
th { background:#1c2128; color:#8b949e; font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:.5px; padding:12px 16px; text-align:left; }
td { padding:10px 16px; border-top:1px solid #21262d; font-size:13px; }
tr:hover td { background:#1c2128; }
.mono { font-family:'Cascadia Code','Fira Code',monospace; font-size:12px; color:#7ee787; }

.badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:11px; font-weight:600; }

.btn { display:inline-block; padding:6px 14px; border-radius:6px; border:1px solid #30363d; background:#21262d; color:#c9d1d9; font-size:12px; cursor:pointer; text-decoration:none; transition:all .15s; }
.btn:hover { background:#30363d; border-color:#8b949e; }
.btn-danger { border-color:#f85149; color:#f85149; }
.btn-danger:hover { background:#da3633; color:#fff; border-color:#da3633; }
.btn-primary { border-color:#58a6ff; color:#58a6ff; }
.btn-primary:hover { background:#1f6feb; color:#fff; border-color:#1f6feb; }
.btn-sm { padding:3px 8px; font-size:11px; }

.search-bar { display:flex; gap:10px; margin-bottom:24px; flex-wrap:wrap; }
.search-bar input,.search-bar select { padding:8px 14px; border:1px solid #30363d; border-radius:6px; background:#0d1117; color:#c9d1d9; font-size:14px; }
.search-bar input { flex:1; min-width:200px; }
.search-bar select { min-width:120px; }
.search-bar button { padding:8px 20px; border:1px solid #58a6ff; border-radius:6px; background:#1f6feb; color:#fff; font-size:14px; cursor:pointer; }
.search-bar button:hover { background:#388bfd; }

.pagination { display:flex; gap:8px; justify-content:center; align-items:center; margin:20px 0; }
.pagination a,.pagination span { padding:6px 12px; border:1px solid #30363d; border-radius:6px; font-size:13px; text-decoration:none; color:#c9d1d9; }
.pagination a:hover { background:#30363d; }
.pagination .current { background:#1f6feb; border-color:#1f6feb; color:#fff; }
.page-info { border:none !important; color:#8b949e !important; font-size:12px !important; }

.detail-header { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:20px; margin-bottom:20px; }
.detail-header .title { font-size:16px; color:#e6edf3; margin-bottom:8px; word-break:break-all; }
.detail-header .meta { font-size:12px; color:#8b949e; }
.detail-header .meta span { margin-right:16px; }
.detail-actions { display:flex; gap:10px; margin-top:12px; }

.truncate { max-width:400px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.wrap { white-space:pre-wrap; word-break:break-word; max-width:600px; }

.alert { padding:12px 16px; border-radius:6px; margin-bottom:16px; font-size:13px; }
.alert-success { background:#1f3d2c; border:1px solid #238636; color:#3fb950; }
.alert-danger { background:#3d1f1f; border:1px solid #f85149; color:#f85149; }
.alert-info { background:#1f3a5f; border:1px solid #388bfd; color:#58a6ff; }

.empty { text-align:center; padding:60px 20px; color:#8b949e; }
.empty .icon { font-size:48px; margin-bottom:16px; }

.modal-overlay { display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,.6); z-index:1000; justify-content:center; align-items:center; }
.modal-overlay.show { display:flex; }
.modal { background:#161b22; border:1px solid #30363d; border-radius:12px; padding:24px; max-width:480px; width:90%; }
.modal h3 { color:#f85149; margin-bottom:12px; font-size:16px; }
.modal p { color:#8b949e; margin-bottom:20px; font-size:13px; }
.modal .actions { display:flex; gap:10px; justify-content:flex-end; }

pre { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px; overflow-x:auto; font-family:'Cascadia Code',monospace; font-size:12px; color:#c9d1d9; }
mark { background:#d29922; color:#0d1117; padding:0 2px; border-radius:2px; }
"""

JS = r"""
let pendingDeleteUrl = null;

function confirmDelete(url, desc) {
  pendingDeleteUrl = url;
  document.getElementById('modalDesc').textContent = desc;
  document.getElementById('confirmModal').classList.add('show');
}

function closeModal() {
  document.getElementById('confirmModal').classList.remove('show');
  pendingDeleteUrl = null;
}

document.getElementById('modalConfirm').addEventListener('click', async () => {
  if (!pendingDeleteUrl) return;
  const url = pendingDeleteUrl;
  closeModal();
  try {
    const resp = await fetch(url, { method: 'DELETE' });
    const data = await resp.json();
    if (data.ok) {
      showAlert('success', data.message || '删除成功');
      setTimeout(() => location.reload(), 600);
    } else {
      showAlert('danger', data.message || '删除失败');
    }
  } catch(e) {
    showAlert('danger', '请求失败：' + e.message);
  }
});

function showAlert(type, msg) {
  const div = document.createElement('div');
  div.className = 'alert alert-' + type;
  div.textContent = msg;
  document.querySelector('.main h2').after(div);
  setTimeout(() => div.remove(), 4000);
}

async function purgeAll() {
  if (!confirm('!!! 危险操作 !!!\n\n确认清除全部记忆数据？\n\n此操作将删除所有资源、记忆条目、分类及追踪状态，不可撤销。\n清除后将自动在后台重新导入全部文档。')) return;
  if (!confirm('二次确认：真的要清除所有记忆数据吗？')) return;
  try {
    const resp = await fetch('/api/purge-all', { method: 'DELETE' });
    const data = await resp.json();
    if (data.ok) {
      showAlert('success', data.message || '已清除全部数据，正在后台重新导入...');
      setTimeout(() => location.reload(), 2000);
    } else {
      showAlert('danger', data.message || '清除失败');
    }
  } catch(e) {
    showAlert('danger', '请求失败：' + e.message);
  }
}

async function reingestDocs() {
  if (!confirm('确认触发全量文档重新导入？\\n\\n此操作会在后台重新扫描并导入所有文档，已存在且未变化的文档会被跳过。')) return;
  try {
    const resp = await fetch('/api/reingest', { method: 'POST' });
    const data = await resp.json();
    if (data.ok) {
      showAlert('success', data.message || '已触发后台重新导入');
      setTimeout(() => location.reload(), 2000);
    } else {
      showAlert('danger', data.message || '触发失败');
    }
  } catch(e) {
    showAlert('danger', '请求失败：' + e.message);
  }
}

async function quickDelete(url, rowId) {
  if (!confirm('确认删除此记忆条目？')) return;
  try {
    const resp = await fetch(url, { method: 'DELETE' });
    const data = await resp.json();
    if (data.ok) {
      const row = document.getElementById(rowId);
      if (row) { row.style.transition='opacity .3s'; row.style.opacity='0'; setTimeout(() => row.remove(), 300); }
      showAlert('success', data.message || '已删除');
    } else {
      showAlert('danger', data.message || '删除失败');
    }
  } catch(e) {
    showAlert('danger', '请求失败：' + e.message);
  }
}
"""

NAV_ITEMS = [
    ("overview",   "/",           "概览"),
    ("memories",   "/memories",   "记忆"),
    ("resources",  "/resources",  "资源"),
    ("categories", "/categories", "分类"),
    ("search",     "/search",     "搜索"),
    ("tracking",   "/tracking",   "文件追踪"),
]


def _page(title: str, body: str, active: str = "") -> str:
    nav = ""
    for key, href, label in NAV_ITEMS:
        cls = ' class="active"' if key == active else ""
        nav += f'<a href="{href}"{cls}>{label}</a>\n'
    return (
        '<!DOCTYPE html><html lang="zh-CN"><head>'
        '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<title>{_esc(title)} - memU 控制台</title>'
        f'<style>{CSS}</style></head><body>'
        f'<nav class="sidebar"><h1>memU 控制台</h1>{nav}</nav>'
        f'<div class="main"><h2>{_esc(title)}</h2>{body}</div>'
        '<div class="modal-overlay" id="confirmModal"><div class="modal">'
        '<h3>确认删除</h3><p id="modalDesc"></p>'
        '<div class="actions"><button class="btn" onclick="closeModal()">取消</button>'
        '<button class="btn btn-danger" id="modalConfirm">删除</button></div>'
        '</div></div>'
        f'<script>{JS}</script></body></html>'
    )


# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------

def _stat_card(label: str, value, sub: str = "") -> str:
    h = f'<div class="stat-card"><div class="label">{_esc(label)}</div><div class="value">{value}</div>'
    if sub:
        h += f'<div class="sub">{sub}</div>'
    return h + "</div>"


def _table(headers: list[str], rows_html: str, col_widths: list | None = None) -> str:
    ths = ""
    for i, h in enumerate(headers):
        style = ""
        if col_widths and i < len(col_widths) and col_widths[i]:
            style = f' style="width:{col_widths[i]}"'
        ths += f"<th{style}>{h}</th>"
    if not rows_html:
        cols = len(headers)
        rows_html = f'<tr><td colspan="{cols}" style="text-align:center;color:#8b949e;padding:40px;">暂无数据</td></tr>'
    return f"<table><tr>{ths}</tr>{rows_html}</table>"


# ---------------------------------------------------------------------------
# API routes - Delete operations
# ---------------------------------------------------------------------------

@app.delete("/api/memory/{item_id}")
async def api_delete_memory(item_id: str):
    item = query_one("SELECT id FROM memu_memory_items WHERE id = ?", (item_id,))
    if not item:
        raise HTTPException(404, "记忆条目不存在")
    cat_count = execute("DELETE FROM memu_category_items WHERE item_id = ?", (item_id,))
    execute("DELETE FROM memu_memory_items WHERE id = ?", (item_id,))
    return {"ok": True, "message": f"已删除记忆条目及 {cat_count} 个分类关联"}


@app.delete("/api/resource/{resource_id}")
async def api_delete_resource(resource_id: str):
    res = query_one("SELECT id, url FROM memu_resources WHERE id = ?", (resource_id,))
    if not res:
        raise HTTPException(404, "资源不存在")
    items = query_all("SELECT id FROM memu_memory_items WHERE resource_id = ?", (resource_id,))
    cat_count = 0
    for i in items:
        cat_count += execute("DELETE FROM memu_category_items WHERE item_id = ?", (i["id"],))
    mem_count = execute("DELETE FROM memu_memory_items WHERE resource_id = ?", (resource_id,))
    execute("DELETE FROM memu_resources WHERE id = ?", (resource_id,))
    _remove_tracking(res.get("url", ""))
    return {"ok": True, "message": f"已删除资源及 {mem_count} 个记忆条目、{cat_count} 个分类关联"}


def _spawn_docs_reingest():
    """Spawn docs_ingest.py as a background subprocess for full re-scan."""
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    docs_script = os.path.join(scripts_dir, "docs_ingest.py")
    try:
        subprocess.Popen(
            [sys.executable, docs_script],
            cwd=scripts_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


@app.delete("/api/purge-all")
async def api_purge_all():
    """Delete ALL memory items, category links, resources, categories, and tracking state."""
    with get_db() as conn:
        cat_link_count = conn.execute("SELECT COUNT(*) FROM memu_category_items").fetchone()[0]
        mem_count = conn.execute("SELECT COUNT(*) FROM memu_memory_items").fetchone()[0]
        res_count = conn.execute("SELECT COUNT(*) FROM memu_resources").fetchone()[0]
        cat_count = conn.execute("SELECT COUNT(*) FROM memu_memory_categories").fetchone()[0]
        conn.execute("DELETE FROM memu_category_items")
        conn.execute("DELETE FROM memu_memory_items")
        conn.execute("DELETE FROM memu_resources")
        conn.execute("DELETE FROM memu_memory_categories")
        conn.commit()
    # Clear tracking state file
    if os.path.isfile(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except Exception:
            pass
    # Clear full-scan marker so watcher will re-trigger full scan on next start
    full_scan_marker = os.path.join(DATA_DIR, "docs_full_scan.marker")
    if os.path.isfile(full_scan_marker):
        try:
            os.remove(full_scan_marker)
        except Exception:
            pass
    # Clear rate-limit backoff state so ingestion is not delayed
    backoff_file = os.path.join(DATA_DIR, "pending_backoff.json")
    if os.path.isfile(backoff_file):
        try:
            os.remove(backoff_file)
        except Exception:
            pass
    # Immediately trigger re-ingestion in background
    _spawn_docs_reingest()
    return {
        "ok": True,
        "message": f"已清除全部数据：{res_count} 个资源、{mem_count} 个记忆条目、{cat_count} 个分类、{cat_link_count} 个关联。\n正在后台重新导入全部文档，请稍后刷新页面查看进度。"
    }


@app.post("/api/reingest")
async def api_reingest():
    """Manually trigger a full document re-ingestion."""
    # Remove the full-scan marker so ingest runs a complete scan
    full_scan_marker = os.path.join(DATA_DIR, "docs_full_scan.marker")
    if os.path.isfile(full_scan_marker):
        try:
            os.remove(full_scan_marker)
        except Exception:
            pass
    # Clear rate-limit backoff
    backoff_file = os.path.join(DATA_DIR, "pending_backoff.json")
    if os.path.isfile(backoff_file):
        try:
            os.remove(backoff_file)
        except Exception:
            pass
    _spawn_docs_reingest()
    return {"ok": True, "message": "已触发后台全量文档重新导入，请稍后刷新查看进度。"}


def _remove_tracking(url: str):
    if not url or not os.path.isfile(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        changed = False
        for key in list(state.get("files", {}).keys()):
            if key == url or os.path.normpath(key) == os.path.normpath(url):
                del state["files"][key]
                changed = True
        if changed:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def page_overview():
    res_count = query_one("SELECT COUNT(*) as cnt FROM memu_resources")["cnt"]
    mem_count = query_one("SELECT COUNT(*) as cnt FROM memu_memory_items")["cnt"]
    cat_count = query_one("SELECT COUNT(*) as cnt FROM memu_memory_categories")["cnt"]
    link_count = query_one("SELECT COUNT(*) as cnt FROM memu_category_items")["cnt"]

    type_rows = query_all(
        "SELECT memory_type, COUNT(*) as cnt FROM memu_memory_items GROUP BY memory_type ORDER BY cnt DESC"
    )
    mod_rows = query_all(
        "SELECT modality, COUNT(*) as cnt FROM memu_resources GROUP BY modality ORDER BY cnt DESC"
    )
    recent = query_all(
        "SELECT i.id, i.memory_type, i.summary, i.created_at, r.url "
        "FROM memu_memory_items i LEFT JOIN memu_resources r ON i.resource_id = r.id "
        "ORDER BY i.created_at DESC LIMIT 10"
    )

    mod_sub = " / ".join(f'{r["modality"]}: {r["cnt"]}' for r in mod_rows)

    body = (
        '<div style="display:flex;justify-content:flex-end;gap:10px;margin-bottom:16px;">'
        '<button class="btn" onclick="reingestDocs()" style="font-size:13px;padding:8px 18px;background:#238636;color:#fff;border:none;border-radius:6px;cursor:pointer;">'
        '↻ 重新导入全部文档</button>'
        '<button class="btn btn-danger" onclick="purgeAll()" style="font-size:13px;padding:8px 18px;">'
        '\u26a0 一键清除全部数据</button></div>'
    )

    body += '<div class="stats">'
    body += _stat_card("资源", res_count, mod_sub)
    body += _stat_card("记忆条目", mem_count)
    body += _stat_card("分类", cat_count, f"{link_count} 个关联")
    body += "</div>"

    # Type breakdown cards
    body += '<h3 style="color:#e6edf3;margin-bottom:12px;">记忆类型</h3><div class="stats">'
    for r in type_rows:
        body += _stat_card(r["memory_type"], r["cnt"])
    body += "</div>"

    # Recent items table
    body += '<h3 style="color:#e6edf3;margin-bottom:12px;">最近记忆</h3>'
    rows_html = ""
    for r in recent:
        s = _esc(_short(r["summary"]))
        sf = _esc(r["summary"] or "")
        src = _esc(_short(r["url"] or "", 40))
        ts = (r["created_at"] or "")[:16]
        rows_html += (
            f'<tr><td>{_badge(r["memory_type"])}</td>'
            f'<td class="truncate" title="{sf}">{s}</td>'
            f'<td class="mono" style="font-size:11px;">{src}</td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td></tr>'
        )
    body += _table(["类型", "摘要", "来源", "创建时间"], rows_html)

    return _page("概览", body, "overview")


# ---------------------------------------------------------------------------
# Page: Memories (browse all with pagination)
# ---------------------------------------------------------------------------

@app.get("/memories", response_class=HTMLResponse)
async def page_memories(
    page: int = Query(1, ge=1),
    type: str = Query("", alias="type"),
    sort: str = Query("newest"),
):
    where = ""
    params: list = []
    if type:
        where = "WHERE i.memory_type = ?"
        params.append(type)

    order_map = {"oldest": "i.created_at ASC", "updated": "i.updated_at DESC"}
    order = order_map.get(sort, "i.created_at DESC")

    total = query_one(f"SELECT COUNT(*) as cnt FROM memu_memory_items i {where}", tuple(params))["cnt"]
    offset = (page - 1) * PAGE_SIZE
    rows = query_all(
        f"SELECT i.id, i.memory_type, i.summary, i.resource_id, i.created_at, "
        f"r.url as resource_url "
        f"FROM memu_memory_items i LEFT JOIN memu_resources r ON i.resource_id = r.id "
        f"{where} ORDER BY {order} LIMIT ? OFFSET ?",
        tuple(params + [PAGE_SIZE, offset])
    )

    types = query_all("SELECT DISTINCT memory_type FROM memu_memory_items ORDER BY memory_type")
    type_opts = '<option value="">All types</option>'
    for t in types:
        sel = " selected" if t["memory_type"] == type else ""
        type_opts += f'<option value="{t["memory_type"]}"{sel}>{t["memory_type"]}</option>'

    sort_opts = ""
    for v, l in [("newest", "最新优先"), ("oldest", "最早优先"), ("updated", "最近更新")]:
        sel = " selected" if v == sort else ""
        sort_opts += f'<option value="{v}"{sel}>{l}</option>'

    body = (
        '<div class="search-bar"><form method="GET" action="/memories" style="display:flex;gap:10px;width:100%;">'
        f'<select name="type">{type_opts}</select>'
        f'<select name="sort">{sort_opts}</select>'
        '<button type="submit">筛选</button></form></div>'
    )

    base_url = f"/memories?type={_esc(type)}&sort={_esc(sort)}&"
    body += _pagination(page, total, base_url)

    rows_html = ""
    for r in rows:
        s = _esc(_short(r["summary"], 90))
        sf = _esc(r["summary"] or "")
        src = _esc(_short(r["resource_url"] or "", 35))
        rid = r["resource_id"] or ""
        ts = (r["created_at"] or "")[:16]
        iid = r["id"]
        rows_html += (
            f'<tr id="row-{iid}">'
            f'<td>{_badge(r["memory_type"])}</td>'
            f'<td class="truncate" title="{sf}" style="max-width:500px;">{s}</td>'
            f'<td class="mono" style="font-size:11px;"><a href="/resources/{rid}" style="color:#7ee787;text-decoration:none;">{src}</a></td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td>'
            f"""<td><button class="btn btn-danger btn-sm" onclick="quickDelete('/api/memory/{iid}','row-{iid}')">删除</button></td>"""
            f'</tr>'
        )

    body += _table(["类型", "摘要", "来源", "创建时间", "操作"], rows_html, [None, None, None, None, "70px"])
    body += _pagination(page, total, base_url)

    return _page(f"记忆 ({total})", body, "memories")


# ---------------------------------------------------------------------------
# Page: Resources
# ---------------------------------------------------------------------------

@app.get("/resources", response_class=HTMLResponse)
async def page_resources():
    rows = query_all(
        "SELECT r.id, r.url, r.modality, r.created_at, "
        "(SELECT COUNT(*) FROM memu_memory_items WHERE resource_id = r.id) as item_count "
        "FROM memu_resources r ORDER BY r.created_at DESC"
    )
    rows_html = ""
    for r in rows:
        url_s = _esc(_short(r["url"] or "", 60))
        url_f = _esc(r["url"] or "")
        ic = r["item_count"]
        ts = (r["created_at"] or "")[:16]
        rid = r["id"]
        rows_html += (
            f'<tr><td><a href="/resources/{rid}" style="color:#58a6ff;text-decoration:none;" title="{url_f}">{url_s}</a></td>'
            f'<td>{_badge(r["modality"])}</td>'
            f'<td style="text-align:center;">{ic}</td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td>'
            f"""<td><button class="btn btn-danger btn-sm" onclick="confirmDelete('/api/resource/{rid}','删除此资源及其 {ic} 个记忆条目？')">级联删除</button></td></tr>"""
        )
    body = _table(["URL", "模态", "条目数", "创建时间", "操作"], rows_html, [None, None, "60px", None, "120px"])
    return _page(f"资源 ({len(rows)})", body, "resources")


# ---------------------------------------------------------------------------
# Page: Resource Detail
# ---------------------------------------------------------------------------

@app.get("/resources/{resource_id}", response_class=HTMLResponse)
async def page_resource_detail(resource_id: str, page: int = Query(1, ge=1)):
    res = query_one("SELECT * FROM memu_resources WHERE id = ?", (resource_id,))
    if not res:
        raise HTTPException(404, "资源不存在")

    total = query_one("SELECT COUNT(*) as cnt FROM memu_memory_items WHERE resource_id = ?", (resource_id,))["cnt"]
    offset = (page - 1) * PAGE_SIZE
    items = query_all(
        "SELECT id, memory_type, summary, created_at FROM memu_memory_items "
        "WHERE resource_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (resource_id, PAGE_SIZE, offset)
    )

    body = (
        '<div class="detail-header">'
        f'<div class="title">{_esc(res["url"] or "")}</div>'
        f'<div class="meta">'
        f'<span>ID: <code class="mono">{res["id"][:12]}...</code></span>'
        f'<span>模态: {_esc(res["modality"])}</span>'
        f'<span>创建时间: {(res["created_at"] or "")[:16]}</span>'
        f'<span>条目数: {total}</span></div>'
        f'<div class="detail-actions">'
        f'<a href="/resources" class="btn">&larr; 返回</a>'
        f"""<button class="btn btn-danger" onclick="confirmDelete('/api/resource/{resource_id}','删除此资源及其全部 {total} 个关联记忆条目？')">级联删除全部</button>"""
        f'</div></div>'
    )

    body += _pagination(page, total, f"/resources/{resource_id}?")

    rows_html = ""
    for r in items:
        sf = _esc(r["summary"] or "")
        ts = (r["created_at"] or "")[:16]
        iid = r["id"]
        rows_html += (
            f'<tr id="row-{iid}">'
            f'<td>{_badge(r["memory_type"])}</td>'
            f'<td class="wrap">{sf}</td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td>'
            f"""<td><button class="btn btn-danger btn-sm" onclick="quickDelete('/api/memory/{iid}','row-{iid}')">删除</button></td>"""
            f'</tr>'
        )

    body += _table(["类型", "摘要（完整）", "创建时间", "操作"], rows_html, [None, None, None, "70px"])
    body += _pagination(page, total, f"/resources/{resource_id}?")

    return _page("资源详情", body, "resources")


# ---------------------------------------------------------------------------
# Page: Categories
# ---------------------------------------------------------------------------

@app.get("/categories", response_class=HTMLResponse)
async def page_categories():
    rows = query_all(
        "SELECT c.id, c.name, c.description, c.summary, "
        "(SELECT COUNT(*) FROM memu_category_items WHERE category_id = c.id) as item_count "
        "FROM memu_memory_categories c ORDER BY c.name"
    )
    rows_html = ""
    for r in rows:
        desc = _esc(_short(r["description"] or r["summary"] or "", 80))
        rows_html += (
            f'<tr><td style="color:#e6edf3;font-weight:500;">{_esc(r["name"] or "")}</td>'
            f'<td class="truncate">{desc}</td>'
            f'<td style="text-align:center;">{r["item_count"]}</td></tr>'
        )
    body = _table(["名称", "描述", "条目数"], rows_html)
    return _page(f"分类 ({len(rows)})", body, "categories")


# ---------------------------------------------------------------------------
# Page: Search
# ---------------------------------------------------------------------------

@app.get("/search", response_class=HTMLResponse)
async def page_search(q: str = Query(""), type: str = Query("")):
    types = query_all("SELECT DISTINCT memory_type FROM memu_memory_items ORDER BY memory_type")
    type_opts = '<option value="">All types</option>'
    for t in types:
        sel = " selected" if t["memory_type"] == type else ""
        type_opts += f'<option value="{t["memory_type"]}"{sel}>{t["memory_type"]}</option>'

    body = (
        '<div class="search-bar"><form method="GET" action="/search" style="display:flex;gap:10px;width:100%;">'
        f'<input type="text" name="q" value="{_esc(q)}" placeholder="搜索记忆内容..." autofocus>'
        f'<select name="type">{type_opts}</select>'
        '<button type="submit">搜索</button></form></div>'
    )

    if not q:
        body += '<div class="empty"><div class="icon">[?]</div><p>输入关键词搜索所有记忆条目</p></div>'
        return _page("搜索", body, "search")

    where_parts = ["i.summary LIKE ?"]
    params: list = [f"%{q}%"]
    if type:
        where_parts.append("i.memory_type = ?")
        params.append(type)
    where = " AND ".join(where_parts)

    rows = query_all(
        f"SELECT i.id, i.memory_type, i.summary, i.resource_id, i.created_at, "
        f"r.url as resource_url "
        f"FROM memu_memory_items i LEFT JOIN memu_resources r ON i.resource_id = r.id "
        f"WHERE {where} ORDER BY i.updated_at DESC LIMIT 200",
        tuple(params)
    )

    body += f'<div class="alert alert-info">找到 {len(rows)} 条关于 "{_esc(q)}" 的结果</div>'

    q_esc = _esc(q)
    rows_html = ""
    for r in rows:
        raw = _esc(r["summary"] or "")
        highlighted = raw.replace(q_esc, f"<mark>{q_esc}</mark>")
        src = _esc(_short(r["resource_url"] or "", 35))
        rid = r["resource_id"] or ""
        ts = (r["created_at"] or "")[:16]
        iid = r["id"]
        rows_html += (
            f'<tr id="row-{iid}">'
            f'<td>{_badge(r["memory_type"])}</td>'
            f'<td class="wrap">{highlighted}</td>'
            f'<td class="mono" style="font-size:11px;"><a href="/resources/{rid}" style="color:#7ee787;text-decoration:none;">{src}</a></td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td>'
            f"""<td><button class="btn btn-danger btn-sm" onclick="quickDelete('/api/memory/{iid}','row-{iid}')">删除</button></td>"""
            f'</tr>'
        )

    body += _table(["类型", "摘要", "来源", "创建时间", "操作"], rows_html, [None, None, None, None, "70px"])
    return _page("搜索", body, "search")


# ---------------------------------------------------------------------------
# Page: File Tracking
# ---------------------------------------------------------------------------

@app.get("/tracking", response_class=HTMLResponse)
async def page_tracking():
    if not os.path.isfile(STATE_FILE):
        body = '<div class="alert alert-info">未找到追踪状态文件，文档尚未被导入。</div>'
        return _page("文件追踪", body, "tracking")

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        body = f'<div class="alert alert-danger">读取状态文件失败：{_esc(str(e))}</div>'
        return _page("文件追踪", body, "tracking")

    files = state.get("files", {})
    body = (
        f'<div class="alert alert-info">正在追踪 {len(files)} 个文件。'
        f'导入进行中时，文件数量会随进度实时增加。状态文件：{_esc(STATE_FILE)}</div>'
        f'<div style="text-align:right;margin-bottom:8px;">'
        f'<button onclick="location.reload()" style="background:#30363d;color:#e6edf3;'
        f'border:1px solid #444;border-radius:6px;padding:4px 12px;cursor:pointer;font-size:13px;">'
        f'&#8635; 刷新状态</button></div>'
    )

    rows_html = ""
    for path, info in sorted(files.items()):
        if isinstance(info, dict):
            sha = (info.get("hash", "") or "")[:16] + "..."
            rid = info.get("resource_id", "") or ""
            raw_ts = info.get("ingested_at", "")
            if isinstance(raw_ts, (int, float)):
                from datetime import datetime, timezone
                ts = datetime.fromtimestamp(raw_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            else:
                ts = (str(raw_ts) if raw_ts else "")[:16]
        else:
            sha = str(info)[:16] + "..."
            rid = ""
            ts = ""
        rid_short = rid[:12] + "..." if rid else ""
        rows_html += (
            f'<tr><td title="{_esc(path)}">{_esc(_short(path, 60))}</td>'
            f'<td class="mono" style="font-size:11px;">{sha}</td>'
            f'<td class="mono" style="font-size:11px;">{rid_short}</td>'
            f'<td style="font-size:11px;color:#8b949e;">{ts}</td></tr>'
        )
    body += _table(["文件路径", "SHA-256", "资源 ID", "导入时间"], rows_html)

    body += '<h3 style="color:#e6edf3;margin:20px 0 12px;">原始状态</h3>'
    body += f"<pre>{_esc(json.dumps(state, ensure_ascii=False, indent=2))}</pre>"

    return _page(f"文件追踪 ({len(files)})", body, "tracking")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.path.isfile(DB_PATH):
        print(f"[错误] 未找到数据库：{DB_PATH}")
        sys.exit(1)

    # Kill old process occupying the port
    def _kill_port_occupant(port: int) -> None:
        """Find and kill any process listening on the given port."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("127.0.0.1", port))
            sock.close()
            if result != 0:
                return  # Port is free
        except Exception:
            return

        print(f"[信息] 端口 {port} 被占用，正在释放...")
        if sys.platform == "win32":
            try:
                out = subprocess.check_output(
                    ["powershell", "-NoProfile", "-Command",
                     f"(Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue).OwningProcess"],
                    timeout=5, text=True
                )
                pids = set()
                for line in out.strip().splitlines():
                    line = line.strip()
                    if line.isdigit() and int(line) > 0:
                        pids.add(int(line))
                my_pid = os.getpid()
                for pid in pids:
                    if pid == my_pid:
                        continue
                    try:
                        subprocess.run(["taskkill", "/PID", str(pid), "/F", "/T"],
                                       capture_output=True, timeout=5)
                        print(f"[信息] 已终止旧进程 PID {pid}")
                    except Exception:
                        pass
            except Exception:
                pass
        else:
            try:
                subprocess.run(["fuser", "-k", f"{port}/tcp"],
                               capture_output=True, timeout=5)
            except Exception:
                pass
        # Brief wait for port release
        import time
        time.sleep(0.5)

    _kill_port_occupant(PORT)

    print(f"[信息] memU 控制台启动于 http://127.0.0.1:{PORT}")
    print(f"[信息] 数据库：{DB_PATH}")

    # Orphan guard: if the parent OpenClaw process exits we shut down automatically.
    if PARENT_PID > 0:
        import time as _time
        print(f"[信息] 孤儿守护已启动，监控父进程 PID {PARENT_PID}")

        def _parent_watchdog() -> None:
            while True:
                _time.sleep(5)
                if sys.platform == "win32":
                    try:
                        out = subprocess.run(
                            ["tasklist", "/FI", f"PID eq {PARENT_PID}", "/NH"],
                            capture_output=True, text=True, timeout=5
                        ).stdout
                        if str(PARENT_PID) not in out:
                            print(f"[信息] 父进程 {PARENT_PID} 已退出，关闭控制台。")
                            os._exit(0)
                    except Exception:
                        pass
                else:
                    try:
                        os.kill(PARENT_PID, 0)
                    except PermissionError:
                        pass  # Process exists but access denied — keep running
                    except (ProcessLookupError, OSError):
                        print(f"[信息] 父进程 {PARENT_PID} 已退出，关闭控制台。")
                        os._exit(0)

        t = threading.Thread(target=_parent_watchdog, daemon=True)
        t.start()

    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
