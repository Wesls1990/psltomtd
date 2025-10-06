#!/usr/bin/env python3
"""
PSL ➜ MTD Reconciliation — single‑file MVP

Quick start:
  pip install flask pandas openpyxl xlrd python-multipart
  python app.py

Open http://127.0.0.1:5000

Notes:
- Upload one or more .xlsx/.xls files (purchases and/or sales). Each file is treated as a "show".
- We auto-detect the first sheet with tabular data. If both purchases and sales exist in one file (multi-sheets), we process all sheets.
- VAT code normaliser is extensible via NORMALISE_MAP and regexes.
- Returns per-show and consolidated Box 1–9. Box logic is simplified and can be tuned.
- Export endpoint returns an .xlsx with a Summary sheet + per-show detail.
"""

from __future__ import annotations
import io
import os
import re
import json
import datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

from flask import Flask, request, send_file, Response
import pandas as pd

APP_TITLE = "PSL ➜ MTD Reconciliation (MVP)"
app = Flask(__name__)

# -------------------------
# Helpers — column matching
# -------------------------
COL_CANDIDATES = {
    "date": ["date", "txn date", "doc date", "invoice date", "posting date"],
    "ref": ["invoice", "inv", "document", "doc no", "reference", "ref"],
    "supplier": ["supplier", "vendor", "customer", "name", "account name"],
    "description": ["description", "narrative", "memo", "details"],
    "net": ["net", "amount (excl)", "amount excl vat", "goods", "taxable amount", "base", "amount"],
    "vat": ["vat", "tax", "vat amount", "tax amount", "vat amt"],
    "gross": ["gross", "amount (incl)", "total", "amount incl vat"],
    "vat_code": ["vat code", "tax code", "code", "t-code", "vat type", "rate", "vat rate"],
    "currency": ["currency", "curr", "ccy"],
}

BOX_NAMES = {
    1: "VAT due on sales (Box 1)",
    4: "VAT reclaimable on purchases (Box 4)",
    6: "Total value of sales ex VAT (Box 6)",
    7: "Total value of purchases ex VAT (Box 7)",
    8: "Value of supplies to EU (NI only) (Box 8)",
    9: "Value of acquisitions from EU (NI only) (Box 9)",
}

# Normalisation map — extend freely
NORMALISE_MAP = {
    # 20% equivalents
    "t20": "T20", "t1": "T20", "std": "T20", "standard": "T20", "20": "T20", "20%": "T20",
    # Zero / Exempt / Out of Scope
    "t0": "T0", "z": "T0", "zero": "T0", "0": "T0", "0%": "T0",
    "e": "EXEMPT", "exempt": "EXEMPT",
    "vx": "OOS", "oos": "OOS", "outofscope": "OOS", "out of scope": "OOS",
    # Reduced rates (tune if needed)
    "t5": "REDUCED", "5": "REDUCED", "5%": "REDUCED", "reduced": "REDUCED",
    # NI/EU markers (heuristic)
    "ni": "NI", "northern ireland": "NI",
    "eu": "EU", "ec": "EU", "eec": "EU",
}

VAT_CODE_REGEX = re.compile(r"(?i)(t\s*-?\s*20|t\s*-?\s*1|std|standard|20%|20) |(t\s*-?\s*0|zero|0%) |(exempt|e) |(vx|out\s*of\s*scope|oos) |(t\s*-?\s*5|5%|reduced) |(ni|northern\s*ireland|\bEU\b|\bEC\b)")

@dataclass
class Line:
    show: str
    sheet: str
    date: Optional[str]
    ref: Optional[str]
    supplier: Optional[str]
    description: Optional[str]
    net: float
    vat: float
    gross: float
    vat_code: Optional[str]
    source_type: str  # "purchases"|"sales"|"unknown"
    raw: Dict[str, Any]

# -------------------------
# Core parsing & mapping
# -------------------------

def _find_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    cols = {c: re.sub(r"\s+", " ", str(c)).strip().lower() for c in df.columns}
    for want in keys:
        want_l = want.lower()
        for c, lc in cols.items():
            if want_l == lc:
                return c
    # fuzzy contains
    for want in keys:
        w = want.lower()
        for c in df.columns:
            if w in str(c).lower():
                return c
    return None


def _to_float(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return 0.0


def normalise_vat_code(val: Any, description: str = "") -> str:
    s = str(val or "").strip().lower()
    # Try regex buckets
    if s:
        m = VAT_CODE_REGEX.search(s)
        if m:
            block = m.group(0).lower()
            for k, v in NORMALISE_MAP.items():
                if k in block:
                    return v
    # Try mapping direct tokens
    tokens = re.split(r"[^a-z0-9%]+", s) if s else []
    for t in tokens:
        if t in NORMALISE_MAP:
            return NORMALISE_MAP[t]
    # Fallback to description scan
    d = (description or "").lower()
    for k, v in NORMALISE_MAP.items():
        if k in d:
            return v
    return "UNKNOWN"


def detect_source_type(sheet_name: str, filename: str) -> str:
    probe = f"{sheet_name} {filename}".lower()
    if any(w in probe for w in ["sale", "ar", "output"]):
        return "sales"
    if any(w in probe for w in ["purchase", "ap", "input", "payable"]):
        return "purchases"
    return "unknown"


def parse_excel(file_storage) -> List[Line]:
    show = os.path.splitext(file_storage.filename)[0]
    content = file_storage.read()
    xls = pd.ExcelFile(io.BytesIO(content))

    lines: List[Line] = []
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            if df.empty:
                continue
            # Identify columns
            c_date = _find_col(df, COL_CANDIDATES["date"]) or None
            c_ref = _find_col(df, COL_CANDIDATES["ref"]) or None
            c_supplier = _find_col(df, COL_CANDIDATES["supplier"]) or None
            c_desc = _find_col(df, COL_CANDIDATES["description"]) or None
            c_net = _find_col(df, COL_CANDIDATES["net"]) or None
            c_vat = _find_col(df, COL_CANDIDATES["vat"]) or None
            c_gross = _find_col(df, COL_CANDIDATES["gross"]) or None
            c_code = _find_col(df, COL_CANDIDATES["vat_code"]) or None

            # If no numeric columns, skip
            if not (c_net or c_vat or c_gross):
                continue

            src_type = detect_source_type(sheet, file_storage.filename)

            for _, r in df.iterrows():
                net = _to_float(r.get(c_net)) if c_net else 0.0
                vat = _to_float(r.get(c_vat)) if c_vat else 0.0
                gross = _to_float(r.get(c_gross)) if c_gross else (net + vat)
                if net == 0 and vat == 0 and gross == 0:
                    continue
                vcode = normalise_vat_code(r.get(c_code), str(r.get(c_desc))) if c_code else normalise_vat_code("", str(r.get(c_desc)))
                line = Line(
                    show=show,
                    sheet=sheet,
                    date=str(r.get(c_date)) if c_date else None,
                    ref=str(r.get(c_ref)) if c_ref else None,
                    supplier=str(r.get(c_supplier)) if c_supplier else None,
                    description=str(r.get(c_desc)) if c_desc else None,
                    net=net,
                    vat=vat,
                    gross=gross,
                    vat_code=vcode,
                    source_type=src_type,
                    raw={k: (None if pd.isna(v) else v) for k, v in r.items()},
                )
                lines.append(line)
        except Exception:
            continue
    return lines

# -------------------------
# Box assignment logic
# -------------------------

def assign_boxes(lines: List[Line]) -> Dict[str, Any]:
    per_show: Dict[str, Dict[str, Any]] = {}

    def init_acc() -> Dict[str, float]:
        return {"1": 0.0, "4": 0.0, "6": 0.0, "7": 0.0, "8": 0.0, "9": 0.0}

    # Aggregate
    for ln in lines:
        show_acc = per_show.setdefault(ln.show, {"boxes": init_acc(), "lines": []})
        # Simplified logic:
        code = ln.vat_code
        st = ln.source_type
        # Box 1 and 6 for sales at standard rate
        if st == "sales" and code == "T20":
            show_acc["boxes"]["1"] += ln.vat
            show_acc["boxes"]["6"] += ln.net
        # Box 4 and 7 for purchases with reclaimable VAT
        elif st in ("purchases", "unknown") and code in ("T20", "REDUCED"):
            show_acc["boxes"]["4"] += ln.vat
            show_acc["boxes"]["7"] += ln.net
        # Zero-rated sales contribute to Box 6 net
        elif st == "sales" and code in ("T0",):
            show_acc["boxes"]["6"] += ln.net
        # NI/EU heuristics
        if code in ("NI", "EU") and st == "sales":
            show_acc["boxes"]["8"] += ln.net
        if code in ("NI", "EU") and st != "sales":
            show_acc["boxes"]["9"] += ln.net

        show_acc["lines"].append(asdict(ln))

    # Consolidated
    consolidated = {"1": 0.0, "4": 0.0, "6": 0.0, "7": 0.0, "8": 0.0, "9": 0.0}
    for show, acc in per_show.items():
        for k in consolidated:
            consolidated[k] += acc["boxes"][k]

    return {"per_show": per_show, "consolidated": consolidated}

# -------------------------
# Routes
# -------------------------

INDEX_HTML = f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\"> 
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{APP_TITLE}</title>
  <script defer src=\"https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js\"></script>
  <script>
    async function uploadFiles(ev) {{
      ev.preventDefault();
      const files = document.getElementById('files').files;
      if (!files.length) return alert('Select at least one .xlsx/.xls file');
      const fd = new FormData();
      for (const f of files) fd.append('files', f);
      const res = await fetch('/api/parse', {{ method: 'POST', body: fd }});
      const data = await res.json();
      window.state = data; // stash
      render(data);
    }}

    function render(data) {{
      const out = document.getElementById('output');
      if (!data || !data.consolidated) {{ out.innerHTML = '<p>No data.</p>'; return; }}
      // Summary table
      let html = '';
      html += `<div class=\"card\"><h2>Consolidated Boxes</h2>`;
      html += tableBoxes(data.consolidated);
      html += `</div>`;
      html += `<div class=\"grid\">`;
      for (const [show, acc] of Object.entries(data.per_show)) {{
        html += `<div class=\"card\"><h3>${{show}}</h3>`;
        html += tableBoxes(acc.boxes);
        html += `</div>`;
      }}
      html += `</div>`;
      out.innerHTML = html;
    }}

    function tableBoxes(boxes) {{
      const labels = {{1:'Box 1',4:'Box 4',6:'Box 6',7:'Box 7',8:'Box 8',9:'Box 9'}};
      let t = `<table><thead><tr><th>Box</th><th>Total (£)</th></tr></thead><tbody>`;
      for (const k of ['1','4','6','7','8','9']) {{
        const v = Number(boxes[k]||0).toFixed(2);
        t += `<tr><td>${{labels[k]}}</td><td style=\"text-align:right\">${{v}}</td></tr>`;
      }}
      t += `</tbody></table>`;
      return t;
    }

    async function exportPack() {{
      if (!window.state) return alert('Parse some files first.');
      const res = await fetch('/api/export', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(window.state)
      }});
      if (!res.ok) return alert('Export failed');
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = 'PSLtoMTD_SubmissionPack.xlsx'; a.click();
      URL.revokeObjectURL(url);
    }}
  </script>
  <style>
    :root {{ --bg:#0b0b0d; --fg:#e7e7f0; --card:#121218; --border:#2a2a39; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font: 15px/1.4 system-ui, -apple-system, Segoe UI, Roboto, Inter, Arial; background: radial-gradient(1200px 600px at 20% -10%, #5920ff33, transparent), radial-gradient(800px 500px at 80% 10%, #00d4ff33, transparent), var(--bg); color: var(--fg); }}
    header {{ padding: 24px; text-align:center; }}
    h1 {{ font-size: 28px; letter-spacing:0.2px; text-shadow: 0 6px 20px #000; }}
    .wrap {{ max-width: 1080px; margin: 0 auto; padding: 16px 24px 48px; }}
    .uploader {{ margin: 12px 0 20px; padding: 16px; border: 1px solid var(--border); background: color-mix(in lab, var(--card) 90%, #000); border-radius: 14px; backdrop-filter: blur(8px); }}
    .btn {{ background: linear-gradient(90deg, #7b5cff, #4aa3ff); border:0; color:white; padding: 10px 14px; border-radius: 12px; cursor: pointer; font-weight:600; box-shadow: 0 8px 24px #00000055; }}
    .btn.secondary {{ background: #1e1e2a; border:1px solid var(--border); }}
    .card {{ border:1px solid var(--border); border-radius: 14px; padding: 14px; margin: 10px 0; background: #0f0f15aa; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(280px,1fr)); gap: 12px; }}
    table {{ width:100%; border-collapse: collapse; margin-top: 8px; }}
    th, td {{ padding: 8px 10px; border-bottom:1px solid #26263a; }}
  </style>
</head>
<body>
  <header>
    <h1>PSL → MTD Reconciliation</h1>
    <p style=\"opacity:.8\">Upload PSL VAT ledgers, auto-map to MTD boxes, consolidate per-show, and export a submission pack.</p>
  </header>
  <div class=\"wrap\">
    <form class=\"uploader\" onsubmit=\"uploadFiles(event)\">
      <input id=\"files\" type=\"file\" accept=\".xlsx,.xls\" multiple />
      <button class=\"btn\" type=\"submit\">Parse Files</button>
      <button class=\"btn secondary\" type=\"button\" onclick=\"exportPack()\">Export Submission Pack</button>
    </form>
    <div id=\"output\"></div>
  </div>
</body>
</html>
"""

@app.get("/")
def index() -> Response:
    return Response(INDEX_HTML, mimetype="text/html")

@app.post("/api/parse")
def api_parse():
    files = request.files.getlist("files")
    if not files:
        return {"error": "No files uploaded"}, 400

    all_lines: List[Line] = []
    for fs in files:
        try:
            all_lines.extend(parse_excel(fs))
        except Exception as e:
            return {"error": f"Failed to parse {fs.filename}: {e}"}, 400

    result = assign_boxes(all_lines)
    # For transport, drop raw lines to keep payload lean
    for show, acc in result["per_show"].items():
        acc["lines"] = acc["lines"][:500]  # cap preview
    return json.dumps(result), 200, {"Content-Type": "application/json"}

@app.post("/api/export")
def api_export():
    payload = request.get_json(silent=True) or {}
    consolidated = payload.get("consolidated", {})
    per_show = payload.get("per_show", {})

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        # Summary sheet
        summary_rows = []
        for k in ["1","4","6","7","8","9"]:
            summary_rows.append([BOX_NAMES.get(int(k), f"Box {k}"), float(consolidated.get(k, 0.0))])
        df_sum = pd.DataFrame(summary_rows, columns=["Box", "Total (£)"])
        df_sum.to_excel(xw, sheet_name="Summary", index=False)

        # Per-show box totals
        show_rows = []
        for show, acc in per_show.items():
            for k in ["1","4","6","7","8","9"]:
                show_rows.append([show, BOX_NAMES.get(int(k), f"Box {k}"), float(acc.get("boxes", {}).get(k, 0.0))])
        pd.DataFrame(show_rows, columns=["Show", "Box", "Total (£)"]).to_excel(xw, sheet_name="Per-Show Totals", index=False)

        # Detail tabs (optional)
        for show, acc in per_show.items():
            lines = acc.get("lines", [])
            if not lines:
                continue
            df = pd.DataFrame(lines)
            # Trim raw if present
            if "raw" in df.columns:
                df.drop(columns=["raw"], inplace=True)
            safe_name = re.sub(r"[^A-Za-z0-9]+", "_", show)[:28]
            df.to_excel(xw, sheet_name=f"{safe_name}_detail", index=False)

    out.seek(0)
    fname = f"PSLtoMTD_SubmissionPack_{dt.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, as_attachment=True, download_name=fname, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    # Bind to 0.0.0.0 for Codespaces/containers, port 5000
    app.run(host="0.0.0.0", port=5000, debug=True)
