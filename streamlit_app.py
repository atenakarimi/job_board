"""
Luxembourg AI Job Board — streamlit_app.py
Run with: streamlit run streamlit_app.py
"""

import io
import time
import pandas as pd
import streamlit as st

from main import get_jobs, filter_jobs, export_csv, CACHE_FILE
import os
import json
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="AI Jobs Luxembourg",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# CUSTOM CSS — dark neon terminal aesthetic
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Courier New', monospace;
    background-color: #07091a;
    color: #c8d6f0;
}
.stApp { background-color: #07091a; }

/* ── Header ── */
.board-header {
    background: #05070f;
    border-bottom: 1px solid #1e3a5f;
    padding: 14px 20px 10px;
    margin-bottom: 16px;
}
.board-title {
    font-size: 22px;
    font-weight: bold;
    letter-spacing: 3px;
    color: #00e5ff;
}
.board-title span { color: #00ff88; }
.board-subtitle {
    font-size: 11px;
    color: #5a7a9a;
    margin-top: 2px;
}

/* ── Metric cards ── */
.metric-row { display: flex; gap: 12px; margin-bottom: 16px; }
.metric-card {
    background: #0d1525;
    border: 1px solid #1e3a5f;
    border-radius: 6px;
    padding: 10px 16px;
    min-width: 120px;
}
.metric-label { font-size: 10px; color: #5a7a9a; text-transform: uppercase; letter-spacing: 1px; }
.metric-value { font-size: 22px; font-weight: bold; color: #00e5ff; }

/* ── Filter bar ── */
[data-testid="stSelectbox"] > div,
[data-testid="stTextInput"] > div > div > input,
[data-testid="stSlider"] {
    background-color: #111827 !important;
    border: 1px solid #1e3a5f !important;
    color: #c8d6f0 !important;
    font-family: 'Courier New', monospace !important;
    font-size: 12px !important;
    border-radius: 4px !important;
}
label { color: #5a7a9a !important; font-size: 11px !important; text-transform: uppercase; letter-spacing: 1px; }

/* ── Buttons ── */
.stButton > button {
    background: transparent;
    border: 1px solid #00ff88;
    color: #00ff88;
    font-family: 'Courier New', monospace;
    font-size: 12px;
    border-radius: 4px;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #00ff88;
    color: #000;
}

/* ── Table ── */
.job-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.job-table thead th {
    background: #05070f;
    color: #00e5ff;
    padding: 8px 8px;
    border-bottom: 1px solid #1e3a5f;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 1px;
    text-align: left;
    white-space: nowrap;
}
.job-table tbody tr:nth-child(odd)  { background: #0b1323; }
.job-table tbody tr:nth-child(even) { background: #09101c; }
.job-table tbody tr:hover { background: #132030; }
.job-table td {
    padding: 7px 8px;
    border-bottom: 1px solid #0d1a2e;
    vertical-align: top;
    color: #c8d6f0;
}

/* ── Pills ── */
.pill { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 10px; font-weight: bold; margin: 1px; white-space: nowrap; }
.pill-li  { background: #0a2a4a; border: 1px solid #0077b5; color: #40a8dc; }
.pill-sen { background: #1a1a3a; border: 1px solid #5a5aff; color: #9090ff; }
.pill-sk  { background: #0d2a1a; border: 1px solid #1a5a2a; color: #4adf7a; font-size: 9px; padding: 1px 5px; border-radius: 3px; }
.pill-fr  { background: #2a0a1a; border: 1px solid #ff4d94; color: #ff4d94; }
.pill-no  { background: #0a1a0a; border: 1px solid #2a6a2a; color: #4adf4a; }
.pill-pref{ background: #1a1a0a; border: 1px solid #6a6a0a; color: #aaaa4a; }

/* ── Links ── */
.jlink { color: #00e5ff; text-decoration: none; font-size: 10px; border-bottom: 1px dashed rgba(0,229,255,.4); }
.jlink:hover { color: #fff; }
.abtn  { display: inline-block; border: 1px solid #00ff88; color: #00ff88; padding: 2px 9px; border-radius: 3px; font-size: 10px; text-decoration: none; }
.abtn:hover { background: #00ff88; color: #000; }

/* ── Countdown ── */
.countdown { font-size: 11px; color: #3a5a7a; font-family: 'Courier New', monospace; }

/* ── Scrollable table wrapper ── */
.table-scroll { overflow-x: auto; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# SESSION STATE
# ---------------------------------------------------------------------------

if "jobs" not in st.session_state:
    st.session_state.jobs = []
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = None
if "loading" not in st.session_state:
    st.session_state.loading = False


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def cache_age_str() -> str:
    if not os.path.exists(CACHE_FILE):
        return "no cache"
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        ts = datetime.fromisoformat(data["timestamp"])
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age < 3600:
            return f"{int(age/60)}m ago"
        return f"{age/3600:.1f}h ago"
    except Exception:
        return "unknown"


def french_pill(val: str) -> str:
    if val == "Yes":
        return '<span class="pill pill-fr">⚠ FR req</span>'
    if val == "Preferred":
        return '<span class="pill pill-pref">~ FR pref</span>'
    return '<span class="pill pill-no">✓ No FR</span>'


def seniority_pill(val: str) -> str:
    return f'<span class="pill pill-sen">{val}</span>'


def source_pill(val: str) -> str:
    return f'<span class="pill pill-li">LI</span>'


def skills_html(skills: list[str]) -> str:
    return " ".join(f'<span class="pill pill-sk">{s}</span>' for s in skills[:7]) or "—"


def truncate_url(url: str, n: int = 38) -> str:
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        s = p.netloc + p.path
        return s[:n] + "…" if len(s) > n else s
    except Exception:
        return url[:n] + "…"


def jobs_to_html_table(jobs: list[dict]) -> str:
    rows = []
    for i, j in enumerate(jobs, 1):
        job_link = f'<a href="{j["job_url"]}" target="_blank" class="jlink" title="{j["job_url"]}">{truncate_url(j["job_url"])}</a>'
        apply_btn = f'<a href="{j["apply_url"]}" target="_blank" class="abtn">Apply →</a>'
        skills = skills_html(j["skills"])
        exp_color = "#ffd700"

        row = f"""
        <tr>
          <td style="color:#3a5a7a">{i}</td>
          <td style="color:#ddeeff;min-width:150px">{j['title']}</td>
          <td style="color:#8ab0d0;min-width:110px">{j['company']}</td>
          <td style="color:#5a7a9a;white-space:nowrap">{j['location']}</td>
          <td>{seniority_pill(j['seniority'])}</td>
          <td style="color:{exp_color};text-align:center">{j['exp_label']}</td>
          <td>{french_pill(j['french'])}</td>
          <td style="max-width:160px">{skills}</td>
          <td style="color:#4a6a8a;white-space:nowrap">{j['posted_label']}</td>
          <td style="max-width:160px">{job_link}</td>
          <td>{apply_btn}</td>
        </tr>"""
        rows.append(row)

    headers = ["#", "Job Title", "Company", "Location", "Level",
               "Exp", "French?", "Skills", "Posted", "Job URL", "Apply"]
    header_html = "".join(f"<th>{h}</th>" for h in headers)

    return f"""
    <div class="table-scroll">
    <table class="job-table">
      <thead><tr>{header_html}</tr></thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
    </div>
    """


# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

st.markdown("""
<div class="board-header">
  <div class="board-title">AI<span>JOBS</span>.LU</div>
  <div class="board-subtitle">Luxembourg Data Science · Machine Learning · AI — LinkedIn (Apify) + Glassdoor (jobspy) · last 2 weeks</div>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# CONTROLS ROW
# ---------------------------------------------------------------------------

col_refresh, col_info, col_export = st.columns([1, 3, 1])

with col_refresh:
    force_refresh = st.button("↻ Refresh jobs", key="refresh_btn")

with col_info:
    age = cache_age_str()
    st.markdown(f'<span class="countdown">Last scraped: {age} &nbsp;|&nbsp; Data: LinkedIn + Glassdoor (Luxembourg, last 14 days)</span>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# FETCH DATA
# ---------------------------------------------------------------------------

if force_refresh or not st.session_state.jobs:
    with st.spinner("Scraping LinkedIn via Apify… this takes ~30–60 seconds on first run."):
        try:
            st.session_state.jobs = get_jobs(force_refresh=force_refresh)
            st.session_state.last_fetch = datetime.now()
        except ValueError as e:
            st.error(f"**Configuration error:** {e}")
            st.info("Set your Apify token: `export APIFY_API_TOKEN=your_token_here`  \nGet it at https://console.apify.com/account/integrations")
            st.stop()
        except Exception as e:
            st.error(f"**Scrape failed:** {e}")
            if os.path.exists(CACHE_FILE):
                st.warning("Showing cached data instead.")
                import json
                with open(CACHE_FILE) as f:
                    st.session_state.jobs = json.load(f)["jobs"]
            else:
                st.stop()

jobs_all = st.session_state.jobs


# ---------------------------------------------------------------------------
# FILTERS
# ---------------------------------------------------------------------------

st.markdown("---")
fc1, fc2, fc3, fc4, fc5 = st.columns([1.2, 1.5, 2, 1.5, 1.2])

with fc1:
    seniority_filter = st.selectbox(
        "Seniority",
        ["All", "Junior", "Intern", "Mid", "Senior", "Director", "VP", "C-Suite"],
        index=1,  # default: Junior
    )

with fc2:
    exp_filter = st.slider(
        "Max experience (years)",
        min_value=0, max_value=10, value=5, step=1
    )

with fc3:
    query_filter = st.text_input("Search", placeholder="title, company, skill…")

with fc4:
    french_filter = st.selectbox(
        "French",
        ["All", "No French", "French Required"],
        index=0
    )

with fc5:
    sort_by = st.selectbox(
        "Sort by",
        ["Posted (newest)", "Posted (oldest)", "Company A–Z", "Exp (low–high)"],
        index=0
    )

st.markdown("---")


# ---------------------------------------------------------------------------
# APPLY FILTERS
# ---------------------------------------------------------------------------

jobs_filtered = filter_jobs(
    jobs_all,
    seniority="" if seniority_filter == "All" else seniority_filter,
    max_exp=exp_filter if exp_filter < 10 else 99,
    french="" if french_filter == "All" else french_filter,
    query=query_filter,
)


# Sort
if sort_by == "Posted (newest)":
    jobs_filtered.sort(key=lambda j: j["posted_at"], reverse=True)
elif sort_by == "Posted (oldest)":
    jobs_filtered.sort(key=lambda j: j["posted_at"])
elif sort_by == "Company A–Z":
    jobs_filtered.sort(key=lambda j: j["company"].lower())
elif sort_by == "Exp (low–high)":
    jobs_filtered.sort(key=lambda j: j["exp_num"])


# ---------------------------------------------------------------------------
# METRIC CARDS
# ---------------------------------------------------------------------------

total = len(jobs_all)
showing = len(jobs_filtered)
entry_count = sum(1 for j in jobs_filtered if j["seniority"] == "Entry")
no_french = sum(1 for j in jobs_filtered if j["french"] == "No")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total scraped", total)
m2.metric("Showing", showing)
m3.metric("Entry / Mid level", entry_count)
m4.metric("No French required", no_french)


# ---------------------------------------------------------------------------
# CSV EXPORT
# ---------------------------------------------------------------------------

with col_export:
    if jobs_filtered:
        csv_data = export_csv(jobs_filtered)
        st.download_button(
            label="↓ Export CSV",
            data=csv_data.encode("utf-8"),
            file_name=f"lux_ai_jobs_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
        )


# ---------------------------------------------------------------------------
# TABLE
# ---------------------------------------------------------------------------

if not jobs_filtered:
    st.warning("No jobs match your filters. Try resetting them.")
else:
    st.markdown(
        jobs_to_html_table(jobs_filtered),
        unsafe_allow_html=True
    )


# ---------------------------------------------------------------------------
# EXPANDABLE JOB DETAILS
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("#### 🔍 Job descriptions")
st.caption("Click any job below to read the full description.")

for j in jobs_filtered:
    fr_tag = " ⚠ FR" if j["french"] == "Yes" else ""
    label = f"**{j['title']}** — {j['company']} ({j['seniority']}, {j['exp_label']} yrs){fr_tag}"
    with st.expander(label):
        col_a, col_b = st.columns([2, 1])
        with col_a:
            st.markdown(f"**Location:** {j['location']}  \n**Posted:** {j['posted_label']}  \n**French:** {j['french']}")
            st.markdown("**Skills detected:**  \n" + "  ".join(f"`{s}`" for s in j["skills"]) or "*none detected*")
            st.text_area("Description", j["description"], height=200, key=f"desc_{j['job_id']}")
        with col_b:
            st.markdown(f"[🔗 View on LinkedIn]({j['job_url']})")
            st.markdown(f"[📝 Apply here]({j['apply_url']})")


# ---------------------------------------------------------------------------
# FOOTER
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(
    '<p style="color:#2a4a6a;font-size:11px;text-align:center;font-family:monospace">'
    'AIJOBS.LU · Scraped via Apify curious_coder/linkedin-jobs-scraper · '
    'Built for MSc Data Science job search in Luxembourg'
    '</p>',
    unsafe_allow_html=True
)
