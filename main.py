"""
Luxembourg AI Job Board — main.py
Scrapes LinkedIn jobs via Apify, processes and filters them.
Run standalone: python main.py
Or imported by streamlit_app.py
"""

import os
import re
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional
from apify_client import ApifyClient

try:
    import jobspy
    _JOBSPY_AVAILABLE = True
except ImportError:
    _JOBSPY_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN", "")

LINKEDIN_SEARCH_URLS = [
    # www.linkedin.com
    "https://www.linkedin.com/jobs/search/?keywords=Data+Scientist&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=Machine+Learning+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=AI+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=Data+Analyst&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=Data+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=NLP+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://www.linkedin.com/jobs/search/?keywords=Computer+Vision+Engineer&location=Luxembourg&f_TPR=r1209600",
    # lu.linkedin.com (Luxembourg-localised subdomain)
    "https://lu.linkedin.com/jobs/search/?keywords=Data+Scientist&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=Machine+Learning+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=AI+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=Data+Analyst&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=Data+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=NLP+Engineer&location=Luxembourg&f_TPR=r1209600",
    "https://lu.linkedin.com/jobs/search/?keywords=Computer+Vision+Engineer&location=Luxembourg&f_TPR=r1209600",
]

ACTOR_ID = "curious_coder/linkedin-jobs-scraper"
ACTOR_INPUT = {
    "urls": LINKEDIN_SEARCH_URLS,
    "count": 60,
    "scrapeCompany": False,
}

RELEVANT_KEYWORDS = [
    "data scientist", "machine learning", "ml engineer", "ai engineer",
    "data analyst", "data engineer", "nlp", "natural language processing",
    "computer vision", "deep learning", "llm", "large language model",
    "artificial intelligence", "data science", "mlops", "rag",
    "neural network", "applied scientist", "research scientist",
    "analytics engineer", "business intelligence", "bi analyst",
    "junior data", "data intern", "ai intern", "ml intern",
]

IRRELEVANT_KEYWORDS = [
    ".net developer", "servicenow", "devsecops", "java developer",
    "php developer", "sap consultant", "erp", "accountant",
    "financial controller", "nurse", "doctor", "sales manager",
    "marketing manager", "lawyer", "legal counsel", "hr manager",
    "secretary", "receptionist", "driver", "warehouse",
]

SKILLS_LIST = [
    "Python", "R", "SQL", "Spark", "Kafka", "Hadoop",
    "Azure", "AWS", "GCP", "Docker", "Kubernetes",
    "PyTorch", "TensorFlow", "scikit-learn", "HuggingFace",
    "LangChain", "RAG", "FAISS", "Milvus", "FastAPI",
    "Streamlit", "MLflow", "MLOps", "NLP", "LLM",
    "OpenCV", "YOLO", "Databricks", "Power BI", "Tableau",
    "JavaScript", "TypeScript", "Java", "C++", "Git",
    "Transformers", "spaCy", "NLTK", "Pandas", "NumPy",
    "Airflow", "dbt", "BigQuery", "Snowflake", "OpenAI",
    "Langchain", "Pinecone", "Weaviate", "ChromaDB",
    "Plotly", "Seaborn", "Matplotlib", "Excel", "Looker",
]

# ---------------------------------------------------------------------------
# SCRAPING
# ---------------------------------------------------------------------------

def run_scraper() -> list[dict]:
    """Run the Apify LinkedIn jobs scraper and return raw items."""
    if not APIFY_TOKEN:
        raise ValueError(
            "APIFY_API_TOKEN environment variable is not set. "
            "Get your token at https://console.apify.com/account/integrations"
        )

    log.info("Starting Apify actor: %s", ACTOR_ID)
    client = ApifyClient(APIFY_TOKEN)

    run = client.actor(ACTOR_ID).call(run_input=ACTOR_INPUT)
    log.info("Actor run finished. Run ID: %s", run.get("id"))

    dataset_id = run.get("defaultDatasetId")
    log.info("Fetching dataset: %s", dataset_id)

    items = list(
        client.dataset(dataset_id).iterate_items()
    )
    log.info("Fetched %d raw items from dataset", len(items))
    return items


def run_glassdoor_scraper() -> list[dict]:
    """Scrape Glassdoor jobs via jobspy (free, no token needed)."""
    if not _JOBSPY_AVAILABLE:
        log.warning("python-jobspy not installed — skipping Glassdoor. Run: pip install python-jobspy")
        return []

    keywords = [
        "Data Scientist", "Machine Learning Engineer", "AI Engineer",
        "Data Analyst", "Data Engineer", "NLP Engineer",
    ]
    raw_rows = []
    for kw in keywords:
        try:
            df = jobspy.scrape_jobs(
                site_name=["glassdoor"],
                search_term=kw,
                location="Luxembourg",
                results_wanted=30,
                hours_old=336,  # 14 days
                country_indeed="Luxembourg",
            )
            raw_rows.append(df)
            log.info("Glassdoor '%s': %d results", kw, len(df))
        except Exception as e:
            log.warning("Glassdoor scrape failed for '%s': %s", kw, e)

    if not raw_rows:
        return []

    import pandas as pd
    combined = pd.concat(raw_rows, ignore_index=True).drop_duplicates(subset=["job_url"])

    items = []
    for _, row in combined.iterrows():
        title = str(row.get("title", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        location = str(row.get("location", "Luxembourg") or "Luxembourg").strip()
        description = str(row.get("description", "") or "")
        job_url = str(row.get("job_url", "") or "")
        posted_at = ""
        if row.get("date_posted") is not None:
            try:
                posted_at = pd.Timestamp(row["date_posted"]).isoformat()
            except Exception:
                posted_at = ""

        if not title or not job_url:
            continue

        items.append({
            "title": title,
            "companyName": company,
            "location": location,
            "postedAt": posted_at,
            "applyUrl": job_url,
            "link": job_url,
            "descriptionText": description,
            "id": hashlib.md5(job_url.encode()).hexdigest(),
            "_source": "Glassdoor",
        })

    log.info("Glassdoor total: %d unique items", len(items))
    return items


# ---------------------------------------------------------------------------
# PARSING HELPERS
# ---------------------------------------------------------------------------

def parse_seniority(title: str, description: str) -> str:
    text = (title + " " + description).lower()
    if re.search(r'\b(intern|internship|stage|trainee)\b', text):
        return "Intern"
    if re.search(r'\b(junior|jr\.?|entry[\s-]level|graduate|grad)\b', text):
        return "Junior"
    if re.search(r'\b(c-suite|cto|cdo|ceo|coo|chief)\b', text):
        return "C-Suite"
    if re.search(r'\b(vp|vice\s*president)\b', text):
        return "VP"
    if re.search(r'\b(director|head\s*of)\b', text):
        return "Director"
    if re.search(r'\b(senior|sr\.?|lead|principal|staff)\b', text):
        return "Senior"
    return "Mid"


def parse_experience(description: str) -> Optional[int]:
    """Return minimum years of experience required, or None if not found."""
    text = description.lower()
    # "3-5 years" → 3
    m = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*years?', text)
    if m:
        return int(m.group(1))
    # "5+ years"
    m = re.search(r'(\d+)\+\s*years?', text)
    if m:
        return int(m.group(1))
    # "5 years of experience"
    m = re.search(r'(\d+)\s*years?\s*(?:of\s*)?experience', text)
    if m:
        return int(m.group(1))
    # intern / 0 years
    if re.search(r'\b(intern|trainee|no\s*exp|0\s*years?)\b', text):
        return 0
    return None


def parse_skills(description: str) -> list[str]:
    found = []
    for skill in SKILLS_LIST:
        if re.search(re.escape(skill), description, re.IGNORECASE):
            if skill not in found:
                found.append(skill)
    return found[:10]


def parse_french_required(description: str) -> str:
    text = description
    if re.search(
        r'french\s*(and|&|\/)\s*english.*required|'
        r'fluent.*french|french.*fluent|'
        r'french.*required|français.*obligatoire|'
        r'bilingue.*français',
        text, re.IGNORECASE
    ):
        return "Yes"
    if re.search(
        r'french.*preferred|french.*beneficial|'
        r'french.*advantage|french.*plus|french.*asset|'
        r'français.*atout|français.*souhaité',
        text, re.IGNORECASE
    ):
        return "Preferred"
    if re.search(r'\bfrench\b|\bfrançais\b', text, re.IGNORECASE):
        return "Yes"
    return "No"


def relative_date(date_str: str) -> str:
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days = (now - dt).days
        if days == 0:
            return "today"
        if days == 1:
            return "1d ago"
        return f"{days}d ago"
    except Exception:
        return date_str[:10] if date_str else "unknown"


def job_fingerprint(title: str, company: str) -> str:
    return hashlib.md5(f"{title.lower().strip()}{company.lower().strip()}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# FILTERING
# ---------------------------------------------------------------------------

def is_relevant(title: str, description: str) -> bool:
    text = (title + " " + description).lower()

    # Hard exclude
    for kw in IRRELEVANT_KEYWORDS:
        if kw in text and kw not in text.replace(kw, "").join(RELEVANT_KEYWORDS):
            if kw in title.lower():
                return False

    # Must match at least one relevant keyword
    for kw in RELEVANT_KEYWORDS:
        if kw in text:
            return True

    return False


def deduplicate(jobs: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for job in jobs:
        fp = job_fingerprint(job["title"], job["company"])
        if fp not in seen:
            seen.add(fp)
            result.append(job)
    return result


# ---------------------------------------------------------------------------
# PROCESSING
# ---------------------------------------------------------------------------

def process_raw_items(raw_items: list[dict]) -> list[dict]:
    """Convert raw Apify output into clean, enriched job dicts."""
    processed = []

    for item in raw_items:
        title = item.get("title", "").strip()
        company = item.get("companyName", "").strip()
        location = item.get("location", "Luxembourg").strip()
        # Clean repeated country suffix
        location = re.sub(r",?\s*Luxembourg,?\s*Luxembourg$", ", Luxembourg", location, flags=re.IGNORECASE)

        posted_at = item.get("postedAt", "")
        apply_url = item.get("applyUrl", "")
        link = item.get("link", "")  # Real LinkedIn URL from scraper
        description = item.get("descriptionText", "") or ""

        if not title or not link:
            continue

        if not is_relevant(title, description):
            log.debug("Filtered out: %s @ %s", title, company)
            continue

        exp_num = parse_experience(description)
        exp_label = (
            "0" if exp_num == 0
            else f"{exp_num}+" if exp_num is not None
            else "N/A"
        )

        job = {
            "job_id": item.get("id", ""),
            "title": title,
            "company": company,
            "location": location,
            "posted_at": posted_at,
            "posted_label": relative_date(posted_at),
            "job_url": link,
            "apply_url": apply_url or link,
            "description": description[:1500],
            "seniority": parse_seniority(title, description),
            "exp_num": exp_num if exp_num is not None else 99,
            "exp_label": exp_label,
            "french": parse_french_required(description),
            "skills": parse_skills(description),
            "source": item.get("_source", "LinkedIn"),
        }
        processed.append(job)

    deduped = deduplicate(processed)
    log.info("Processed: %d relevant jobs (from %d raw)", len(deduped), len(raw_items))
    return deduped


# ---------------------------------------------------------------------------
# CACHE
# ---------------------------------------------------------------------------

CACHE_FILE = "jobs_cache.json"
CACHE_TTL_HOURS = 24


def save_cache(jobs: list[dict]) -> None:
    data = {"timestamp": datetime.now(timezone.utc).isoformat(), "jobs": jobs}
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Saved %d jobs to cache: %s", len(jobs), CACHE_FILE)


def load_cache() -> Optional[list[dict]]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        ts = datetime.fromisoformat(data["timestamp"])
        age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        if age_hours > CACHE_TTL_HOURS:
            log.info("Cache expired (%.1f hours old)", age_hours)
            return None
        log.info("Loaded %d jobs from cache (%.1f hours old)", len(data["jobs"]), age_hours)
        return data["jobs"]
    except Exception as e:
        log.warning("Cache load failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def get_jobs(force_refresh: bool = False) -> list[dict]:
    """
    Main entry point. Returns list of processed job dicts.
    Uses cache if available and fresh, otherwise scrapes.
    """
    if not force_refresh:
        cached = load_cache()
        if cached is not None:
            return cached

    raw_linkedin = run_scraper()
    raw_glassdoor = run_glassdoor_scraper()
    jobs = process_raw_items(raw_linkedin + raw_glassdoor)
    save_cache(jobs)
    return jobs


def filter_jobs(
    jobs: list[dict],
    seniority: str = "",
    max_exp: int = 99,
    source: str = "",
    french: str = "",
    query: str = "",
) -> list[dict]:
    """Apply user-facing filters to a list of jobs."""
    result = []
    q = query.lower().strip()

    for j in jobs:
        if seniority and j["seniority"] != seniority:
            continue
        if max_exp < 99 and j["exp_num"] != 99 and j["exp_num"] > max_exp:
            continue
        if source and j["source"] != source:
            continue
        if french == "No French" and j["french"] != "No":
            continue
        if french == "French Required" and j["french"] != "Yes":
            continue
        if q:
            searchable = " ".join([
                j["title"], j["company"],
                " ".join(j["skills"]),
                j["description"][:300]
            ]).lower()
            if q not in searchable:
                continue
        result.append(j)

    return result


def export_csv(jobs: list[dict]) -> str:
    """Return CSV string for the given list of jobs."""
    import io
    import csv

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "#", "Title", "Company", "Location", "Seniority",
        "Exp", "French", "Skills", "Posted", "Job URL", "Apply URL", "Source"
    ])
    for i, j in enumerate(jobs, 1):
        writer.writerow([
            i, j["title"], j["company"], j["location"], j["seniority"],
            j["exp_label"], j["french"], "; ".join(j["skills"]),
            j["posted_label"], j["job_url"], j["apply_url"], j["source"]
        ])
    return output.getvalue()


# ---------------------------------------------------------------------------
# STANDALONE RUN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Luxembourg AI Job Board scraper")
    parser.add_argument("--refresh", action="store_true", help="Force re-scrape ignoring cache")
    parser.add_argument("--output", default="jobs_cache.json", help="Output JSON file")
    args = parser.parse_args()

    jobs = get_jobs(force_refresh=args.refresh)

    print(f"\n{'='*60}")
    print(f"  Luxembourg AI Jobs — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Found: {len(jobs)} relevant jobs")
    print(f"{'='*60}\n")

    for i, j in enumerate(jobs, 1):
        fr_flag = " ⚠ FR" if j["french"] == "Yes" else ""
        skills_str = ", ".join(j["skills"][:5]) or "—"
        print(f"{i:>3}. [{j['seniority']:6}] {j['title'][:45]:<45} | {j['company'][:30]:<30} | {j['exp_label']:>4} yrs{fr_flag}")
        print(f"     Skills: {skills_str}")
        print(f"     URL:    {j['job_url']}")
        print()

    # Save CSV too
    csv_path = "jobs_export.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(export_csv(jobs))
    print(f"CSV saved to: {csv_path}")
