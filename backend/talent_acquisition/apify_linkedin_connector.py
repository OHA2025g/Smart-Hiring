"""Apify LinkedIn search + profile enrichment for Find Matches."""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

APIFY_BASE_URL = "https://api.apify.com/v2"
APIFY_RUNS_COLLECTION = "apify_linkedin_runs"
CONNECTOR_NAME = "LINKEDIN"
CONNECTOR_COLL = "connector_configs"

DEFAULT_SEARCH_ACTOR = "harvestapi/linkedin-profile-search"
DEFAULT_ENRICH_ACTOR = "dev_fusion/linkedin-profile-scraper"
DEFAULT_EMAIL_ACTOR = "khadinakbar/linkedin-profile-email-scraper"
HARVESTAPI_SEARCH_ACTOR = "harvestapi/linkedin-profile-search"
POWERAI_SEARCH_ACTOR = "powerai/linkedin-peoples-search-scraper"

PIPELINE_SEARCH_RUNNING = "search_running"
PIPELINE_ENRICH_RUNNING = "enrich_running"
PIPELINE_EMAIL_RUNNING = "email_running"
PIPELINE_COMPLETED = "completed"
PIPELINE_FAILED = "failed"

PIPELINE_ACTIVE_STATUSES = (
    PIPELINE_SEARCH_RUNNING,
    PIPELINE_ENRICH_RUNNING,
    PIPELINE_EMAIL_RUNNING,
)

PLACEHOLDER_EMAIL_SUFFIXES = ("@apify-import.local", "@linkedin.local")
HARVESTAPI_PROFILE_SCRAPER_MODE = "Full + email search"

APIFY_RUN_SUCCEEDED = "SUCCEEDED"
APIFY_RUN_FAILED = {"FAILED", "ABORTED", "TIMED-OUT"}

GEOCODE_HINTS: Dict[str, str] = {
    "bangalore": "in:0:0:0:0:0:0",
    "bengaluru": "in:0:0:0:0:0:0",
    "mumbai": "in:0:0:0:0:0:0",
    "delhi": "in:0:0:0:0:0:0",
    "new delhi": "in:0:0:0:0:0:0",
    "hyderabad": "in:0:0:0:0:0:0",
    "pune": "in:0:0:0:0:0:0",
    "chennai": "in:0:0:0:0:0:0",
    "kolkata": "in:0:0:0:0:0:0",
    "india": "in:0:0:0:0:0:0",
    "san francisco": "us:0:0:0:0:0:0",
    "new york": "us:0:0:0:0:0:0",
    "london": "gb:0:0:0:0:0:0",
    "united states": "us:0:0:0:0:0:0",
    "usa": "us:0:0:0:0:0:0",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_apify_token() -> str:
    return (os.environ.get("APIFY_API_TOKEN") or "").strip()


def actor_api_path(actor_id: str) -> str:
    return actor_id.replace("/", "~")


def validate_apify_config(cfg: Dict[str, Any]) -> Tuple[bool, str]:
    if not cfg.get("enabled"):
        return False, "LinkedIn connector is disabled"
    if cfg.get("api_mode", "talent_rsc") != "apify":
        return False, "LinkedIn api_mode is not apify"
    token = get_apify_token()
    if not token:
        return False, "APIFY_API_TOKEN is not set in environment"
    if not (cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR):
        return False, "Missing apify_search_actor_id"
    if not (cfg.get("apify_enrich_actor_id") or DEFAULT_ENRICH_ACTOR):
        return False, "Missing apify_enrich_actor_id"
    return True, "Apify LinkedIn configured"


def resolve_geocode_location(job: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    default = (cfg.get("apify_default_geocode") or "").strip() or None
    loc_raw = (
        job.get("location")
        or job.get("job_location")
        or job.get("city")
        or job.get("country")
        or ""
    )
    loc = str(loc_raw).lower()
    if not loc:
        return default
    if "remote" in loc and "india" not in loc and "us" not in loc:
        return default
    for hint, code in GEOCODE_HINTS.items():
        if hint in loc:
            return code
    return default


def is_harvestapi_search_actor(actor_id: str) -> bool:
    aid = (actor_id or "").lower()
    return "harvestapi" in aid or "linkedin-profile-search" in aid


def is_powerai_search_actor(actor_id: str) -> bool:
    aid = (actor_id or "").lower()
    return "powerai" in aid or "linkedin-peoples-search-scraper" in aid


def resolve_locations_from_job(job: Dict[str, Any]) -> List[str]:
    loc_raw = (
        job.get("location")
        or job.get("job_location")
        or job.get("city")
        or ""
    )
    loc = str(loc_raw).strip()
    if not loc or "remote" in loc.lower():
        return []
    parts: List[str] = []
    seen: set[str] = set()
    for chunk in loc.replace("|", "/").replace(",", "/").split("/"):
        city = chunk.strip()
        if not city:
            continue
        city = city.split("(")[0].strip()
        if city.lower() in {"hybrid", "onsite", "on-site", "remote", "india", "usa", "uk"}:
            continue
        key = city.lower()
        if key not in seen:
            seen.add(key)
            parts.append(city[:80])
    return parts[:3]


def build_powerai_search_input(job: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    jt = (job.get("title") or job.get("normalized_title") or "").strip()
    title = jt or "software engineer"
    max_results = int(cfg.get("apify_max_results_per_search") or 30)
    max_results = max(5, min(max_results, 100))
    payload: Dict[str, Any] = {
        "title": title[:200],
        "maxResults": max_results,
    }
    geocode = resolve_geocode_location(job, cfg)
    if geocode:
        payload["geocode_location"] = geocode
    company = (job.get("company_name") or job.get("client_name") or "").strip()
    if company:
        payload["company"] = company[:120]
    return payload


def build_harvestapi_search_input(job: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    jt = (job.get("title") or job.get("normalized_title") or "").strip()
    search_query = jt or "software engineer"
    max_results = int(cfg.get("apify_max_results_per_search") or 30)
    max_results = max(5, min(max_results, 100))
    payload: Dict[str, Any] = {
        "searchQuery": search_query[:200],
        "maxItems": max_results,
        "takePages": max(1, min(4, (max_results + 24) // 25)),
        # HarvestAPI discovers work/personal emails during profile scrape.
        "profileScraperMode": HARVESTAPI_PROFILE_SCRAPER_MODE,
    }
    locations = resolve_locations_from_job(job)
    if locations:
        payload["locations"] = locations
    elif (cfg.get("apify_default_geocode") or "").startswith("in:"):
        payload["locations"] = ["India"]
    return payload


def is_placeholder_email(email: Any) -> bool:
    e = str(email or "").strip().lower()
    if not e or "@" not in e:
        return True
    return any(e.endswith(suffix) for suffix in PLACEHOLDER_EMAIL_SUFFIXES)


def is_real_email(email: Any) -> bool:
    e = str(email or "").strip()
    return bool(e) and "@" in e and not is_placeholder_email(e)


def extract_email_from_item(item: Dict[str, Any]) -> Optional[str]:
    """Pull the first usable email from HarvestAPI / enrich / email-actor shapes."""
    candidates: List[Any] = [
        item.get("email"),
        item.get("workEmail"),
        item.get("personalEmail"),
        item.get("primaryEmail"),
        item.get("emailAddress"),
    ]
    emails_field = item.get("emails")
    if isinstance(emails_field, list):
        candidates.extend(emails_field)
    elif isinstance(emails_field, str):
        candidates.append(emails_field)
    contact = item.get("contact") or item.get("contacts")
    if isinstance(contact, dict):
        candidates.extend(
            [
                contact.get("email"),
                contact.get("workEmail"),
                contact.get("personalEmail"),
            ]
        )
        nested = contact.get("emails")
        if isinstance(nested, list):
            candidates.extend(nested)
    for entry in candidates:
        if isinstance(entry, str) and entry.strip():
            value = entry.strip()
            if is_real_email(value):
                return value
        if isinstance(entry, dict):
            value = (
                entry.get("email")
                or entry.get("address")
                or entry.get("value")
                or entry.get("emailAddress")
            )
            if value and is_real_email(str(value).strip()):
                return str(value).strip()
    return None


def extract_phone_from_item(item: Dict[str, Any]) -> Optional[str]:
    candidates: List[Any] = [
        item.get("mobileNumber"),
        item.get("phone"),
        item.get("phoneNumber"),
        item.get("mobile"),
        item.get("telephone"),
    ]
    phones_field = item.get("phones") or item.get("phoneNumbers")
    if isinstance(phones_field, list):
        candidates.extend(phones_field)
    contact = item.get("contact") or item.get("contacts")
    if isinstance(contact, dict):
        candidates.extend(
            [
                contact.get("phone"),
                contact.get("mobile"),
                contact.get("mobileNumber"),
            ]
        )
    for entry in candidates:
        if isinstance(entry, str) and entry.strip():
            return entry.strip()
        if isinstance(entry, (int, float)):
            return str(entry)
        if isinstance(entry, dict):
            value = entry.get("number") or entry.get("phone") or entry.get("value")
            if value:
                return str(value).strip()
    return None


def build_email_actor_input(profile_urls: List[str], cfg: Dict[str, Any]) -> Dict[str, Any]:
    urls = [u for u in (profile_urls or []) if u]
    max_results = max(1, min(len(urls) or 1, int(cfg.get("apify_max_results_per_search") or 30)))
    return {
        "profileUrls": urls[:max_results],
        "includeEmail": True,
        "maxResults": max_results,
    }


def build_search_input(
    job: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    actor_id: Optional[str] = None,
) -> Dict[str, Any]:
    actor = actor_id or cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR
    if is_harvestapi_search_actor(actor):
        return build_harvestapi_search_input(job, cfg)
    return build_powerai_search_input(job, cfg)


def _job_reference_values(job: Dict[str, Any]) -> List[str]:
    refs: List[str] = []
    seen: set[str] = set()
    for field in ("id", "job_code", "requisition_id", "external_id"):
        v = job.get(field)
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in seen:
            seen.add(s)
            refs.append(s)
    return refs


def extract_profile_urls_from_search_items(items: List[Dict[str, Any]]) -> List[str]:
    urls: List[str] = []
    seen: set[str] = set()
    for item in items or []:
        url = (
            item.get("url")
            or item.get("profileUrl")
            or item.get("linkedinUrl")
            or item.get("linkedin_url")
        )
        if not url:
            continue
        u = str(url).strip().split("?")[0].rstrip("/")
        if not u or u in seen:
            continue
        if "linkedin.com" not in u.lower():
            continue
        seen.add(u)
        urls.append(u)
    return urls


def _skill_name_variants(name: str) -> List[str]:
    """Expand LinkedIn-style skill labels into matchable variants."""
    raw = str(name or "").strip()
    if not raw:
        return []
    variants = [raw]
    # "Python (Programming Language)" → also "Python"
    if "(" in raw and raw.endswith(")"):
        base = raw.split("(", 1)[0].strip()
        if base:
            variants.append(base)
    # "Amazon Web Services (AWS)" → also "AWS"
    if "(" in raw and ")" in raw:
        inner = raw[raw.find("(") + 1 : raw.rfind(")")].strip()
        if inner and len(inner) <= 12:
            variants.append(inner)
    return variants


def _add_skill(out: List[Dict[str, Any]], seen: set[str], name: Any) -> None:
    if not name:
        return
    for variant in _skill_name_variants(str(name)):
        key = variant.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({"skill_name": variant.strip(), "proficiency": None})


def _skills_from_enriched(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    raw_lists = [
        item.get("skills") or [],
        item.get("topSkills") or [],
        item.get("skillList") or [],
    ]
    for pos in item.get("currentPosition") or []:
        if isinstance(pos, dict):
            raw_lists.append(pos.get("skills") or [])
    for exp in item.get("experiences") or item.get("experience") or item.get("positions") or []:
        if isinstance(exp, dict):
            raw_lists.append(exp.get("skills") or [])
    for raw in raw_lists:
        if isinstance(raw, str) and raw.strip():
            # HarvestAPI sometimes returns comma/pipe-separated skill strings
            parts = [p.strip() for p in re.split(r"[,|/•·;]", raw) if p.strip()]
            for part in parts or [raw.strip()]:
                _add_skill(out, seen, part)
            continue
        if not isinstance(raw, list):
            continue
        for s in raw:
            if isinstance(s, str) and s.strip():
                _add_skill(out, seen, s)
            elif isinstance(s, dict):
                _add_skill(
                    out,
                    seen,
                    s.get("title") or s.get("skill_name") or s.get("name") or s.get("label"),
                )

    # Infer skills from headline / about when structured skills are thin
    if len(out) < 3:
        blob = " | ".join(
            str(x)
            for x in [
                item.get("headline"),
                item.get("jobTitle"),
                item.get("title"),
                item.get("summary"),
                item.get("about"),
            ]
            if x
        )
        stop = {
            "senior",
            "junior",
            "lead",
            "manager",
            "engineer",
            "developer",
            "specialist",
            "analyst",
            "consultant",
            "architect",
            "at",
            "the",
            "and",
            "or",
            "with",
            "for",
            "in",
            "of",
            "to",
            "a",
            "an",
        }
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+#.]{1,24}", blob):
            if _norm_token(token) in stop:
                continue
            _add_skill(out, seen, token)
    return out


def _norm_token(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _parse_year_month(value: Any) -> Optional[Tuple[int, int]]:
    if value is None:
        return None
    if isinstance(value, dict):
        year = value.get("year") or value.get("Year")
        month = value.get("month") or value.get("Month") or 1
        try:
            y = int(year)
            m = max(1, min(12, int(month or 1)))
            return y, m
        except (TypeError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    m = re.search(r"(20\d{2}|19\d{2})", text)
    if not m:
        return None
    year = int(m.group(1))
    month_match = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b",
        text,
        re.I,
    )
    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    month = month_map.get(month_match.group(1).lower()[:3], 1) if month_match else 1
    return year, month


def _experience_years_from_enriched(item: Dict[str, Any]) -> Optional[float]:
    """Estimate total years from experience date ranges when Apify omits an explicit field."""
    explicit = (
        item.get("total_experience_years")
        or item.get("yearsOfExperience")
        or item.get("years_of_experience")
        or item.get("experienceYears")
    )
    if explicit is not None:
        try:
            years = float(explicit)
            if years >= 0:
                return round(years, 1)
        except (TypeError, ValueError):
            pass

    experiences = (
        item.get("experiences")
        or item.get("experience")
        or item.get("positions")
        or item.get("currentPosition")
        or []
    )
    if not isinstance(experiences, list) or not experiences:
        return None

    now = datetime.now(timezone.utc)
    total_months = 0
    for exp in experiences:
        if not isinstance(exp, dict):
            continue
        start = _parse_year_month(
            exp.get("startDate")
            or exp.get("startedOn")
            or exp.get("start")
            or (
                exp.get("dateRange", {}).get("start")
                if isinstance(exp.get("dateRange"), dict)
                else None
            )
        )
        if start is None:
            # Some actors only give "durationInMonths" / "tenure"
            for key in ("durationInMonths", "months", "tenureMonths"):
                if exp.get(key) is not None:
                    try:
                        total_months += max(0, int(exp.get(key)))
                        break
                    except (TypeError, ValueError):
                        pass
            continue
        end = _parse_year_month(
            exp.get("endDate")
            or exp.get("endedOn")
            or exp.get("end")
            or (
                exp.get("dateRange", {}).get("end")
                if isinstance(exp.get("dateRange"), dict)
                else None
            )
        )
        if end is None and (
            exp.get("isCurrent")
            or exp.get("current")
            or not (exp.get("endDate") or exp.get("endedOn") or exp.get("end"))
        ):
            end = (now.year, now.month)
        if end is None:
            continue
        months = (end[0] - start[0]) * 12 + (end[1] - start[1])
        total_months += max(0, months)

    if total_months <= 0:
        return None
    return round(total_months / 12.0, 1)


def _resume_text_from_enriched(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    summary = item.get("summary") or item.get("about") or item.get("headline")
    if summary:
        parts.append(str(summary).strip())
    for exp in (
        item.get("experiences")
        or item.get("experience")
        or item.get("positions")
        or item.get("currentPosition")
        or []
    ):
        if not isinstance(exp, dict):
            continue
        line = " | ".join(
            str(x)
            for x in [
                exp.get("title") or exp.get("jobTitle") or exp.get("position"),
                exp.get("companyName") or exp.get("company"),
                exp.get("jobDescription") or exp.get("description"),
            ]
            if x
        )
        if line:
            parts.append(line)
    for edu in item.get("educations") or item.get("education") or []:
        if not isinstance(edu, dict):
            continue
        line = " | ".join(
            str(x)
            for x in [
                edu.get("school") or edu.get("schoolName"),
                edu.get("degree"),
                edu.get("field") or edu.get("fieldOfStudy"),
            ]
            if x
        )
        if line:
            parts.append(line)
    return "\n".join(parts).strip()


def normalize_enriched_profile(
    item: Dict[str, Any],
    *,
    job: Dict[str, Any],
    pipeline_id: str,
) -> Dict[str, Any]:
    linkedin_url = (
        item.get("linkedinUrl")
        or item.get("linkedinPublicUrl")
        or item.get("profile_url")
        or item.get("url")
        or ""
    )
    linkedin_url = str(linkedin_url).strip().split("?")[0].rstrip("/")
    full_name = (
        item.get("fullName")
        or item.get("full_name")
        or " ".join(
            x
            for x in [item.get("firstName"), item.get("lastName")]
            if x
        ).strip()
        or "LinkedIn Candidate"
    )
    email = extract_email_from_item(item)
    if not email and full_name and linkedin_url:
        slug = linkedin_url.rsplit("/", 1)[-1] or uuid.uuid4().hex[:8]
        email = f"linkedin.{slug}@apify-import.local"
    job_refs = _job_reference_values(job)
    now = _iso_now()
    location = item.get("location") or item.get("jobLocation")
    if isinstance(location, dict):
        location = (
            location.get("linkedinText")
            or (location.get("parsed") or {}).get("text")
            or location.get("text")
        )
    experience = (
        item.get("experiences")
        or item.get("experience")
        or item.get("positions")
        or item.get("currentPosition")
        or []
    )
    experience_years = _experience_years_from_enriched(item)
    phone = extract_phone_from_item(item)
    return {
        "full_name": full_name,
        "email": email,
        "phone": phone,
        "location": location,
        "headline": item.get("headline") or item.get("jobTitle") or item.get("title"),
        "skills": _skills_from_enriched(item),
        "experience": experience,
        "total_experience_years": experience_years,
        "resume_text": _resume_text_from_enriched(item),
        "source": "LINKEDIN",
        "linkedin_url": linkedin_url or None,
        "linkedin_external_job_id": job_refs[0] if job_refs else job.get("id"),
        "import_metadata": {
            "provider": "apify",
            "pipeline_id": pipeline_id,
            "scraped_at": item.get("scrapedAt") or now,
        },
        "created_at": now,
        "updated_at": now,
    }


class ApifyClient:
    def __init__(self, token: Optional[str] = None) -> None:
        self.token = token or get_apify_token()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def start_actor_run(self, actor_id: str, run_input: Dict[str, Any]) -> Dict[str, Any]:
        path = actor_api_path(actor_id)
        url = f"{APIFY_BASE_URL}/acts/{path}/runs"
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=self._headers(), json=run_input)
            resp.raise_for_status()
            body = resp.json()
            return body.get("data") or body

    async def get_run(self, run_id: str) -> Dict[str, Any]:
        url = f"{APIFY_BASE_URL}/actor-runs/{run_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers=self._headers())
            resp.raise_for_status()
            body = resp.json()
            return body.get("data") or body

    async def list_dataset_items(self, dataset_id: str, *, limit: int = 1000) -> List[Dict[str, Any]]:
        url = f"{APIFY_BASE_URL}/datasets/{dataset_id}/items"
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.get(
                url,
                headers=self._headers(),
                params={"limit": limit, "clean": "true"},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return []

    async def get_run_log_excerpt(self, run_id: str, *, max_chars: int = 400) -> str:
        url = f"{APIFY_BASE_URL}/logs/{run_id}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, headers=self._headers())
                resp.raise_for_status()
                text = resp.text or ""
                if not text:
                    return ""
                tail = text[-max_chars:]
                for line in reversed(tail.splitlines()):
                    lower = line.lower()
                    if "error" in lower or "503" in lower or "429" in lower or "failed" in lower:
                        return line.strip()[:300]
                return tail.strip().splitlines()[-1][:300] if tail.strip() else ""
        except Exception as exc:
            logger.debug("Apify log fetch failed for %s: %s", run_id, exc)
            return ""


async def ensure_apify_linkedin_defaults(db) -> None:
    """Enable Apify mode when token is present (idempotent)."""
    token = get_apify_token()
    if not token:
        return
    existing = await db[CONNECTOR_COLL].find_one({"name": CONNECTOR_NAME}, {"_id": 0}) or {}
    search_actor = existing.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR
    if search_actor in {POWERAI_SEARCH_ACTOR, "powerai~linkedin-peoples-search-scraper"}:
        search_actor = DEFAULT_SEARCH_ACTOR
    # One-time: enable email fallback (older defaults wrote False).
    if existing.get("apify_email_fallback_migrated_v1"):
        email_fallback = bool(existing.get("apify_email_fallback_enabled", True))
    else:
        email_fallback = True
    update: Dict[str, Any] = {
        "name": CONNECTOR_NAME,
        "enabled": True,
        "api_mode": "apify",
        "apify_search_actor_id": search_actor,
        "apify_enrich_actor_id": existing.get("apify_enrich_actor_id") or DEFAULT_ENRICH_ACTOR,
        "apify_email_actor_id": existing.get("apify_email_actor_id") or DEFAULT_EMAIL_ACTOR,
        "apify_max_results_per_search": existing.get("apify_max_results_per_search") or 30,
        "apify_enrich_batch_size": existing.get("apify_enrich_batch_size") or 30,
        "apify_default_geocode": existing.get("apify_default_geocode") or "in:0:0:0:0:0:0",
        "apify_email_fallback_enabled": email_fallback,
        "apify_email_fallback_migrated_v1": True,
        "updated_at": _iso_now(),
    }
    if not existing:
        update["created_at"] = _iso_now()
    await db[CONNECTOR_COLL].update_one({"name": CONNECTOR_NAME}, {"$set": update}, upsert=True)


async def get_active_pipeline_for_job(db, job_id: str, *, max_age_minutes: int = 45) -> Optional[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)).isoformat()
    doc = await db[APIFY_RUNS_COLLECTION].find_one(
        {
            "job_id": job_id,
            "status": {"$in": list(PIPELINE_ACTIVE_STATUSES)},
            "created_at": {"$gte": cutoff},
        },
        {"_id": 0},
        sort=[("created_at", -1)],
    )
    return doc


async def start_apify_pipeline_for_job(
    db,
    job: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> Dict[str, Any]:
    ok, msg = validate_apify_config(cfg)
    if not ok:
        return {"started": False, "message": msg}

    job_id = str(job.get("id") or "")
    if not job_id:
        return {"started": False, "message": "Job id missing"}

    active = await get_active_pipeline_for_job(db, job_id)
    if active:
        # EasyPanel / no-cron deploys: advance the stuck-or-running pipeline when
        # Find Matches / Search LinkedIn is clicked again.
        try:
            await process_pending_apify_pipelines(
                db,
                cfg,
                upsert_candidate,
                limit=1,
                pipeline_id=str(active.get("id") or ""),
            )
            refreshed = await db[APIFY_RUNS_COLLECTION].find_one(
                {"id": active.get("id")},
                {"_id": 0},
            )
            if refreshed:
                active = refreshed
        except Exception:
            logger.exception("Apify advance on active pipeline re-entry failed for job %s", job_id)
        return {
            "started": False,
            "message": (
                "Apify pipeline already running for this job"
                if active.get("status") in PIPELINE_ACTIVE_STATUSES
                else f"Apify pipeline status: {active.get('status')}"
            ),
            "pipeline": _public_pipeline(active),
        }

    search_actor = cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR
    search_input = build_search_input(job, cfg, actor_id=search_actor)
    client = ApifyClient()
    try:
        run = await client.start_actor_run(search_actor, search_input)
    except Exception as exc:
        logger.exception("Apify search start failed for job %s", job_id)
        return {"started": False, "message": f"Apify search failed: {exc}"}

    pipeline_id = str(uuid.uuid4())
    now = _iso_now()
    doc = {
        "id": pipeline_id,
        "job_id": job_id,
        "status": PIPELINE_SEARCH_RUNNING,
        "search_actor_id": search_actor,
        "enrich_actor_id": cfg.get("apify_enrich_actor_id") or DEFAULT_ENRICH_ACTOR,
        "email_actor_id": cfg.get("apify_email_actor_id") or DEFAULT_EMAIL_ACTOR,
        "search_input": search_input,
        "search_run_id": run.get("id"),
        "search_dataset_id": run.get("defaultDatasetId"),
        "enrich_run_id": None,
        "enrich_dataset_id": None,
        "email_run_id": None,
        "email_dataset_id": None,
        "profile_urls": [],
        "candidates_ingested": 0,
        "emails_resolved": 0,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }
    await db[APIFY_RUNS_COLLECTION].insert_one(dict(doc))
    return {"started": True, "message": "LinkedIn search started via Apify", "pipeline": _public_pipeline(doc)}


def _public_pipeline(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": doc.get("id"),
        "job_id": doc.get("job_id"),
        "status": doc.get("status"),
        "search_run_id": doc.get("search_run_id"),
        "enrich_run_id": doc.get("enrich_run_id"),
        "email_run_id": doc.get("email_run_id"),
        "profile_urls_count": len(doc.get("profile_urls") or []),
        "candidates_ingested": doc.get("candidates_ingested") or 0,
        "emails_resolved": doc.get("emails_resolved") or 0,
        "error": doc.get("error"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }


async def ingest_apify_linkedin_for_job(
    cfg: Dict[str, Any],
    job: Dict[str, Any],
    limit: int,
    db,
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Start async Apify pipeline; returns upserted rows only if pipeline already completed."""
    result = await start_apify_pipeline_for_job(db, job, cfg, upsert_candidate=upsert_candidate)
    if not result.get("started"):
        pipeline = result.get("pipeline") or {}
        if pipeline.get("status") == PIPELINE_COMPLETED:
            rows = await db.candidates.find(
                {
                    "source": "LINKEDIN",
                    "import_metadata.pipeline_id": pipeline.get("id"),
                },
                {"_id": 0},
            ).limit(limit).to_list(limit)
            return rows
        return []
    return []


async def _ingest_profile_items(
    db,
    pipeline: Dict[str, Any],
    job: Dict[str, Any],
    items: List[Dict[str, Any]],
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> int:
    ingested = 0
    for item in items:
        try:
            normalized = normalize_enriched_profile(
                item,
                job=job,
                pipeline_id=str(pipeline["id"]),
            )
            if not normalized.get("linkedin_url"):
                continue
            existing = await db.candidates.find_one(
                {"linkedin_url": normalized["linkedin_url"]},
                {"_id": 0, "id": 1},
            )
            if existing and existing.get("id"):
                normalized["id"] = existing["id"]
            await upsert_candidate(normalized)
            ingested += 1
        except Exception as exc:
            logger.error("Apify profile upsert failed: %s", exc)
    return ingested


async def _urls_needing_email_fallback(db, pipeline_id: str) -> Tuple[List[str], int]:
    """Return (urls still missing a real email, candidate_row_count for this pipeline)."""
    rows = await db.candidates.find(
        {"import_metadata.pipeline_id": pipeline_id},
        {"_id": 0, "linkedin_url": 1, "email": 1},
    ).to_list(500)
    urls: List[str] = []
    seen: set[str] = set()
    for row in rows or []:
        url = str(row.get("linkedin_url") or "").strip().rstrip("/")
        if not url or url in seen:
            continue
        if is_placeholder_email(row.get("email")):
            seen.add(url)
            urls.append(url)
    return urls, len(rows or [])


async def _try_start_email_fallback(
    db,
    pipeline: Dict[str, Any],
    cfg: Dict[str, Any],
    client: ApifyClient,
    *,
    profile_urls: Optional[List[str]] = None,
) -> bool:
    """Start khadinakbar email actor when enabled and real emails are still missing."""
    if not cfg.get("apify_email_fallback_enabled", True):
        return False
    urls, candidate_count = await _urls_needing_email_fallback(db, str(pipeline["id"]))
    # Only fall back to raw profile_urls when no candidates were stored yet.
    if not urls and candidate_count == 0 and profile_urls:
        urls = [str(u).strip().rstrip("/") for u in profile_urls if u]
    if not urls:
        return False
    email_actor = (
        pipeline.get("email_actor_id")
        or cfg.get("apify_email_actor_id")
        or DEFAULT_EMAIL_ACTOR
    )
    email_input = build_email_actor_input(urls, cfg)
    try:
        email_run = await client.start_actor_run(email_actor, email_input)
    except Exception:
        logger.exception("Apify email fallback start failed for pipeline %s", pipeline.get("id"))
        return False
    await db[APIFY_RUNS_COLLECTION].update_one(
        {"id": pipeline["id"]},
        {
            "$set": {
                "status": PIPELINE_EMAIL_RUNNING,
                "email_actor_id": email_actor,
                "email_run_id": email_run.get("id"),
                "email_dataset_id": email_run.get("defaultDatasetId"),
                "email_input": email_input,
                "profile_urls": email_input.get("profileUrls") or urls,
                "updated_at": _iso_now(),
            }
        },
    )
    return True


async def _complete_pipeline(
    db,
    pipeline: Dict[str, Any],
    *,
    candidates_ingested: Optional[int] = None,
    profile_urls: Optional[List[str]] = None,
    error: Optional[str] = None,
    set_error: bool = True,
) -> None:
    payload: Dict[str, Any] = {
        "status": PIPELINE_COMPLETED,
        "updated_at": _iso_now(),
    }
    if candidates_ingested is not None:
        payload["candidates_ingested"] = candidates_ingested
    if profile_urls is not None:
        payload["profile_urls"] = profile_urls
    if set_error:
        payload["error"] = error
    await db[APIFY_RUNS_COLLECTION].update_one({"id": pipeline["id"]}, {"$set": payload})


async def _finalize_ingest_or_email(
    db,
    pipeline: Dict[str, Any],
    cfg: Dict[str, Any],
    client: ApifyClient,
    *,
    ingested: int,
    profile_urls: Optional[List[str]] = None,
    empty_error: Optional[str] = None,
) -> None:
    set_fields: Dict[str, Any] = {
        "candidates_ingested": ingested,
        "updated_at": _iso_now(),
    }
    if profile_urls is not None:
        set_fields["profile_urls"] = profile_urls
    await db[APIFY_RUNS_COLLECTION].update_one({"id": pipeline["id"]}, {"$set": set_fields})
    pipeline = {**pipeline, **set_fields}
    if await _try_start_email_fallback(db, pipeline, cfg, client, profile_urls=profile_urls):
        return
    await _complete_pipeline(
        db,
        pipeline,
        candidates_ingested=ingested,
        profile_urls=profile_urls,
        error=None if ingested else (empty_error or "Search returned no LinkedIn profiles"),
    )


async def _advance_email_stage(
    db,
    pipeline: Dict[str, Any],
    job: Dict[str, Any],
    client: ApifyClient,
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> None:
    run_id = pipeline.get("email_run_id")
    if not run_id:
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {"$set": {"status": PIPELINE_FAILED, "error": "Missing email_run_id", "updated_at": _iso_now()}},
        )
        return
    run = await client.get_run(str(run_id))
    status = str(run.get("status") or "").upper()
    if status not in {APIFY_RUN_SUCCEEDED, *APIFY_RUN_FAILED}:
        return
    if status != APIFY_RUN_SUCCEEDED:
        # Email fallback is best-effort — keep ingested candidates and complete.
        logger.warning("Apify email run %s ended with %s; completing pipeline", run_id, status)
        await _complete_pipeline(
            db,
            pipeline,
            candidates_ingested=pipeline.get("candidates_ingested"),
            profile_urls=pipeline.get("profile_urls"),
            error=f"Email fallback {status} (profiles kept)",
        )
        return
    dataset_id = run.get("defaultDatasetId") or pipeline.get("email_dataset_id")
    items = await client.list_dataset_items(str(dataset_id)) if dataset_id else []
    resolved = 0
    for item in items or []:
        if not isinstance(item, dict):
            continue
        linkedin_url = (
            item.get("linkedinUrl")
            or item.get("profileUrl")
            or item.get("url")
            or item.get("linkedin_url")
            or ""
        )
        linkedin_url = str(linkedin_url).strip().split("?")[0].rstrip("/")
        email = extract_email_from_item(item)
        phone = extract_phone_from_item(item)
        if not linkedin_url or (not email and not phone):
            continue
        existing = await db.candidates.find_one(
            {"linkedin_url": linkedin_url},
            {"_id": 0},
        )
        if not existing:
            continue
        patch: Dict[str, Any] = {
            "id": existing.get("id"),
            "full_name": existing.get("full_name") or "LinkedIn Candidate",
            "linkedin_url": linkedin_url,
            "source": existing.get("source") or "LINKEDIN",
            "import_metadata": {
                **(existing.get("import_metadata") or {}),
                "provider": "apify",
                "pipeline_id": pipeline.get("id"),
                "email_fallback_at": _iso_now(),
            },
        }
        if email:
            patch["email"] = email
        if phone:
            patch["phone"] = phone
        try:
            await upsert_candidate(patch)
            if email:
                resolved += 1
        except Exception as exc:
            logger.error("Apify email merge failed for %s: %s", linkedin_url, exc)
    await db[APIFY_RUNS_COLLECTION].update_one(
        {"id": pipeline["id"]},
        {
            "$set": {
                "status": PIPELINE_COMPLETED,
                "emails_resolved": resolved,
                "error": None,
                "updated_at": _iso_now(),
            }
        },
    )


async def _advance_search_stage(
    db,
    pipeline: Dict[str, Any],
    cfg: Dict[str, Any],
    client: ApifyClient,
    *,
    job: Dict[str, Any],
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> None:
    run_id = pipeline.get("search_run_id")
    if not run_id:
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {"$set": {"status": PIPELINE_FAILED, "error": "Missing search_run_id", "updated_at": _iso_now()}},
        )
        return
    run = await client.get_run(str(run_id))
    status = str(run.get("status") or "").upper()
    if status not in {APIFY_RUN_SUCCEEDED, *APIFY_RUN_FAILED}:
        return
    if status != APIFY_RUN_SUCCEEDED:
        log_excerpt = await client.get_run_log_excerpt(str(run_id))
        err = f"Search run {status}"
        if log_excerpt:
            err = f"{err}: {log_excerpt}"
        if is_powerai_search_actor(pipeline.get("search_actor_id") or "") and "503" in err:
            err = (
                f"{err}. The PowerAI search actor is currently failing against LinkedIn. "
                "Switch to harvestapi/linkedin-profile-search in Admin → Integrations."
            )
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {
                "$set": {
                    "status": PIPELINE_FAILED,
                    "error": err[:500],
                    "updated_at": _iso_now(),
                }
            },
        )
        return
    dataset_id = run.get("defaultDatasetId") or pipeline.get("search_dataset_id")
    items = await client.list_dataset_items(str(dataset_id)) if dataset_id else []
    search_actor = pipeline.get("search_actor_id") or cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR

    if is_harvestapi_search_actor(search_actor):
        ingested = await _ingest_profile_items(db, pipeline, job, items, upsert_candidate)
        profile_urls = extract_profile_urls_from_search_items(items)
        await _finalize_ingest_or_email(
            db,
            pipeline,
            cfg,
            client,
            ingested=ingested,
            profile_urls=profile_urls,
            empty_error="Search returned no LinkedIn profiles",
        )
        return

    urls = extract_profile_urls_from_search_items(items)
    batch_size = int(cfg.get("apify_enrich_batch_size") or 30)
    urls = urls[:batch_size]
    if not urls:
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {
                "$set": {
                    "status": PIPELINE_COMPLETED,
                    "profile_urls": [],
                    "candidates_ingested": 0,
                    "error": "Search returned no profile URLs",
                    "updated_at": _iso_now(),
                }
            },
        )
        return
    enrich_actor = pipeline.get("enrich_actor_id") or cfg.get("apify_enrich_actor_id") or DEFAULT_ENRICH_ACTOR
    enrich_input = {"profileUrls": urls}
    enrich_run = await client.start_actor_run(enrich_actor, enrich_input)
    await db[APIFY_RUNS_COLLECTION].update_one(
        {"id": pipeline["id"]},
        {
            "$set": {
                "status": PIPELINE_ENRICH_RUNNING,
                "profile_urls": urls,
                "enrich_run_id": enrich_run.get("id"),
                "enrich_dataset_id": enrich_run.get("defaultDatasetId"),
                "updated_at": _iso_now(),
            }
        },
    )


async def _advance_enrich_stage(
    db,
    pipeline: Dict[str, Any],
    job: Dict[str, Any],
    cfg: Dict[str, Any],
    client: ApifyClient,
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> None:
    run_id = pipeline.get("enrich_run_id")
    if not run_id:
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {"$set": {"status": PIPELINE_FAILED, "error": "Missing enrich_run_id", "updated_at": _iso_now()}},
        )
        return
    run = await client.get_run(str(run_id))
    status = str(run.get("status") or "").upper()
    if status not in {APIFY_RUN_SUCCEEDED, *APIFY_RUN_FAILED}:
        return
    if status != APIFY_RUN_SUCCEEDED:
        await db[APIFY_RUNS_COLLECTION].update_one(
            {"id": pipeline["id"]},
            {
                "$set": {
                    "status": PIPELINE_FAILED,
                    "error": f"Enrich run {status}",
                    "updated_at": _iso_now(),
                }
            },
        )
        return
    dataset_id = run.get("defaultDatasetId") or pipeline.get("enrich_dataset_id")
    items = await client.list_dataset_items(str(dataset_id)) if dataset_id else []
    ingested = await _ingest_profile_items(db, pipeline, job, items, upsert_candidate)
    await _finalize_ingest_or_email(
        db,
        pipeline,
        cfg,
        client,
        ingested=ingested,
        profile_urls=pipeline.get("profile_urls") or extract_profile_urls_from_search_items(items),
    )


async def process_pending_apify_pipelines(
    db,
    cfg: Dict[str, Any],
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    *,
    limit: int = 10,
    pipeline_id: Optional[str] = None,
) -> Dict[str, Any]:
    ok, msg = validate_apify_config(cfg)
    if not ok:
        return {"processed": 0, "message": msg}
    client = ApifyClient()
    query: Dict[str, Any] = {"status": {"$in": list(PIPELINE_ACTIVE_STATUSES)}}
    if pipeline_id:
        query["id"] = pipeline_id
    pipelines = await db[APIFY_RUNS_COLLECTION].find(
        query,
        {"_id": 0},
    ).sort("created_at", 1).limit(limit).to_list(limit)
    processed = 0
    for pipeline in pipelines:
        job = await db.jobs.find_one({"id": pipeline.get("job_id")}, {"_id": 0}) or {"id": pipeline.get("job_id")}
        try:
            status = pipeline.get("status")
            if status == PIPELINE_SEARCH_RUNNING:
                await _advance_search_stage(
                    db,
                    pipeline,
                    cfg,
                    client,
                    job=job,
                    upsert_candidate=upsert_candidate,
                )
            elif status == PIPELINE_ENRICH_RUNNING:
                await _advance_enrich_stage(
                    db, pipeline, job, cfg, client, upsert_candidate
                )
            elif status == PIPELINE_EMAIL_RUNNING:
                await _advance_email_stage(db, pipeline, job, client, upsert_candidate)
            processed += 1
        except Exception as exc:
            logger.exception("Apify pipeline %s failed", pipeline.get("id"))
            await db[APIFY_RUNS_COLLECTION].update_one(
                {"id": pipeline.get("id")},
                {"$set": {"status": PIPELINE_FAILED, "error": str(exc)[:500], "updated_at": _iso_now()}},
            )
    return {"processed": processed, "message": f"Processed {processed} pipeline(s)"}


async def get_latest_pipeline_for_job(db, job_id: str) -> Optional[Dict[str, Any]]:
    doc = await db[APIFY_RUNS_COLLECTION].find_one(
        {"job_id": job_id},
        {"_id": 0},
        sort=[("created_at", -1)],
    )
    return _public_pipeline(doc) if doc else None


async def test_apify_connection(cfg: Dict[str, Any]) -> Dict[str, Any]:
    ok, msg = validate_apify_config(cfg)
    if not ok:
        return {"ok": False, "message": msg}
    client = ApifyClient()
    try:
        actor = cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR
        run = await client.start_actor_run(
            actor,
            build_search_input({"title": "software engineer", "location": "India"}, cfg, actor_id=actor),
        )
        return {
            "ok": True,
            "message": "Apify search actor started successfully (smoke test)",
            "run_id": run.get("id"),
            "search_actor_id": cfg.get("apify_search_actor_id") or DEFAULT_SEARCH_ACTOR,
            "enrich_actor_id": cfg.get("apify_enrich_actor_id") or DEFAULT_ENRICH_ACTOR,
        }
    except Exception as exc:
        return {"ok": False, "message": f"Apify test failed: {exc}"}
