#!/usr/bin/env python3
"""Wayback-Crawler für deutsche KI-Berichterstattung.

Crawlt via Wayback Machine historische Snapshots deutscher Publisher-Domains,
extrahiert Haupttext und Content-Datum, filtert nach deutscher Sprache und
KI-bezogenen Keywords und schreibt pro Domain eine JSONL-Datei.

Aufruf:
    python wayback_crawler.py --input root_domains.xlsx

Robustheitsstufen für CDX-Queries (wichtig für stark archivierte Domains
wie aerzteblatt.de, bfarm.de, rki.de):

  1. Gesamtquery (ganzer Jahresbereich) mit Paginierung via resumeKey
  2. Jahr-für-Jahr-Queries (paginiert)
  3. Halbjahres-Sub-Fallback (paginiert)
  4. Quartals-Sub-Sub-Fallback (paginiert)
  5. Monats-Sub-Sub-Sub-Fallback (paginiert)          ← NEU
  6. Letzter Ausweg: CDX mit collapse=timestamp:4     ← NEU
     (ein Eintrag pro URL pro Jahr direkt vom Server)

Paginierung via resumeKey ist die wichtigste Verbesserung: Wayback liefert
große CDX-Ergebnisse in Seiten à CDX_PAGE_SIZE Zeilen. Der Server gibt am
Ende jeder vollen Seite einen resumeKey zurück. Ohne Paginierung bricht
Wayback bei sehr großen Domains still ab oder liefert ungültiges JSON.

Siehe README.md für Details zu Output-Schema, Cache-Strategie und Resume.
"""

from __future__ import annotations

import argparse
import calendar
import gzip
import hashlib
import json
import os
import random
import re
import signal
import sys
import threading
import time
import traceback
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests
import trafilatura
try:
    from trafilatura.metadata import extract_metadata as _trafi_extract_metadata
except ImportError:
    _trafi_extract_metadata = None  # type: ignore[assignment]
from bs4 import BeautifulSoup
from langdetect import DetectorFactory, LangDetectException, detect
from loguru import logger
from requests_cache import CachedSession
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from tqdm import tqdm
from w3lib.url import canonicalize_url

DetectorFactory.seed = 0


# ============================== KONSTANTEN ===================================

EXCLUDED_PATH_SUBSTRINGS = [
    "/impressum", "/imprint",
    "/privacy", "/datenschutz",
    "/terms", "/conditions", "/legal", "/disclaimer",
    "/cookie", "/cookies",
    "/barrierefreiheit", "/accessibility",
    "/hilfe", "/help", "/faq",
    "/login", "/log-in", "/signin", "/sign-in",
    "/signup", "/sign-up", "/register", "/registration",
    "/account", "/my-account", "/user/", "/users/",
    "/cart", "/checkout", "/warenkorb",
    "/contact", "/kontakt",
    "/support", "/service", "/kundenservice",
    "/standorte", "/locations",
    "/office", "/offices",
    "/jobs", "/job/", "/job-", "/careers", "/career",
    "/karriere", "/stellen", "/stellenangebote",
    "/vacancies", "/join-us", "/work-with-us",
    "/graduates", "/talent",
    "/search", "/suche",
    "/category/", "/categories/",
    "/author/", "/authors/",
    "/profile/", "/profiles/",
    "/feed/", "/rss",
    "/newsletter", "/subscribe", "/subscription",
    "/share", "/sharing", "/social",
    "/wp-content/", "/wp-json/", "/wp-admin/",
    "/api/", "/ajax/", "/assets/", "/static/", "/_next/", "/cdn-cgi/",
]

TRACKING_PARAMS = [
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src", "yclid",
    "_ga", "igshid",
]

AI_KEYWORDS = {
    "artificial intelligence": re.compile(
        r"\b(artificial intelligence|künstliche\w*\s+intelligenz\w*)\b",
        re.IGNORECASE,
    ),
    "machine learning": re.compile(
        r"\b(machine learning|maschinelle\w*\s+lernen|maschinelles\s+lernen)\b",
        re.IGNORECASE,
    ),
    "deep learning": re.compile(r"\bdeep learning\b", re.IGNORECASE),
    "neural networks": re.compile(
        r"\b(neural networks?|neuronale\w*\s+netze\w*)\b", re.IGNORECASE
    ),
    "llm": re.compile(
        r"\b(llm|large[\-\s]?language[\-\s]?models?|"
        r"großes?\s+sprachmodell\w*|große\w*\s+sprachmodell\w*)\b",
        re.IGNORECASE,
    ),
    "medical device regulation": re.compile(
        r"\b(medical device regulation|medizinprodukte-?verordnung|mdr)\b",
        re.IGNORECASE,
    ),
    "ai act": re.compile(
        r"\b(ai[\-\s]?act|ki[\-\s]?verordnung|ki[\-\s]?gesetz)\b",
        re.IGNORECASE,
    ),
    "ai": re.compile(r"\b(ai|ki)(?=[\s\-\.,;:!?]|$)", re.IGNORECASE),
    "algorithm": re.compile(
        r"\b(algorithm\w*|algorithmus|algorithmen)\b", re.IGNORECASE
    ),
    "chatbot": re.compile(
        r"\b(chatbot\w*|chat[\-\s]?gpt|gpt[\-\s]?\d|sprachassistent\w*)\b",
        re.IGNORECASE,
    ),
    "generative ai": re.compile(
        r"\b(generative\s+(ai|ki|artificial|künstliche\w*)|"
        r"generative\w*\s+ki|genai)\b",
        re.IGNORECASE,
    ),
}

HEALTH_KEYWORDS = {
    "gesundheit": re.compile(r"\bgesundheit\w*\b", re.IGNORECASE),
    "health": re.compile(r"\bhealth\w*\b", re.IGNORECASE),
    "medizin": re.compile(r"\bmedizin\w*\b", re.IGNORECASE),
    "medical": re.compile(r"\b(medical|medicine|medicinal)\b", re.IGNORECASE),
    "arzt": re.compile(
        r"\b(arzt|ärzt\w*|doktor\w*|doctor|physician|mediziner\w*)\b",
        re.IGNORECASE,
    ),
    "hospital": re.compile(
        r"\b(krankenhaus|krankenhäuser|krankenhäusern|hospital\w*|"
        r"klinik\w*|clinic\w*|praxis|praxen)\b",
        re.IGNORECASE,
    ),
    "diagnosis": re.compile(
        r"\b(diagnos\w*|radiolog\w*|röntgen\w*|bildgebung)\b", re.IGNORECASE
    ),
    "care": re.compile(
        r"\b(pflege\w*|nurs\w*|versorgung\w*|betreuung\w*)\b", re.IGNORECASE
    ),
    "treatment": re.compile(
        r"\b(therap\w*|behandlung\w*|treatment\w*|heilung\w*)\b", re.IGNORECASE
    ),
    "patient": re.compile(r"\bpatient\w*\b", re.IGNORECASE),
    "pharma": re.compile(
        r"\b(pharma\w*|arzneimittel\w*|medikament\w*|wirkstoff\w*)\b",
        re.IGNORECASE,
    ),
}

USER_AGENT_DEFAULT = "WaybackResearchCrawler/1.0 (+mailto:research@example.com)"

CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"
SNAPSHOT_TEMPLATE = "http://web.archive.org/web/{timestamp}id_/{url}"

# Wie viele Zeilen pro CDX-Seite angefordert werden. Wayback gibt bei
# Überschreitung des Limits einen resumeKey zurück. 50 000 ist konservativ
# genug, dass auch schwache Server-Instanzen nicht abstürzen.
CDX_PAGE_SIZE = 50_000

# Maximale Gesamtzeilen pro CDX-Query (Sicherheitspuffer, falls eine Domain
# wirklich absurd viel archiviert wurde und wir nicht ewig laden wollen).
CDX_MAX_TOTAL_ROWS = 5_000_000

BODY_MAX_CHARS = 200_000
MIN_VALID_BODY_BYTES = 500
WAYBACK_ERROR_MARKERS = (
    "this page is not available on the web",
    "blocked by robots.txt",
    "the wayback machine has not archived",
)
CDX_CACHE_TTL_SECONDS = 7 * 24 * 3600  # 7 Tage

ARTICLE_JSONLD_TYPES = {
    "Article",
    "NewsArticle",
    "BlogPosting",
    "TechArticle",
    "ScholarlyArticle",
    "Report",
    "ReportageNewsArticle",
    "LiveBlogPosting",
    "AnalysisNewsArticle",
    "BackgroundNewsArticle",
    "OpinionNewsArticle",
    "ReviewNewsArticle",
}

GERMAN_PATH_MARKERS = (
    "/de",
    "/de-de",
    "/de_de",
    "/de-at",
    "/de-ch",
    "/germany",
    "/deutschland",
)


# ============================== DATACLASSES ==================================


@dataclass(frozen=True)
class DomainTarget:
    """Repräsentiert einen Eintrag aus der Input-Excel."""

    raw: str
    host: str
    path_prefix: str
    file_slug: str

    @property
    def has_path_prefix(self) -> bool:
        return bool(self.path_prefix)

    @property
    def is_de_tld(self) -> bool:
        return self.host.endswith(".de")


@dataclass(frozen=True)
class CDXRow:
    """Ein Zeileneintrag aus dem CDX-Index."""

    urlkey: str
    timestamp: str
    original: str
    mimetype: str
    statuscode: str
    digest: str
    length: str

    @property
    def capture_date(self) -> date:
        return datetime.strptime(self.timestamp[:8], "%Y%m%d").date()


@dataclass
class ParsedRecord:
    """Extrahierter und normalisierter Inhalt eines Snapshots."""

    url: str
    snapshot_timestamp: str
    wayback_url: str
    parse_success: bool
    skip_reason: Optional[str] = None
    body: Optional[str] = None
    body_truncated: bool = False
    date_iso: Optional[str] = None
    date_precision: Optional[str] = None
    date_source: Optional[str] = None
    language: Optional[str] = None
    http_last_modified: Optional[str] = None


@dataclass
class DomainStats:
    """Laufzeitstatistik pro Domain."""

    domain: str
    urls_total: int = 0
    urls_processed: int = 0
    hits: int = 0
    errors: int = 0
    skipped_no_date: int = 0
    skipped_lang: int = 0
    skipped_no_body: int = 0
    skipped_no_keywords: int = 0
    skipped_no_health: int = 0
    skipped_path: int = 0
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


@dataclass
class Config:
    """Zentrale Laufzeit-Konfiguration."""

    input_path: Path
    output_dir: Path
    resume: bool
    dry_run: bool
    verbose: bool
    rate_limit: float
    limit_urls_per_year: int
    sample_seed: int
    years: range
    user_agent: str
    cache_dir: Path = field(default_factory=lambda: Path(".cache"))
    state_dir: Path = field(default_factory=lambda: Path("state"))
    logs_dir: Path = field(default_factory=lambda: Path("logs"))

    @property
    def parsed_cache_dir(self) -> Path:
        return self.cache_dir / "parsed"

    @property
    def cdx_cache_path(self) -> Path:
        return self.cache_dir / "cdx_cache.sqlite"

    @property
    def http_cache_path(self) -> Path:
        return self.cache_dir / "http_cache.sqlite"

    @property
    def progress_path(self) -> Path:
        return self.state_dir / "progress.json"


# =============================== GLOBAL STATE ================================

_shutdown_requested = threading.Event()
_rate_limit_lock = threading.Lock()
_last_request_monotonic: list[float] = [0.0]


# ============================== LOGGING SETUP ================================


def setup_logging(cfg: Config) -> dict[str, int]:
    cfg.logs_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    console_level = "DEBUG" if cfg.verbose else "INFO"
    logger.add(
        sys.stderr,
        level=console_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}:{line}</cyan> | {message}",
    )
    logger.add(
        cfg.logs_dir / "crawler.log",
        level="INFO",
        rotation="10 MB",
        retention="14 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )

    def _make_filter(tag: str):
        def _filter(record):
            return record["extra"].get("skip") == tag
        return _filter

    sink_ids: dict[str, int] = {}
    skip_files = {
        "no_date": "no_date.log",
        "lang_skipped": "lang_skipped.log",
        "no_body": "no_body.log",
        "no_keywords": "no_keywords.log",
        "fetch_errors": "fetch_errors.log",
        "skipped_urls": "skipped_urls.log",
    }
    for tag, fname in skip_files.items():
        sink_id = logger.add(
            cfg.logs_dir / fname,
            level="DEBUG",
            filter=_make_filter(tag),
            rotation="10 MB",
            retention="30 days",
            encoding="utf-8",
            format="{time:YYYY-MM-DD HH:mm:ss} | {extra[domain]} | {extra[url]} | {message}",
        )
        sink_ids[tag] = sink_id
    return sink_ids


def log_skip(tag: str, domain: str, url: str, reason: str) -> None:
    logger.bind(skip=tag, domain=domain, url=url).info(reason)


# ============================== UTILITIES ====================================


def parse_years_range(text: str) -> range:
    m = re.fullmatch(r"\s*(\d{4})\s*-\s*(\d{4})\s*", text)
    if not m:
        raise ValueError(f"Ungültiger years-Range '{text}', erwartet z.B. '2015-2025'")
    start, end = int(m.group(1)), int(m.group(2))
    if start > end:
        raise ValueError(f"Start-Jahr {start} liegt nach End-Jahr {end}")
    return range(start, end + 1)


def rate_limit_wait(rate_limit: float) -> None:
    if rate_limit <= 0:
        return
    min_interval = 1.0 / rate_limit
    with _rate_limit_lock:
        now = time.monotonic()
        wait = min_interval - (now - _last_request_monotonic[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_monotonic[0] = time.monotonic()


def _install_signal_handlers() -> None:
    def _handler(signum, frame):  # noqa: ARG001
        if not _shutdown_requested.is_set():
            logger.warning("Signal {} empfangen, beende nach aktueller URL sauber …", signum)
            _shutdown_requested.set()
        else:
            logger.error("Zweites Signal empfangen, breche hart ab.")
            sys.exit(130)

    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, AttributeError):
        pass


# ============================== INPUT LOADING ================================


def _sanitize_slug(raw: str) -> str:
    s = raw.strip().strip("/").lower()
    s = re.sub(r"[\\/]+", "_", s)
    s = re.sub(r"[^a-z0-9._\-]+", "_", s)
    s = re.sub(r"_{2,}", "_", s).strip("_")
    return s or "unnamed"


def _parse_domain_target(raw: str) -> Optional[DomainTarget]:
    if not raw:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    s = re.sub(r"^https?://", "", s)
    s = s.strip("/")
    if not s:
        return None
    if "/" in s:
        host, _, path_rest = s.partition("/")
    else:
        host, path_rest = s, ""
    if not host or "." not in host:
        return None
    path_prefix = ""
    if path_rest:
        path_prefix = "/" + path_rest.strip("/")
    slug = _sanitize_slug(s)
    return DomainTarget(raw=s, host=host, path_prefix=path_prefix, file_slug=slug)


def load_domains(path: Path) -> list[DomainTarget]:
    if not path.exists():
        raise FileNotFoundError(f"Input-Datei nicht gefunden: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    if "domain" not in df.columns:
        raise ValueError(
            f"Pflichtspalte 'domain' fehlt in {path}. Gefundene Spalten: {list(df.columns)}"
        )
    seen: set[str] = set()
    out: list[DomainTarget] = []
    for raw in df["domain"].tolist():
        if pd.isna(raw):
            continue
        target = _parse_domain_target(str(raw))
        if target is None:
            continue
        if target.raw in seen:
            continue
        seen.add(target.raw)
        out.append(target)
    return out


# ============================== URL HANDLING =================================


def _url_path_starts_with_prefix(url: str, prefix: str) -> bool:
    if not prefix:
        return True
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    path = (parsed.path or "").lower()
    pfx = prefix.lower()
    if not path.startswith(pfx):
        return False
    tail = path[len(pfx):]
    if tail == "" or tail.startswith("/"):
        return True
    return False


def _url_matches_target(url: str, target: DomainTarget) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    t_host = target.host.lower()
    if host != t_host and not host.endswith("." + t_host):
        return False
    if target.has_path_prefix:
        return _url_path_starts_with_prefix(url, target.path_prefix)
    return True


def _url_has_german_marker(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    path = (parsed.path or "").lower()
    for marker in GERMAN_PATH_MARKERS:
        if path == marker or path.startswith(marker + "/"):
            return True
    return False


def _host_has_de_tld(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    return host.endswith(".de")


def is_excluded_path(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return True
    path = (parsed.path or "").lower()
    for sub in EXCLUDED_PATH_SUBSTRINGS:
        if sub.lower() in path:
            return True
    return False


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.query:
        params = parse_qsl(parsed.query, keep_blank_values=True)
        tracking_lower = {p.lower() for p in TRACKING_PARAMS}
        filtered = [(k, v) for k, v in params if k.lower() not in tracking_lower]
        new_query = urlencode(filtered, doseq=True)
        parsed = parsed._replace(query=new_query)
    parsed = parsed._replace(fragment="")
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        parsed = parsed._replace(path=parsed.path.rstrip("/"))
    rebuilt = urlunparse(parsed)
    return canonicalize_url(rebuilt)


# ============================== CDX FETCH ====================================


class WaybackErrorPageException(Exception):
    pass


class CDXFetchError(Exception):
    """Ein CDX-Request hat keine verwertbare Antwort geliefert."""


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.exceptions.ChunkedEncodingError):
        return True
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        if resp is not None and resp.status_code in (429, 500, 502, 503, 504):
            return True
    return False


_retry_decorator = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=32),
    retry=retry_if_exception(_should_retry),
)

# Aggressivere Retries speziell für CDX: bis zu 8 Versuche, bis zu 120s Warten.
_cdx_retry_decorator = retry(
    reraise=True,
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=4, max=120),
    retry=retry_if_exception(_should_retry),
)


def _cdx_file_cache_path(cfg: Config, target: DomainTarget, years: range) -> Path:
    years_key = f"{years[0]}-{years[-1]}"
    return cfg.cache_dir / "cdx" / f"{target.file_slug}__{years_key}.json.gz"


def _read_cdx_file_cache(path: Path, ttl_seconds: int) -> Optional[list[CDXRow]]:
    if not path.exists():
        return None
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if age > ttl_seconds:
        logger.debug("CDX-Cache {} abgelaufen ({:.1f}d alt)", path.name, age / 86400.0)
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return [CDXRow(**row) for row in data]
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("CDX-Cache {} defekt, ignoriere: {}", path, exc)
        return None


def _write_cdx_file_cache(path: Path, rows: list[CDXRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump([asdict(r) for r in rows], f, ensure_ascii=False)
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning("CDX-Cache {} konnte nicht geschrieben werden: {}", path, exc)
        try:
            tmp.unlink()
        except OSError:
            pass


def _parse_cdx_rows_from_data(data: list, label: str) -> tuple[list[CDXRow], Optional[str]]:
    """Parst eine Liste von CDX-JSON-Zeilen.

    Der Wayback CDX-Server hängt bei paginierter Ausgabe eine letzte Zeile
    mit dem resumeKey an — erkennbar daran, dass die Zeile nur ein Element
    (den Key-String) enthält statt der üblichen 7 Felder.

    Returns:
        Tupel (rows, resume_key). resume_key ist None, wenn keine weiteren
        Seiten existieren.
    """
    if not data:
        return [], None

    # Erste Zeile ist immer der Header (fieldnames), überspringen.
    rows_raw = data[1:]
    resume_key: Optional[str] = None

    # Letztes Element: resumeKey-Sentinel prüfen.
    # Wayback liefert entweder ["resumeKey", "<value>"] (2-Element-Liste)
    # oder direkt den Key als einfachen String — beides abfangen.
    if rows_raw:
        last = rows_raw[-1]
        if isinstance(last, list) and len(last) == 1 and isinstance(last[0], str):
            # Einzel-Element-Liste → resumeKey-Wert
            resume_key = last[0]
            rows_raw = rows_raw[:-1]
        elif isinstance(last, list) and len(last) == 2 and last[0] == "resumeKey":
            resume_key = str(last[1])
            rows_raw = rows_raw[:-1]

    out: list[CDXRow] = []
    for row in rows_raw:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            out.append(CDXRow(
                urlkey=str(row[0]),
                timestamp=str(row[1]),
                original=str(row[2]),
                mimetype=str(row[3]),
                statuscode=str(row[4]),
                digest=str(row[5]),
                length=str(row[6]) if len(row) > 6 else "",
            ))
        except (TypeError, ValueError):
            continue
    return out, resume_key


def _fetch_cdx_paginated(
    cdx_url: str,
    match_type: str,
    from_ts: str,
    to_ts: str,
    session: requests.Session,
    rate_limit: float,
    label: str,
    page_size: int = CDX_PAGE_SIZE,
    collapse: Optional[str] = None,
) -> list[CDXRow]:
    """Holt CDX-Ergebnisse mit Paginierung via resumeKey.

    Wayback gibt bei limit-basierten Queries am Ende jeder vollen Seite einen
    resumeKey zurück. Diese Funktion iteriert automatisch alle Seiten.

    Args:
        cdx_url: Der ``url``-Parameter für CDX.
        match_type: ``"domain"`` oder ``"prefix"``.
        from_ts: Start-Timestamp (YYYYMMDD).
        to_ts: End-Timestamp (YYYYMMDD).
        session: Plain requests.Session.
        rate_limit: Rate-Limit in req/s.
        label: Nur fürs Logging.
        page_size: Zeilen pro Seite.
        collapse: Optionales CDX ``collapse``-Feld, z.B. ``"timestamp:4"``.

    Returns:
        Alle CDXRow über alle Seiten gesammelt.

    Raises:
        CDXFetchError: Bei nicht behebbarem Fehler.
    """
    all_rows: list[CDXRow] = []
    resume_key: Optional[str] = None
    page_num = 0

    while True:
        if len(all_rows) >= CDX_MAX_TOTAL_ROWS:
            logger.warning(
                "CDX {} hat CDX_MAX_TOTAL_ROWS={} erreicht, breche ab.",
                label, CDX_MAX_TOTAL_ROWS,
            )
            break

        params: list[tuple[str, str]] = [
            ("url", cdx_url),
            ("matchType", match_type),
            ("from", from_ts),
            ("to", to_ts),
            ("filter", "statuscode:200"),
            ("filter", "mimetype:text/html"),
            ("output", "json"),
            ("limit", str(page_size)),
        ]
        if collapse:
            params.append(("collapse", collapse))
        if resume_key:
            params.append(("resumeKey", resume_key))

        # Closure über aktuelle params-Kopie, damit der Retry korrekt funktioniert.
        current_params = list(params)

        @_cdx_retry_decorator
        def _do_page_request(p=current_params) -> bytes:
            rate_limit_wait(rate_limit)
            r = session.get(
                CDX_ENDPOINT,
                params=p,
                headers={"Accept-Encoding": "identity"},
                timeout=(30, 300),
                stream=True,
            )
            if r.status_code in (429, 500, 502, 503, 504):
                r.raise_for_status()
            if r.status_code != 200:
                return b""
            chunks: list[bytes] = []
            try:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)
            finally:
                try:
                    r.close()
                except Exception:
                    pass
            return b"".join(chunks)

        try:
            body = _do_page_request()
        except Exception as exc:
            raise CDXFetchError(
                f"Request für {label} Seite {page_num} fehlgeschlagen: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        if not body:
            # Leere Response = Ende der Daten oder Fehler.
            logger.debug("CDX {} Seite {}: leere Response, stoppe Paginierung.", label, page_num)
            break

        try:
            data = json.loads(body.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError) as exc:
            try:
                preview = body[:500].decode("utf-8", errors="replace").replace("\n", " ")
            except Exception:
                preview = "<unreadable>"
            raise CDXFetchError(
                f"Non-JSON-Response für {label} Seite {page_num} "
                f"({len(body)} bytes). Preview: {preview!r}"
            ) from exc

        if not isinstance(data, list) or len(data) <= 1:
            # Nur Header-Zeile oder leer → Ende.
            logger.debug("CDX {} Seite {}: keine Datenzeilen, stoppe.", label, page_num)
            break

        page_rows, new_resume_key = _parse_cdx_rows_from_data(data, label)
        all_rows.extend(page_rows)
        page_num += 1

        logger.debug(
            "CDX {} Seite {}: {} Zeilen geladen (gesamt {}), resumeKey={}",
            label, page_num, len(page_rows), len(all_rows),
            "ja" if new_resume_key else "nein",
        )

        if new_resume_key is None:
            # Letzte Seite erreicht.
            break

        resume_key = new_resume_key

        # Kleines Extra-Delay zwischen Seiten: Wayback-Infrastruktur schonen.
        # Bei großen Domains (aerzteblatt.de, bfarm.de) führt zu schnelles
        # Paginieren zu 503-Kaskaden.
        jitter = random.uniform(0.5, 1.5)
        time.sleep(jitter)

    return all_rows


def _month_ranges_for_year(year: int) -> list[tuple[str, str]]:
    """Gibt 12 (from_ts, to_ts)-Paare für alle Monate eines Jahres zurück."""
    result: list[tuple[str, str]] = []
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        result.append((f"{year}{month:02d}01", f"{year}{month:02d}{last_day:02d}"))
    return result


def _fetch_cdx_range_with_fallbacks(
    cdx_url: str,
    match_type: str,
    from_ts: str,
    to_ts: str,
    session: requests.Session,
    rate_limit: float,
    label: str,
    depth: int = 0,
    max_depth: int = 4,
) -> tuple[list[CDXRow], bool]:
    """Versucht CDX-Fetch für einen Zeitraum, splittet bei Bedarf rekursiv.

    Fallback-Hierarchie bei Fehlschlag:
        depth=0 → ganzen Zeitraum via Paginierung
        depth=1 → Jahre
        depth=2 → Halbjahre
        depth=3 → Quartale
        depth=4 → Monate (kein weiteres Splitting)

    Returns:
        Tupel (rows, ok). ok=False wenn ALLE Sub-Queries fehlschlagen.
    """
    # Versuche zunächst paginiert.
    try:
        rows = _fetch_cdx_paginated(
            cdx_url, match_type, from_ts, to_ts,
            session, rate_limit, label,
        )
        return rows, True
    except CDXFetchError as exc:
        if depth >= max_depth:
            logger.warning(
                "CDX {} [{}..{}] Tiefe {} erreicht, aufgegeben: {}",
                label, from_ts, to_ts, depth, exc,
            )
            return [], False
        logger.warning(
            "CDX {} [{}..{}] Tiefe {}: Fehler ({}), versuche Split.",
            label, from_ts, to_ts, depth, exc,
        )

    # Zeitraum in zwei Hälften teilen.
    try:
        start = datetime.strptime(from_ts[:8], "%Y%m%d")
        end = datetime.strptime(to_ts[:8], "%Y%m%d")
        mid = start + (end - start) / 2
        mid_ts = mid.strftime("%Y%m%d")
        # mid_ts darf nicht gleich from_ts oder to_ts sein.
        if mid_ts <= from_ts or mid_ts >= to_ts:
            # Zeitraum zu klein zum Splitten → aufgeben.
            logger.warning(
                "CDX {} [{}..{}]: Zeitraum zu klein für Split, aufgegeben.",
                label, from_ts, to_ts,
            )
            return [], False
    except ValueError:
        return [], False

    # Einen Tag vor mid als to_ts der ersten Hälfte berechnen.
    mid_date = datetime.strptime(mid_ts, "%Y%m%d").date()
    # erste Hälfte: from_ts .. mid_ts-1
    prev_day = (mid_date.replace(day=mid_date.day) - 
                __import__("datetime").timedelta(days=1))
    first_to = prev_day.strftime("%Y%m%d")
    second_from = mid_ts

    combined: list[CDXRow] = []
    any_ok = False

    for (f, t), sub_label in [
        ((from_ts, first_to), f"{label}[H1]"),
        ((second_from, to_ts), f"{label}[H2]"),
    ]:
        if f > t:
            continue
        rows, ok = _fetch_cdx_range_with_fallbacks(
            cdx_url, match_type, f, t,
            session, rate_limit, sub_label,
            depth=depth + 1, max_depth=max_depth,
        )
        combined.extend(rows)
        if ok or rows:
            any_ok = True

    return combined, any_ok


def fetch_cdx_index(
    target: DomainTarget,
    session: requests.Session,
    years: range,
    rate_limit: float,
    cfg: Config,
) -> list[CDXRow]:
    """Holt den CDX-Index für eine Domain über den angegebenen Jahresbereich.

    Strategie (Robustheitsstufen):

    1. **File-Cache-Check** (7 Tage TTL). Bei Hit: zurück aus Cache.
    2. **Paginierte Gesamtquery** über kompletten Jahresbereich.
       Klappt das → Ergebnis cachen und zurückgeben.
    3. **Jahr-für-Jahr-Fallback** (paginiert), falls Gesamtquery leer oder
       fehlerhaft. Pro Jahr ggf. Halbjahres- → Quartals- → Monats-Split.
    4. **collapse=timestamp:4-Notfallmodus**: Falls selbst monatliche Queries
       für manche Jahre fehlschlagen, wird eine sehr kompakte CDX-Query mit
       ``collapse=timestamp:4`` versucht. Das liefert nur einen Eintrag pro
       URL pro Jahr (den zeitlich ersten), verliert also Datums-Granularität,
       ist aber extrem zuverlässig auch für stark archivierte Domains.

    Paginierung: Alle CDX-Requests nutzen ``limit=CDX_PAGE_SIZE`` und
    iterieren den von Wayback zurückgegebenen ``resumeKey`` bis zum Ende.
    Damit werden auch Domains mit Millionen von archivierten URLs sauber
    geladen, ohne dass ein einzelner Request die Server-Infrastruktur
    überlastet.

    Args:
        target: Die zu crawlende Domain-Spezifikation.
        session: Plain ``requests.Session``.
        years: Jahresbereich als ``range``.
        rate_limit: Rate-Limit in Requests/s.
        cfg: Konfiguration (für Cache-Pfad).

    Returns:
        Liste von ``CDXRow``-Instanzen. Leer, wenn Wayback tatsächlich nichts hat.

    Raises:
        CDXFetchError: Wenn alle Strategien für alle Jahre fehlschlagen.
    """
    # 1) File-Cache-Check
    cache_path = _cdx_file_cache_path(cfg, target, years)
    cached = _read_cdx_file_cache(cache_path, CDX_CACHE_TTL_SECONDS)
    if cached is not None:
        logger.info(
            "CDX für {}: {} Zeilen aus File-Cache ({})",
            target.raw, len(cached), cache_path.name,
        )
        return cached

    if target.has_path_prefix:
        cdx_url = f"{target.host}{target.path_prefix}/"
        match_type = "prefix"
    else:
        cdx_url = target.host
        match_type = "domain"

    start_ts = f"{years[0]}0101"
    end_ts = f"{years[-1]}1231"

    # 2) Paginierte Gesamtquery
    logger.info("CDX {} Gesamtquery (paginiert) [{}-{}] …", target.raw, years[0], years[-1])
    try:
        rows = _fetch_cdx_paginated(
            cdx_url, match_type, start_ts, end_ts,
            session, rate_limit,
            label=f"{target.raw}[gesamt]",
        )
        if rows:
            logger.info("CDX für {}: {} Rohzeilen (Gesamtquery)", target.raw, len(rows))
            _write_cdx_file_cache(cache_path, rows)
            return rows
        logger.warning(
            "CDX-Gesamtquery für {} lieferte 0 Zeilen, versuche Jahr-für-Jahr.",
            target.raw,
        )
    except CDXFetchError as exc:
        logger.warning(
            "CDX-Gesamtquery für {} fehlgeschlagen ({}), versuche Jahr-für-Jahr.",
            target.raw, exc,
        )

    # 3) Jahr-für-Jahr mit rekursivem Split (Halbjahr → Quartal → Monat)
    combined: list[CDXRow] = []
    years_ok = 0
    years_failed: list[int] = []

    for year in years:
        year_label = f"{target.raw}[{year}]"
        logger.info("CDX {} Jahresquery {} (paginiert, bis Monat-Split) …", target.raw, year)
        year_rows, ok = _fetch_cdx_range_with_fallbacks(
            cdx_url, match_type,
            f"{year}0101", f"{year}1231",
            session, rate_limit,
            label=year_label,
            depth=0,
            # depth=0: versucht Gesamtjahr
            # depth=1: H1/H2
            # depth=2: Quartale
            # depth=3: Monate
            # depth=4: Wochen (durch binären Split)
            max_depth=4,
        )
        if ok or year_rows:
            combined.extend(year_rows)
            years_ok += 1
            logger.info(
                "CDX {} Jahr {}: {} Zeilen (rekursiver Split erfolgreich)",
                target.raw, year, len(year_rows),
            )
        else:
            logger.warning("CDX {} Jahr {}: ALLE Sub-Queries fehlgeschlagen.", target.raw, year)
            years_failed.append(year)

    # 4) Notfallmodus: collapse=timestamp:4 für fehlgeschlagene Jahre
    #    Dieser Modus liefert nur einen Capture pro URL pro Jahr (den ersten),
    #    was die Datums-Granularität leicht einschränkt, aber extrem
    #    zuverlässig ist. Die Qualität des Outputs bleibt hoch, weil der
    #    downstream-Code in group_captures_by_url_and_year die beste
    #    Annäherung an Mitte-Jahr wählt — hier gibt es zwar nur eine Option,
    #    aber es ist besser als gar keine Daten.
    if years_failed:
        logger.info(
            "CDX {} Notfallmodus (collapse=timestamp:4) für {} fehlgeschlagene Jahre: {}",
            target.raw, len(years_failed), years_failed,
        )
        for year in years_failed:
            collapse_label = f"{target.raw}[{year}:collapse]"
            try:
                collapse_rows = _fetch_cdx_paginated(
                    cdx_url, match_type,
                    f"{year}0101", f"{year}1231",
                    session, rate_limit,
                    label=collapse_label,
                    collapse="timestamp:4",
                )
                if collapse_rows:
                    combined.extend(collapse_rows)
                    years_ok += 1
                    years_failed.remove(year)
                    logger.info(
                        "CDX {} Jahr {} via collapse-Notfallmodus: {} Zeilen",
                        target.raw, year, len(collapse_rows),
                    )
                else:
                    logger.warning(
                        "CDX {} Jahr {} collapse-Notfallmodus lieferte 0 Zeilen.",
                        target.raw, year,
                    )
            except CDXFetchError as exc:
                logger.error(
                    "CDX {} Jahr {} collapse-Notfallmodus fehlgeschlagen: {}",
                    target.raw, year, exc,
                )

    logger.info(
        "CDX für {}: {} Rohzeilen (Jahr-für-Jahr: {}/{} Jahre ok, {} endgültig fehlgeschlagen)",
        target.raw, len(combined), years_ok, len(list(years)), len(years_failed),
    )

    if years_ok == 0 and not combined:
        raise CDXFetchError(
            f"Alle CDX-Strategien für {target.raw} fehlgeschlagen "
            f"({len(list(years))} Jahre, inkl. collapse-Notfallmodus)"
        )

    # Cache schreiben, wenn nicht zu unvollständig.
    if years_failed and len(years_failed) > len(list(years)) / 2:
        logger.warning(
            "CDX-Ergebnis für {} unvollständig ({}/{} Jahre), wird NICHT gecacht.",
            target.raw, years_ok, len(list(years)),
        )
    elif combined:
        _write_cdx_file_cache(cache_path, combined)

    return combined


# =============================== SAMPLING ====================================


def pick_closest_to_july2(captures: list[CDXRow], year: int) -> CDXRow:
    target = date(year, 7, 2)

    def _key(row: CDXRow) -> tuple[int, str]:
        delta = abs((row.capture_date - target).days)
        return (delta, row.timestamp)

    return min(captures, key=_key)


def group_captures_by_url_and_year(
    rows: list[CDXRow], years: range, target: DomainTarget
) -> dict[str, dict[int, CDXRow]]:
    year_set = set(years)
    bucket: dict[str, dict[int, list[CDXRow]]] = {}
    for row in rows:
        try:
            year = int(row.timestamp[:4])
        except ValueError:
            continue
        if year not in year_set:
            continue
        orig = row.original
        parsed = urlparse(orig)
        if parsed.scheme not in ("http", "https"):
            continue
        if is_excluded_path(orig):
            continue
        if not _url_matches_target(orig, target):
            continue
        try:
            norm = normalize_url(orig)
        except Exception:
            continue
        if not _url_matches_target(norm, target):
            continue
        bucket.setdefault(norm, {}).setdefault(year, []).append(row)

    result: dict[str, dict[int, CDXRow]] = {}
    for url, year_map in bucket.items():
        per_year: dict[int, CDXRow] = {}
        for year, caps in year_map.items():
            per_year[year] = pick_closest_to_july2(caps, year)
        result[url] = per_year
    return result


def sample_urls_per_year(
    url_year_map: dict[str, dict[int, CDXRow]],
    limit_per_year: int,
    seed: int,
) -> list[tuple[str, int, CDXRow]]:
    rng = random.Random(seed)
    per_year: dict[int, list[tuple[str, CDXRow]]] = {}
    for url, year_map in url_year_map.items():
        for year, row in year_map.items():
            per_year.setdefault(year, []).append((url, row))

    out: list[tuple[str, int, CDXRow]] = []
    for year in sorted(per_year.keys()):
        bucket = per_year[year]
        bucket.sort(key=lambda x: x[0])
        if len(bucket) > limit_per_year:
            chosen = rng.sample(bucket, limit_per_year)
            chosen.sort(key=lambda x: x[0])
        else:
            chosen = bucket
        for url, row in chosen:
            out.append((url, year, row))
    return out


# =============================== SNAPSHOT FETCH ==============================


def is_valid_wayback_response(html: str) -> bool:
    if not html:
        return False
    if len(html.encode("utf-8", errors="ignore")) < MIN_VALID_BODY_BYTES:
        return False
    lowered = html.lower()
    for marker in WAYBACK_ERROR_MARKERS:
        if marker in lowered:
            return False
    return True


def fetch_snapshot(
    timestamp: str,
    url: str,
    session: CachedSession,
    rate_limit: float,
) -> Optional[tuple[str, dict[str, str]]]:
    fetch_url = SNAPSHOT_TEMPLATE.format(timestamp=timestamp, url=url)

    @_retry_decorator
    def _do_request() -> requests.Response:
        cache_hit = False
        cache = getattr(session, "cache", None)
        if cache is not None:
            try:
                prepared = requests.Request("GET", fetch_url).prepare()
                cache_key = session.cache.create_key(prepared)  # type: ignore[attr-defined]
                cache_hit = session.cache.contains(key=cache_key)  # type: ignore[attr-defined]
            except Exception:
                cache_hit = False
        if not cache_hit:
            rate_limit_wait(rate_limit)
        r = session.get(fetch_url, timeout=(30, 60))
        if r.status_code in (429, 500, 502, 503, 504):
            r.raise_for_status()
        return r

    resp = _do_request()

    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        logger.warning("Snapshot {} lieferte Status {}", fetch_url, resp.status_code)
        return None

    html = resp.text
    if not is_valid_wayback_response(html):
        return None

    headers = {k: v for k, v in resp.headers.items()}
    return html, headers


# =============================== EXTRACTION ==================================


def _normalize_body(body: str) -> tuple[str, bool]:
    if not body:
        return "", False
    nb = unicodedata.normalize("NFKC", body)
    nb = re.sub(r"[ \t]+", " ", nb)
    nb = re.sub(r"\n{3,}", "\n\n", nb)
    nb = re.sub(r" *\n *", "\n", nb)
    nb = nb.strip()
    truncated = False
    if len(nb) > BODY_MAX_CHARS:
        cut = nb.rfind(" ", 0, BODY_MAX_CHARS)
        if cut < BODY_MAX_CHARS // 2:
            cut = BODY_MAX_CHARS
        nb = nb[:cut].rstrip()
        truncated = True
    return nb, truncated


def extract_text(html: str) -> Optional[str]:
    try:
        body = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
            deduplicate=True,
        )
    except Exception as exc:
        logger.debug("trafilatura.extract warf Exception: {}", exc)
        body = None
    if body and body.strip():
        return body

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return None
    for tag in soup(["script", "style", "noscript", "nav", "header", "footer", "aside"]):
        tag.decompose()
    container = soup.find("article") or soup.find("main") or soup.find("body")
    if container is None:
        return None
    text = container.get_text(separator="\n", strip=True)
    return text or None


def _iter_jsonld_dicts(soup: BeautifulSoup):
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string
        if not raw:
            raw = script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            try:
                cleaned = re.sub(r"^<!--|-->$", "", raw).strip()
                data = json.loads(cleaned)
            except (json.JSONDecodeError, ValueError):
                continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
            elif isinstance(item, dict):
                yield item
                if "@graph" in item and isinstance(item["@graph"], list):
                    stack.extend(item["@graph"])


def _matches_article_type(obj: dict) -> bool:
    t = obj.get("@type")
    if isinstance(t, list):
        return any(isinstance(x, str) and x in ARTICLE_JSONLD_TYPES for x in t)
    if isinstance(t, str):
        return t in ARTICLE_JSONLD_TYPES
    return False


def extract_metadata(html: str) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return out

    for obj in _iter_jsonld_dicts(soup):
        if not _matches_article_type(obj):
            continue
        if "jsonld_datePublished" not in out and obj.get("datePublished"):
            out["jsonld_datePublished"] = str(obj["datePublished"])
        if "jsonld_dateModified" not in out and obj.get("dateModified"):
            out["jsonld_dateModified"] = str(obj["dateModified"])

    def _meta(attrs: dict) -> Optional[str]:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            return tag["content"].strip()
        return None

    mp = _meta({"property": "article:published_time"})
    if mp:
        out["meta_article_published_time"] = mp
    mm = _meta({"property": "article:modified_time"})
    if mm:
        out["meta_article_modified_time"] = mm

    for name in ("pubdate", "publishdate", "date", "DC.date.issued"):
        v = _meta({"name": re.compile(rf"^{re.escape(name)}$", re.IGNORECASE)})
        if v:
            out.setdefault("meta_pubdate", v)
            break

    lm = _meta({"name": re.compile(r"^last-modified$", re.IGNORECASE)})
    if lm:
        out["meta_last_modified"] = lm

    if _trafi_extract_metadata is not None:
        try:
            meta_obj = _trafi_extract_metadata(html)
            if meta_obj is not None:
                md = getattr(meta_obj, "date", None)
                if md is None and isinstance(meta_obj, dict):
                    md = meta_obj.get("date")
                if md:
                    out["trafilatura"] = str(md)
        except Exception as exc:
            logger.debug("trafilatura metadata extract failed: {}", exc)

    container = soup.find("article") or soup.find("main") or soup
    t = container.find("time") if container else None
    if t and t.get("datetime"):
        out["time_tag"] = t["datetime"].strip()

    return out


# ============================== DATE RESOLUTION ==============================


_DATE_PATTERNS = [
    (re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$"), "day"),
    (re.compile(r"^(\d{4})/(\d{2})/(\d{2})"), "day"),
    (re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})"), "day_de"),
    (re.compile(r"^(\d{4})-(\d{2})$"), "month"),
    (re.compile(r"^(\d{4})$"), "year"),
    (
        re.compile(
            r"^[A-Za-z]{3},\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})", re.IGNORECASE
        ),
        "rfc1123",
    ),
]

_RFC_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date_string(raw: str) -> Optional[tuple[date, str]]:
    if not raw:
        return None
    s = raw.strip()
    for pattern, kind in _DATE_PATTERNS:
        m = pattern.match(s)
        if not m:
            continue
        try:
            if kind == "day":
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3))), "day"
            if kind == "day_de":
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), "day"
            if kind == "month":
                return date(int(m.group(1)), int(m.group(2)), 1), "month"
            if kind == "year":
                return date(int(m.group(1)), 1, 1), "year"
            if kind == "rfc1123":
                day = int(m.group(1))
                mon = _RFC_MONTHS.get(m.group(2).lower())
                if mon is None:
                    continue
                year = int(m.group(3))
                return date(year, mon, day), "day"
        except (ValueError, IndexError):
            continue
    try:
        core = s
        if len(core) >= 10:
            d = datetime.fromisoformat(core[:10])
            return d.date(), "day"
    except ValueError:
        pass
    return None


def resolve_publication_date(
    meta: dict[str, str], headers: dict[str, str]
) -> Optional[tuple[date, str, str]]:
    published_candidates = [
        ("jsonld_datePublished", meta.get("jsonld_datePublished")),
        ("meta_article_published_time", meta.get("meta_article_published_time")),
        ("meta_pubdate", meta.get("meta_pubdate")),
        ("trafilatura", meta.get("trafilatura")),
        ("time_tag", meta.get("time_tag")),
    ]
    modified_candidates = [
        ("jsonld_dateModified", meta.get("jsonld_dateModified")),
        ("meta_article_modified_time", meta.get("meta_article_modified_time")),
        ("meta_last_modified", meta.get("meta_last_modified")),
        (
            "http_last_modified",
            headers.get("Last-Modified") or headers.get("X-Archive-Orig-Last-Modified"),
        ),
    ]

    published: Optional[tuple[date, str, str]] = None
    for label, raw in published_candidates:
        if not raw:
            continue
        parsed = _parse_date_string(raw)
        if parsed is not None:
            published = (parsed[0], parsed[1], label)
            break

    modified: Optional[tuple[date, str, str]] = None
    for label, raw in modified_candidates:
        if not raw:
            continue
        parsed = _parse_date_string(raw)
        if parsed is not None:
            modified = (parsed[0], parsed[1], label)
            break

    if not published and not modified:
        return None
    if published and modified:
        if modified[0] > published[0]:
            return modified
        return published
    if modified:
        return modified
    return published


# ============================== LANGUAGE + KEYWORDS ==========================


def detect_language_safe(body: str, url: str) -> str:
    if len(body) >= 200:
        try:
            return detect(body)
        except LangDetectException:
            return "lang_error"
    if _host_has_de_tld(url) or _url_has_german_marker(url):
        return "de_assumed"
    return "too_short_unknown"


def find_ai_keywords(body: str) -> list[str]:
    hits: list[str] = []
    for key, pattern in AI_KEYWORDS.items():
        if pattern.search(body):
            hits.append(key)
    return hits


def find_health_keywords(body: str) -> list[str]:
    hits: list[str] = []
    for key, pattern in HEALTH_KEYWORDS.items():
        if pattern.search(body):
            hits.append(key)
    return hits


def find_keywords(body: str) -> list[str]:
    """Deprecated: nutze find_ai_keywords."""
    return find_ai_keywords(body)


# ============================== PARSED CACHE =================================


def _parsed_cache_key(url: str, timestamp: str) -> str:
    h = hashlib.sha1(f"{timestamp}|{url}".encode("utf-8")).hexdigest()
    return h


def _parsed_cache_path(cfg: Config, url: str, timestamp: str) -> Path:
    return cfg.parsed_cache_dir / f"{_parsed_cache_key(url, timestamp)}.json"


def _write_parsed_cache(cfg: Config, record: ParsedRecord) -> None:
    path = _parsed_cache_path(cfg, record.url, record.snapshot_timestamp)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(asdict(record), f, ensure_ascii=False)
    os.replace(tmp, path)


def _read_parsed_cache(cfg: Config, url: str, timestamp: str) -> Optional[ParsedRecord]:
    path = _parsed_cache_path(cfg, url, timestamp)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return ParsedRecord(**data)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Parsed-Cache {} kaputt, wird ignoriert: {}", path, exc)
        return None


def load_or_fetch_parsed(
    url: str,
    timestamp: str,
    session: CachedSession,
    cfg: Config,
) -> Optional[ParsedRecord]:
    cached = _read_parsed_cache(cfg, url, timestamp)
    if cached is not None:
        return cached

    wb_url = SNAPSHOT_TEMPLATE.format(timestamp=timestamp, url=url)

    try:
        fetched = fetch_snapshot(timestamp, url, session, cfg.rate_limit)
    except RetryError as exc:
        logger.warning("Retries erschöpft für {}: {}", wb_url, exc)
        return None
    except Exception as exc:
        logger.warning("Fetch-Fehler für {}: {}", wb_url, exc)
        return None

    if fetched is None:
        record = ParsedRecord(
            url=url,
            snapshot_timestamp=timestamp,
            wayback_url=wb_url,
            parse_success=False,
            skip_reason="wayback_404_or_error_page",
        )
        _write_parsed_cache(cfg, record)
        _delete_http_cache_entry(session, wb_url)
        return record

    html, headers = fetched
    try:
        raw_body = extract_text(html)
    except Exception as exc:
        logger.warning("Textextraktion warf Exception für {}: {}", wb_url, exc)
        raw_body = None

    if not raw_body:
        record = ParsedRecord(
            url=url,
            snapshot_timestamp=timestamp,
            wayback_url=wb_url,
            parse_success=False,
            skip_reason="no_body",
        )
        _write_parsed_cache(cfg, record)
        _delete_http_cache_entry(session, wb_url)
        return record

    body, truncated = _normalize_body(raw_body)
    meta = extract_metadata(html)
    date_info = resolve_publication_date(meta, headers)

    if date_info is None:
        record = ParsedRecord(
            url=url,
            snapshot_timestamp=timestamp,
            wayback_url=wb_url,
            parse_success=False,
            skip_reason="no_date",
            body=body,
            body_truncated=truncated,
            http_last_modified=(
                headers.get("Last-Modified") or headers.get("X-Archive-Orig-Last-Modified")
            ),
        )
        _write_parsed_cache(cfg, record)
        _delete_http_cache_entry(session, wb_url)
        return record

    d, precision, source = date_info
    language = detect_language_safe(body, url)

    record = ParsedRecord(
        url=url,
        snapshot_timestamp=timestamp,
        wayback_url=wb_url,
        parse_success=True,
        body=body,
        body_truncated=truncated,
        date_iso=d.isoformat(),
        date_precision=precision,
        date_source=source,
        language=language,
        http_last_modified=(
            headers.get("Last-Modified") or headers.get("X-Archive-Orig-Last-Modified")
        ),
    )
    _write_parsed_cache(cfg, record)
    _delete_http_cache_entry(session, wb_url)
    return record


def _delete_http_cache_entry(session: CachedSession, url: str) -> None:
    cache = getattr(session, "cache", None)
    if cache is None:
        return
    try:
        cache.delete(urls=[url])
        return
    except TypeError:
        pass
    except Exception as exc:
        logger.debug("HTML-Cache-Delete (1.x) fehlgeschlagen für {}: {}", url, exc)
    delete_url = getattr(cache, "delete_url", None)
    if callable(delete_url):
        try:
            delete_url(url)
            return
        except Exception as exc:
            logger.debug("HTML-Cache-Delete (0.x) fehlgeschlagen für {}: {}", url, exc)
    try:
        cache.delete(url)
    except Exception as exc:
        logger.debug("HTML-Cache-Delete (Fallback) fehlgeschlagen für {}: {}", url, exc)


# ============================== OUTPUT RECORD ================================


def build_output_record(
    parsed: ParsedRecord,
    domain: str,
    ai_tags: list[str],
    health_tags: list[str],
) -> dict:
    assert parsed.parse_success and parsed.body is not None and parsed.date_iso
    hash_input = f"{parsed.url}|{parsed.date_iso}".encode("utf-8")
    rec_id = hashlib.sha1(hash_input).hexdigest()[:16]
    return {
        "id": rec_id,
        "url": parsed.url,
        "date": parsed.date_iso,
        "date_precision": parsed.date_precision,
        "date_source": parsed.date_source,
        "body": parsed.body,
        "body_truncated": parsed.body_truncated,
        "tags": ai_tags + health_tags,
        "ai_tags": ai_tags,
        "health_tags": health_tags,
        "source_domain": domain,
        "language": parsed.language,
        "snapshot_timestamp": parsed.snapshot_timestamp,
        "wayback_url": parsed.wayback_url,
    }


# ============================== PROGRESS STATE ===============================


def load_progress(cfg: Config) -> dict:
    if not cfg.progress_path.exists():
        return {}
    try:
        with cfg.progress_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("progress.json kaputt, starte leer: {}", exc)
        return {}


def save_progress(cfg: Config, progress: dict) -> None:
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    tmp = cfg.progress_path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    os.replace(tmp, cfg.progress_path)


def update_domain_progress(
    cfg: Config,
    progress: dict,
    domain: str,
    stats: DomainStats,
    status: str,
    last_url: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    entry = progress.setdefault(domain, {})
    entry["status"] = status
    entry["urls_total"] = stats.urls_total
    entry["urls_processed"] = stats.urls_processed
    entry["hits"] = stats.hits
    if last_url is not None:
        entry["last_url"] = last_url
    if stats.started_at and not entry.get("started_at"):
        entry["started_at"] = stats.started_at
    if stats.finished_at:
        entry["finished_at"] = stats.finished_at
    entry["error"] = error
    save_progress(cfg, progress)


def finalize_domain_output(cfg: Config, target: DomainTarget) -> int:
    partial = cfg.state_dir / f"{target.file_slug}.partial.jsonl"
    final = cfg.output_dir / f"{target.file_slug}.jsonl"
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    if not partial.exists():
        final.touch()
        return 0
    os.replace(partial, final)
    count = 0
    with final.open("r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


# ============================== DOMAIN WORKER ================================


def _make_cdx_session(cfg: Config) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": cfg.user_agent})
    return session


def _make_html_session(cfg: Config) -> CachedSession:
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    session = CachedSession(
        cache_name=str(cfg.http_cache_path.with_suffix("")),
        backend="sqlite",
        expire_after=-1,
        allowable_methods=("GET",),
        allowable_codes=(200,),
        stale_if_error=False,
    )
    session.headers.update({"User-Agent": cfg.user_agent})
    return session


def process_domain(
    target: DomainTarget,
    cfg: Config,
    progress: dict,
    cdx_session: requests.Session,
    html_session: CachedSession,
) -> DomainStats:
    raw = target.raw
    stats = DomainStats(domain=raw)
    stats.started_at = datetime.now().isoformat(timespec="seconds")
    update_domain_progress(cfg, progress, raw, stats, status="in_progress")

    # 1) CDX abrufen
    try:
        cdx_rows = fetch_cdx_index(target, cdx_session, cfg.years, cfg.rate_limit, cfg)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        logger.error("CDX-Fehler für {}: {}\n{}", raw, err, traceback.format_exc())
        log_skip("fetch_errors", raw, raw, f"cdx_failed: {err}")
        stats.errors += 1
        stats.finished_at = datetime.now().isoformat(timespec="seconds")
        update_domain_progress(cfg, progress, raw, stats, status="failed", error=err)
        return stats

    # 2) Gruppieren + Sampling
    url_year_map = group_captures_by_url_and_year(cdx_rows, cfg.years, target)
    urls_sampled = sample_urls_per_year(url_year_map, cfg.limit_urls_per_year, cfg.sample_seed)
    stats.urls_total = len(urls_sampled)

    n_raw = len(cdx_rows)
    n_path_excl = sum(
        1 for r in cdx_rows
        if is_excluded_path(r.original) or urlparse(r.original).scheme not in ("http", "https")
    )
    n_target_mismatch = sum(1 for r in cdx_rows if not _url_matches_target(r.original, target))
    stats.skipped_path = n_path_excl
    logger.info(
        "Domain {}: cdx_rows={}, path_excl={}, target_mismatch={}, sampled={}",
        raw, n_raw, n_path_excl, n_target_mismatch, len(urls_sampled),
    )

    if cfg.dry_run:
        logger.info("Dry-run: {} URLs ermittelt, kein Fetch.", len(urls_sampled))
        stats.finished_at = datetime.now().isoformat(timespec="seconds")
        update_domain_progress(cfg, progress, raw, stats, status="done", last_url=None)
        return stats

    # 3) Resume
    partial_path = cfg.state_dir / f"{target.file_slug}.partial.jsonl"
    cfg.state_dir.mkdir(parents=True, exist_ok=True)

    start_index = 0
    if cfg.resume:
        last_url = progress.get(raw, {}).get("last_url")
        if last_url:
            for i, (u, y, _r) in enumerate(urls_sampled):
                if f"{u}|{y}" == last_url:
                    start_index = i + 1
                    break
            if start_index:
                logger.info("Resume: überspringe {} bereits verarbeitete URLs", start_index)
    else:
        if partial_path.exists():
            partial_path.unlink()

    # 4) Hauptschleife
    pbar = tqdm(
        total=len(urls_sampled),
        initial=start_index,
        desc=raw,
        unit="url",
        leave=False,
    )

    try:
        with partial_path.open("a", encoding="utf-8") as partial_fh:
            for idx in range(start_index, len(urls_sampled)):
                if _shutdown_requested.is_set():
                    logger.warning("Shutdown requested, pausiere Domain {}", raw)
                    stats.finished_at = datetime.now().isoformat(timespec="seconds")
                    update_domain_progress(
                        cfg, progress, raw, stats, status="in_progress",
                        last_url=progress.get(raw, {}).get("last_url"),
                    )
                    pbar.close()
                    return stats

                url, year, cdx_row = urls_sampled[idx]
                stats.urls_processed += 1

                try:
                    record = load_or_fetch_parsed(url, cdx_row.timestamp, html_session, cfg)
                except Exception as exc:
                    stats.errors += 1
                    log_skip("fetch_errors", raw, url, f"unexpected: {type(exc).__name__}: {exc}")
                    logger.debug("Traceback: {}", traceback.format_exc())
                    record = None

                if record is None:
                    pass
                elif not record.parse_success:
                    reason = record.skip_reason or "unknown"
                    if reason == "no_body":
                        stats.skipped_no_body += 1
                        log_skip("no_body", raw, url, "textextraktion fehlgeschlagen")
                    elif reason == "no_date":
                        stats.skipped_no_date += 1
                        log_skip("no_date", raw, url, "kein datePublished/dateModified")
                    elif reason == "wayback_404_or_error_page":
                        stats.errors += 1
                        log_skip("fetch_errors", raw, url, f"{reason} timestamp={cdx_row.timestamp}")
                    else:
                        log_skip("fetch_errors", raw, url, reason)
                else:
                    assert record.date_iso is not None
                    try:
                        rec_date = date.fromisoformat(record.date_iso)
                    except ValueError:
                        stats.skipped_no_date += 1
                        log_skip("no_date", raw, url, f"invalid date_iso {record.date_iso}")
                    else:
                        if rec_date.year not in cfg.years:
                            stats.skipped_no_date += 1
                            log_skip("no_date", raw, url, f"date {record.date_iso} outside range")
                        elif record.language not in ("de", "de_assumed"):
                            stats.skipped_lang += 1
                            log_skip("lang_skipped", raw, url, f"language={record.language}")
                        else:
                            body_text = record.body or ""
                            ai_tags = find_ai_keywords(body_text)
                            if not ai_tags:
                                stats.skipped_no_keywords += 1
                                log_skip("no_keywords", raw, url, "keine AI_KEYWORDS im Body")
                            else:
                                health_tags = find_health_keywords(body_text)
                                if not health_tags:
                                    stats.skipped_no_health += 1
                                    log_skip(
                                        "no_keywords", raw, url,
                                        f"AI ok ({','.join(ai_tags)}) aber keine HEALTH_KEYWORDS",
                                    )
                                else:
                                    out_rec = build_output_record(record, raw, ai_tags, health_tags)
                                    partial_fh.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                                    partial_fh.flush()
                                    stats.hits += 1

                last_key = f"{url}|{year}"
                update_domain_progress(
                    cfg, progress, raw, stats, status="in_progress", last_url=last_key,
                )
                pbar.set_postfix(year=year, hits=stats.hits, errors=stats.errors)
                pbar.update(1)
    finally:
        pbar.close()

    # 5) Abschluss
    stats.finished_at = datetime.now().isoformat(timespec="seconds")
    try:
        lines = finalize_domain_output(cfg, target)
        logger.info("Domain {} abgeschlossen: {} Treffer geschrieben", raw, lines)
        update_domain_progress(cfg, progress, raw, stats, status="done", last_url=None, error=None)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        logger.error("Finalize für {} fehlgeschlagen: {}\n{}", raw, err, traceback.format_exc())
        update_domain_progress(cfg, progress, raw, stats, status="failed", error=err)
    return stats


# ================================ CLI ========================================


def parse_args(argv: Optional[list[str]] = None) -> Config:
    parser = argparse.ArgumentParser(
        description="Wayback-Crawler für deutsche KI-Berichterstattung"
    )
    parser.add_argument("--input", type=Path, default=Path("root_domains_test.xlsx"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument("--resume", dest="resume", action="store_true")
    resume_group.add_argument("--no-resume", dest="resume", action="store_false")
    parser.set_defaults(resume=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--rate-limit", type=float, default=1.0)
    parser.add_argument("--limit-urls-per-year", type=int, default=500)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--years", type=str, default="2015-2025")
    parser.add_argument("--user-agent", type=str, default=USER_AGENT_DEFAULT)
    args = parser.parse_args(argv)
    years = parse_years_range(args.years)
    return Config(
        input_path=args.input,
        output_dir=args.output_dir,
        resume=args.resume,
        dry_run=args.dry_run,
        verbose=args.verbose,
        rate_limit=args.rate_limit,
        limit_urls_per_year=args.limit_urls_per_year,
        sample_seed=args.sample_seed,
        years=years,
        user_agent=args.user_agent,
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv)

    for p in (cfg.output_dir, cfg.state_dir, cfg.logs_dir, cfg.cache_dir, cfg.parsed_cache_dir):
        p.mkdir(parents=True, exist_ok=True)

    setup_logging(cfg)
    _install_signal_handlers()

    logger.info(
        "Starte Crawler | input={} | years={}-{} | rate_limit={} req/s | resume={}",
        cfg.input_path, cfg.years[0], cfg.years[-1], cfg.rate_limit, cfg.resume,
    )

    try:
        targets = load_domains(cfg.input_path)
    except Exception as exc:
        logger.error("Domains konnten nicht geladen werden: {}", exc)
        return 2

    if not targets:
        logger.error("Keine Domains gefunden in {}", cfg.input_path)
        return 2

    logger.info(
        "{} Domain-Targets geladen (davon {} mit explizitem Pfad-Präfix).",
        len(targets), sum(1 for t in targets if t.has_path_prefix),
    )

    cdx_session = _make_cdx_session(cfg)
    html_session = _make_html_session(cfg)
    progress = load_progress(cfg) if cfg.resume else {}

    global_stats: list[DomainStats] = []
    exit_code = 0
    try:
        for target in targets:
            if _shutdown_requested.is_set():
                logger.warning("Shutdown requested, keine weiteren Domains.")
                break

            entry = progress.get(target.raw, {})
            status = entry.get("status")
            if cfg.resume and status == "done":
                logger.info("Überspringe {} (status=done)", target.raw)
                continue

            try:
                stats = process_domain(target, cfg, progress, cdx_session, html_session)
                global_stats.append(stats)
                logger.info(
                    "Summary {}: total={} processed={} hits={} errors={} "
                    "no_date={} lang={} no_body={} no_ai_kw={} no_health_kw={}",
                    target.raw, stats.urls_total, stats.urls_processed,
                    stats.hits, stats.errors, stats.skipped_no_date,
                    stats.skipped_lang, stats.skipped_no_body,
                    stats.skipped_no_keywords, stats.skipped_no_health,
                )
            except Exception as exc:
                exit_code = 1
                logger.error(
                    "Unerwarteter Fehler bei {}: {}\n{}",
                    target.raw, exc, traceback.format_exc(),
                )
                continue
    finally:
        try:
            cdx_session.close()
        except Exception:
            pass
        try:
            html_session.close()
        except Exception:
            pass

    total_hits = sum(s.hits for s in global_stats)
    total_processed = sum(s.urls_processed for s in global_stats)
    logger.info(
        "Lauf beendet: {} Domains, {} URLs verarbeitet, {} Treffer geschrieben",
        len(global_stats), total_processed, total_hits,
    )
    if _shutdown_requested.is_set():
        return 130
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
    print("done")
