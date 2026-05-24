#!/usr/bin/env python3
"""Wayback-Crawler für deutsche KI-Berichterstattung.

Crawlt via Wayback Machine historische Snapshots deutscher Publisher-Domains,
extrahiert Haupttext und Content-Datum, filtert nach deutscher Sprache und
KI-bezogenen Keywords und schreibt pro Domain eine JSONL-Datei.

Aufruf:
    python wayback_crawler.py --input root_domains.xlsx

Siehe README.md für Details zu Output-Schema, Cache-Strategie und Resume.
"""

from __future__ import annotations

import argparse
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
except ImportError:  # pragma: no cover - ältere trafilatura
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

# langdetect ist standardmäßig nicht-deterministisch: einmal fixieren.
DetectorFactory.seed = 0


# ============================== KONSTANTEN ===================================

EXCLUDED_PATH_SUBSTRINGS = [
    # Rechtliches / Meta-Seiten
    "/impressum", "/imprint",
    "/privacy", "/datenschutz",
    "/terms", "/conditions", "/legal", "/disclaimer",
    "/cookie", "/cookies",
    "/barrierefreiheit", "/accessibility",
    "/hilfe", "/help", "/faq",

    # Auth / Account / Commerce
    "/login", "/log-in", "/signin", "/sign-in",
    "/signup", "/sign-up", "/register", "/registration",
    "/account", "/my-account", "/user/", "/users/",
    "/cart", "/checkout", "/warenkorb",

    # Kontakt / Service / Standorte
    "/contact", "/kontakt",
    "/support", "/service", "/kundenservice",
    "/standorte", "/locations",
    "/office", "/offices",

    # Karriere / Jobs
    "/jobs", "/job/", "/job-", "/careers", "/career",
    "/karriere", "/stellen", "/stellenangebote",
    "/vacancies", "/join-us", "/work-with-us",
    "/graduates", "/talent",

    # Navigations-Indizes / Listings
    "/search", "/suche",
    "/category/", "/categories/",
    "/author/", "/authors/",
    "/profile/", "/profiles/",
    "/feed/", "/rss",

    # Newsletter / Social
    "/newsletter", "/subscribe", "/subscription",
    "/share", "/sharing", "/social",

    # Technische Pfade / CMS-internes
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
        r"\b(artificial intelligence|künstliche intelligenz)\b", re.IGNORECASE
    ),
    "machine learning": re.compile(
        r"\b(machine learning|machinelles lernen)\b", re.IGNORECASE
    ),
    "deep learning": re.compile(r"\bdeep learning\b", re.IGNORECASE),
    "neural networks": re.compile(
        r"\b(neural networks?|neuronale netze)\b", re.IGNORECASE
    ),
    "llm": re.compile(
        r"\b(llm|large[\-\s]?language[\-\s]?models?)\b", re.IGNORECASE
    ),
    "medical device regulation": re.compile(
        r"\b(medical device regulation|medizinprodukteverordnung)\b", re.IGNORECASE
    ),
    "ai act": re.compile(r"\b(ai[\-\s]?act|ki[\-\s]?verordnung)\b", re.IGNORECASE),
    "ai": re.compile(r"\b(ai|ki)(?=[\s\-\.,;:!?]|$)", re.IGNORECASE),
}

USER_AGENT_DEFAULT = "WaybackResearchCrawler/1.0 (+mailto:research@example.com)"

CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"
SNAPSHOT_TEMPLATE = "http://web.archive.org/web/{timestamp}id_/{url}"

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


# ============================== DATACLASSES ==================================


# Pfad-Marker, die einen Bereich einer Seite eindeutig als deutschsprachig
# ausweisen. Wird sowohl für Pfad-Präfix-Matching als auch für den
# verschärften Sprachfilter bei kurzen Texten genutzt.
GERMAN_PATH_MARKERS = (
    "/de",
    "/de-de",
    "/de_de",
    "/de-at",
    "/de-ch",
    "/germany",
    "/deutschland",
)


@dataclass(frozen=True)
class DomainTarget:
    """Repräsentiert einen Eintrag aus der Input-Excel.

    Die Input-Spalte kann entweder nur einen Host (``example.de``) oder einen
    Host + Pfad-Präfix (``siemens.com/de``, ``bosch.com/germany``) enthalten.
    ``DomainTarget`` normalisiert beides und hält beide Teile getrennt.

    Attributes:
        raw: Der ursprüngliche Eintrag aus der Excel, lowercased + getrimmt,
            dient als Label in Output und progress.json.
        host: Nur der Host, z.B. ``siemens.com``.
        path_prefix: Normalisierter Pfad-Präfix (``/de``, ``/de-de``,
            ``/germany`` …) oder leerer String, wenn nur ein Host angegeben war.
        file_slug: Filesystem-safe Variante von ``raw``.
    """

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
    timestamp: str  # YYYYMMDDHHMMSS
    original: str
    mimetype: str
    statuscode: str
    digest: str
    length: str

    @property
    def capture_date(self) -> date:
        """Tagesdatum des Captures."""
        return datetime.strptime(self.timestamp[:8], "%Y%m%d").date()


@dataclass
class ParsedRecord:
    """Extrahierter und normalisierter Inhalt eines Snapshots (Cache-tauglich)."""

    url: str
    snapshot_timestamp: str
    wayback_url: str
    parse_success: bool
    skip_reason: Optional[str] = None
    body: Optional[str] = None
    body_truncated: bool = False
    date_iso: Optional[str] = None  # YYYY-MM-DD (aufgefüllt)
    date_precision: Optional[str] = None  # "day"|"month"|"year"
    date_source: Optional[str] = None
    language: Optional[str] = None  # "de"|"de_assumed"|andere ISO-Codes
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
    skipped_path: int = 0
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


@dataclass
class Config:
    """Zentrale Laufzeit-Konfiguration (aus CLI-Args)."""

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
    # Abgeleitete Pfade:
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
# Nur Shutdown-Flag und Locks, keine fachlichen Globals.

_shutdown_requested = threading.Event()
_rate_limit_lock = threading.Lock()
_last_request_monotonic: list[float] = [0.0]  # boxed, damit mutable über Threads


# ============================== LOGGING SETUP ================================


def setup_logging(cfg: Config) -> dict[str, int]:
    """Konfiguriert loguru und legt separate Sink-IDs für Skip-Logs an.

    Returns:
        Dict mit Sink-IDs pro Skip-Kategorie. Nicht strikt nötig, aber erlaubt
        späteres Abräumen bei Tests.
    """
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

    # Strukturierte Skip-Logs: je eine Datei, gefiltert über Record-Extra "skip".
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
    """Hilfsfunktion: schreibt eine Zeile in das strukturierte Skip-Log."""
    logger.bind(skip=tag, domain=domain, url=url).info(reason)


# ============================== UTILITIES ====================================


def parse_years_range(text: str) -> range:
    """Parst einen String wie ``"2015-2025"`` zu einem ``range``.

    Args:
        text: Der Range-String im Format ``"START-END"`` (inklusive).

    Returns:
        Ein ``range`` von START bis END+1.

    Raises:
        ValueError: Bei ungültigem Format oder Reihenfolge.
    """
    m = re.fullmatch(r"\s*(\d{4})\s*-\s*(\d{4})\s*", text)
    if not m:
        raise ValueError(
            f"Ungültiger years-Range '{text}', erwartet z.B. '2015-2025'"
        )
    start, end = int(m.group(1)), int(m.group(2))
    if start > end:
        raise ValueError(f"Start-Jahr {start} liegt nach End-Jahr {end}")
    return range(start, end + 1)


def rate_limit_wait(rate_limit: float) -> None:
    """Blockiert, bis seit dem letzten Aufruf 1/rate_limit Sekunden vergangen sind.

    Args:
        rate_limit: Requests pro Sekunde. Werte <= 0 deaktivieren das Limit.
    """
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
    """Installiert SIGINT/SIGTERM-Handler, die nur das Shutdown-Flag setzen."""

    def _handler(signum, frame):  # noqa: ARG001
        if not _shutdown_requested.is_set():
            logger.warning(
                "Signal {} empfangen, beende nach aktueller URL sauber …",
                signum,
            )
            _shutdown_requested.set()
        else:
            logger.error("Zweites Signal empfangen, breche hart ab.")
            sys.exit(130)

    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, AttributeError):
        # z.B. unter Windows nicht unterstützt
        pass


# ============================== INPUT LOADING ================================


def _sanitize_slug(raw: str) -> str:
    """Wandelt ein Domain-Target in einen Filesystem-safen Slug.

    ``siemens.com/de`` → ``siemens.com_de``, ``pfizer.com/de-de/``
    → ``pfizer.com_de-de``. Nicht-alphanumerische Zeichen außer ``.-_`` werden
    durch ``_`` ersetzt.
    """
    s = raw.strip().strip("/").lower()
    s = re.sub(r"[\\/]+", "_", s)
    s = re.sub(r"[^a-z0-9._\-]+", "_", s)
    s = re.sub(r"_{2,}", "_", s).strip("_")
    return s or "unnamed"


def _parse_domain_target(raw: str) -> Optional[DomainTarget]:
    """Parst einen Excel-Eintrag zu einem ``DomainTarget``.

    Akzeptiert Formen wie ``example.de``, ``https://siemens.com/de``,
    ``bosch.com/germany/``, ``sap.com/de-de``. Scheme, Trailing-Slash und
    ``www.`` werden entfernt; der Rest hinter dem Host wird als ``path_prefix``
    behandelt (mit führendem ``/``, ohne Trailing-``/``).

    Returns:
        ``DomainTarget`` oder ``None``, wenn der Eintrag unbrauchbar ist
        (leer, ohne Host).
    """
    if not raw:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    # Scheme entfernen
    s = re.sub(r"^https?://", "", s)
    # "www."-Prefix behalten? Viele Sites nutzen www., andere nicht. Für CDX
    # mit matchType=domain spielt das keine Rolle (alle Subdomains inkludiert),
    # für matchType=prefix aber schon. Wir lassen www. dran, wenn der User es
    # so geschrieben hat — das ist explizites User-Intent.
    # Trailing slash weg
    s = s.strip("/")
    if not s:
        return None

    # Erste Aufteilung in Host vs. Rest
    if "/" in s:
        host, _, path_rest = s.partition("/")
    else:
        host, path_rest = s, ""

    if not host or "." not in host:
        return None

    # path_prefix normalisieren: führendes "/", kein Trailing "/"
    path_prefix = ""
    if path_rest:
        path_prefix = "/" + path_rest.strip("/")

    slug = _sanitize_slug(s)
    return DomainTarget(
        raw=s, host=host, path_prefix=path_prefix, file_slug=slug
    )


def load_domains(path: Path) -> list[DomainTarget]:
    """Lädt die Spalte ``domain`` aus der Input-Excel-Datei und parst jedes Feld.

    Args:
        path: Pfad zur ``.xlsx``-Datei.

    Returns:
        Liste von ``DomainTarget`` in Reihenfolge der Datei, dedupliziert auf
        ``raw``.

    Raises:
        FileNotFoundError: Wenn die Datei nicht existiert.
        ValueError: Wenn die Pflichtspalte ``domain`` fehlt oder leer ist.
    """
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
    """Prüft, ob der URL-Pfad mit dem angegebenen Präfix beginnt.

    Case-insensitive Vergleich. Ein Präfix ``/de`` matcht ``/de``,
    ``/de/foo``, ``/de-de/…``, aber NICHT ``/design`` oder ``/dev`` — nach dem
    Präfix muss entweder das Pfad-Ende, ein ``/`` oder ein Trenner stehen.

    Args:
        url: Vollständige URL.
        prefix: Normalisierter Pfad-Präfix (mit führendem ``/``, ohne Trailing-``/``).
    """
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
    # Nach dem Präfix muss Ende, ``/`` oder ein Delimiter folgen.
    if tail == "" or tail.startswith("/"):
        return True
    return False


def _url_matches_target(url: str, target: DomainTarget) -> bool:
    """Prüft, ob eine URL zur Domain-Ziel-Spezifikation passt.

    - Host muss gleich dem Target-Host sein oder eine Subdomain davon.
    - Wenn Target einen ``path_prefix`` hat, muss der URL-Pfad damit beginnen.
    """
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
    """Heuristik: True, wenn der Pfad einen eindeutigen DE-Marker enthält.

    Wird als Fallback genutzt, wenn der Textkörper für ``langdetect`` zu kurz
    ist. Beispiele: ``https://x.com/de/foo`` → True, ``https://x.com/de-de/…``
    → True, ``https://x.com/news/…`` → False.
    """
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
    """True, wenn der Host der URL auf ``.de`` endet."""
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    return host.endswith(".de")


def is_excluded_path(url: str) -> bool:
    """Prüft, ob der Pfad-Teil der URL einen ausgeschlossenen Substring enthält.

    Args:
        url: Absolute URL.

    Returns:
        True, wenn der Pfad (case-insensitive) einen Eintrag aus
        ``EXCLUDED_PATH_SUBSTRINGS`` enthält.
    """
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
    """Normalisiert eine URL für robuste Deduplizierung.

    - Entfernt Tracking-Parameter aus ``TRACKING_PARAMS`` (case-insensitive)
    - Entfernt das Fragment
    - Vereinheitlicht den Trailing-Slash (nur Root behält ``/``)
    - Sortiert verbleibende Query-Parameter alphabetisch (via w3lib)
    - Lowercased Scheme und Host (via w3lib)

    Args:
        url: Eingabe-URL.

    Returns:
        Die normalisierte URL.
    """
    parsed = urlparse(url)
    # Tracking-Parameter entfernen
    if parsed.query:
        params = parse_qsl(parsed.query, keep_blank_values=True)
        tracking_lower = {p.lower() for p in TRACKING_PARAMS}
        filtered = [(k, v) for k, v in params if k.lower() not in tracking_lower]
        new_query = urlencode(filtered, doseq=True)
        parsed = parsed._replace(query=new_query)
    # Fragment weg
    parsed = parsed._replace(fragment="")
    # Trailing slash normalisieren (Root-Slash bleibt)
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        parsed = parsed._replace(path=parsed.path.rstrip("/"))
    rebuilt = urlunparse(parsed)
    # w3lib übernimmt finale Kanonisierung (Sortierung, Encoding, Lowercase
    # scheme/host, Prozent-Encoding).
    return canonicalize_url(rebuilt)


# ============================== CDX FETCH ====================================


class WaybackErrorPageException(Exception):
    """Wirft, wenn Wayback eine der bekannten Fehlerseiten ausliefert."""


def _should_retry(exc: BaseException) -> bool:
    """Tenacity-Predicate: True, wenn Exception retryable ist.

    Retryt bei Netz-Fehlern und bei 429/500/502/503/504. Kein Retry bei 404.
    """
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


def _cdx_file_cache_path(cfg: Config, target: DomainTarget, years: range) -> Path:
    """Pfad zum file-basierten CDX-Cache für ein Target + Jahresbereich.

    Das JSON wird gzipped gespeichert, weil CDX-Responses für stark
    archivierte Domains mehrere hundert MB bis GB erreichen können.
    """
    years_key = f"{years[0]}-{years[-1]}"
    return cfg.cache_dir / "cdx" / f"{target.file_slug}__{years_key}.json.gz"


def _read_cdx_file_cache(path: Path, ttl_seconds: int) -> Optional[list[CDXRow]]:
    """Liest gecachte CDX-Rows, wenn die Datei jünger als ``ttl_seconds`` ist.

    Returns:
        Liste von ``CDXRow`` oder ``None``, wenn Cache nicht existiert, abgelaufen
        oder defekt ist.
    """
    if not path.exists():
        return None
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if age > ttl_seconds:
        logger.debug(
            "CDX-Cache {} abgelaufen ({:.1f}d alt)", path.name, age / 86400.0
        )
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return [CDXRow(**row) for row in data]
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("CDX-Cache {} defekt, ignoriere: {}", path, exc)
        return None


def _write_cdx_file_cache(path: Path, rows: list[CDXRow]) -> None:
    """Schreibt CDX-Rows atomar als gzipped JSON."""
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


class CDXFetchError(Exception):
    """Ein CDX-Request hat keine verwertbare Antwort geliefert.

    Unterschieden von ``requests.HTTPError`` etc., damit der Aufrufer
    gezielt auf Nicht-JSON / Empty-Response / Exception reagieren kann
    (z.B. Jahr-für-Jahr-Fallback).
    """


def _fetch_cdx_single_query(
    cdx_url: str,
    match_type: str,
    from_ts: str,
    to_ts: str,
    session: requests.Session,
    rate_limit: float,
    label: str,
) -> list[CDXRow]:
    """Führt EINE einzelne CDX-Query aus. Wirft ``CDXFetchError`` bei Problemen.

    Args:
        cdx_url: Der ``url``-Parameter für CDX (Host oder Host+Pfad).
        match_type: ``"domain"`` oder ``"prefix"``.
        from_ts: Start-Timestamp (YYYYMMDD).
        to_ts: End-Timestamp (YYYYMMDD).
        session: Plain requests.Session.
        rate_limit: Rate-Limit in req/s.
        label: Label nur fürs Logging (z.B. ``"bfarm.de [2018]"``).

    Returns:
        Liste von CDXRow. Kann leer sein, wenn Wayback tatsächlich nichts hat.

    Raises:
        CDXFetchError: Bei Non-JSON-Response, HTTP-Fehler nach Retry-Erschöpfung,
            oder unerwarteter Response-Struktur.
    """
    params = [
        ("url", cdx_url),
        ("matchType", match_type),
        ("from", from_ts),
        ("to", to_ts),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
        ("output", "json"),
    ]

    # Accept-Encoding: identity → kein gzip. Wayback's Gzip-Streaming bei
    # großen Responses ist instabil und reißt mit ChunkedEncodingError ab.
    # Ungepackt ist größer auf der Leitung, aber deutlich zuverlässiger.
    request_headers = {"Accept-Encoding": "identity"}

    @_retry_decorator
    def _do_request_and_read() -> bytes:
        """Feuert den Request UND liest den kompletten Body innerhalb des Retry-Scopes.

        Der Body-Read muss hier passieren, weil ``ChunkedEncodingError`` erst
        beim Streamen der Chunks auftritt — nicht beim initialen ``get()``.
        Ohne Body-Read im Retry-Scope würde der Retry-Mechanismus nie greifen.
        """
        rate_limit_wait(rate_limit)
        r = session.get(
            CDX_ENDPOINT,
            params=params,
            headers=request_headers,
            timeout=(30, 300),
            stream=True,
        )
        if r.status_code in (429, 500, 502, 503, 504):
            r.raise_for_status()
        if r.status_code != 200:
            # Andere Status-Codes NICHT retryen (z.B. 404); Caller entscheidet.
            r.status_code_for_caller = r.status_code  # type: ignore[attr-defined]
            return b""
        # Kompletten Body in den Retry-Scope ziehen.
        # iter_content ist robuster als .content bei großen Streams.
        chunks: list[bytes] = []
        try:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    chunks.append(chunk)
        finally:
            try:
                r.close()
            except Exception:  # noqa: BLE001
                pass
        return b"".join(chunks)

    try:
        body = _do_request_and_read()
    except Exception as exc:  # noqa: BLE001
        raise CDXFetchError(
            f"Request für {label} fehlgeschlagen: {type(exc).__name__}: {exc}"
        ) from exc

    if not body:
        # Entweder leerer Body (200 OK) oder Non-200-Status, der oben gesetzt
        # wurde. Beides als "leere Response" behandeln.
        return []

    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except (ValueError, UnicodeDecodeError) as exc:
        # Preview der Response ins Log, damit man sieht was Wayback schickt
        # (oft HTML-Fehlerseite bei überlasteten Queries, oder trunkiertes
        # JSON bei serverseitig abgebrochenen Streams).
        try:
            preview = body[:500].decode("utf-8", errors="replace").replace("\n", " ")
        except Exception:  # noqa: BLE001
            preview = "<unreadable>"
        raise CDXFetchError(
            f"Response für {label} war kein gültiges JSON ({len(body)} bytes). "
            f"Preview: {preview!r}"
        ) from exc

    if data is None:
        return []
    if not isinstance(data, list):
        raise CDXFetchError(
            f"Response für {label} war unerwartetes Format: {type(data).__name__}"
        )
    if not data:
        return []

    # Erste Zeile ist der Header, Rest sind Daten.
    rows = data[1:]
    out: list[CDXRow] = []
    for row in rows:
        if len(row) < 6:
            continue
        try:
            out.append(
                CDXRow(
                    urlkey=str(row[0]),
                    timestamp=str(row[1]),
                    original=str(row[2]),
                    mimetype=str(row[3]),
                    statuscode=str(row[4]),
                    digest=str(row[5]),
                    length=str(row[6]) if len(row) > 6 else "",
                )
            )
        except (TypeError, ValueError):
            continue
    return out


def fetch_cdx_index(
    target: DomainTarget,
    session: requests.Session,
    years: range,
    rate_limit: float,
    cfg: Config,
) -> list[CDXRow]:
    """Holt den CDX-Index für eine Domain über den angegebenen Jahresbereich.

    Strategie:

    1. **File-Cache-Check** (7 Tage TTL). Bei Hit: zurück aus Cache.
    2. **Ein großer Request** über den kompletten Jahresbereich. Klappt das →
       Ergebnis cachen und zurückgeben.
    3. **Fallback auf Jahr-für-Jahr-Queries**, wenn der große Request:
       - Eine Exception/Nicht-JSON-Response lieferte ODER
       - 0 Zeilen für einen Target lieferte, von dem wir mehr erwarten
         (Heuristik: jedes Target, das nicht explizit limitiert wurde)

    Hintergrund Fallback: stark archivierte Domains (bfarm.de, rki.de, etc.)
    überlasten die CDX-Gesamt-Query, und Wayback liefert dann statt JSON eine
    HTML-Fehlerseite oder `[]` zurück. Pro-Jahr-Queries sind viel kleiner und
    gehen verlässlich durch.

    Verhalten abhängig vom ``DomainTarget``:

    - Ohne Pfad-Präfix (z.B. ``example.de``): ``matchType=domain`` → alle
      Subdomains und Pfade.
    - Mit Pfad-Präfix (z.B. ``siemens.com/de``): ``matchType=prefix`` mit
      Trailing-Slash am Präfix → nur URLs unter diesem Pfad.

    ``collapse=urlkey`` wird NICHT gesetzt, damit Multi-Edit-Szenarien (eine
    URL mit mehreren ``dateModified``-Jahren) über mehrere Captures erkannt
    werden können.

    Args:
        target: Die zu crawlende Domain-Spezifikation.
        session: Plain ``requests.Session`` (ohne requests-cache).
        years: Jahresbereich als ``range``.
        rate_limit: Rate-Limit in Requests/s.
        cfg: Konfiguration (für Cache-Pfad).

    Returns:
        Liste von ``CDXRow``-Instanzen. Leer, wenn Wayback tatsächlich nichts
        hat.

    Raises:
        CDXFetchError: Wenn sowohl die Gesamtquery als auch ALLE
            Jahr-für-Jahr-Queries fehlschlagen.
    """
    # 1) File-Cache-Check (billig, umgeht Rate-Limit).
    cache_path = _cdx_file_cache_path(cfg, target, years)
    cached = _read_cdx_file_cache(cache_path, CDX_CACHE_TTL_SECONDS)
    if cached is not None:
        logger.info(
            "CDX für {}: {} Zeilen aus File-Cache ({})",
            target.raw,
            len(cached),
            cache_path.name,
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

    # 2) Große Gesamtquery versuchen.
    try:
        rows = _fetch_cdx_single_query(
            cdx_url,
            match_type,
            start_ts,
            end_ts,
            session,
            rate_limit,
            label=f"{target.raw} [{years[0]}-{years[-1]}]",
        )
        if rows:
            logger.info(
                "CDX für {}: {} Rohzeilen (Gesamtquery)", target.raw, len(rows)
            )
            _write_cdx_file_cache(cache_path, rows)
            return rows
        # Leere Response: das KANN legitim sein (Wayback hat nichts), aber
        # bei stark archivierten .de-Hosts deutet es meist auf einen stillen
        # Query-Drop hin. Wir versuchen den Jahr-für-Jahr-Fallback.
        logger.warning(
            "CDX-Gesamtquery für {} lieferte 0 Zeilen, "
            "versuche Jahr-für-Jahr-Fallback",
            target.raw,
        )
    except CDXFetchError as exc:
        logger.warning(
            "CDX-Gesamtquery für {} fehlgeschlagen ({}), "
            "versuche Jahr-für-Jahr-Fallback",
            target.raw,
            exc,
        )

    # 3) Jahr-für-Jahr-Fallback.
    combined: list[CDXRow] = []
    years_ok = 0
    years_failed: list[int] = []
    for year in years:
        try:
            year_rows = _fetch_cdx_single_query(
                cdx_url,
                match_type,
                f"{year}0101",
                f"{year}1231",
                session,
                rate_limit,
                label=f"{target.raw} [{year}]",
            )
            combined.extend(year_rows)
            years_ok += 1
            logger.debug(
                "CDX {} Jahr {}: {} Zeilen", target.raw, year, len(year_rows)
            )
        except CDXFetchError as exc:
            # Sub-Fallback: das ganze Jahr in 2 Halbjahre splitten.
            # Hilft bei sehr stark archivierten Domains, deren einzelne Jahre
            # allein schon zu groß für Wayback sind (bfarm.de ab 2019 etc.).
            logger.warning(
                "CDX-Jahresquery {} [{}] fehlgeschlagen ({}), "
                "versuche Halbjahres-Fallback",
                target.raw,
                year,
                exc,
            )
            half_rows: list[CDXRow] = []
            halves_ok = 0
            for half_from, half_to in (
                (f"{year}0101", f"{year}0630"),
                (f"{year}0701", f"{year}1231"),
            ):
                try:
                    hr = _fetch_cdx_single_query(
                        cdx_url,
                        match_type,
                        half_from,
                        half_to,
                        session,
                        rate_limit,
                        label=f"{target.raw} [{half_from[:6]}]",
                    )
                    half_rows.extend(hr)
                    halves_ok += 1
                except CDXFetchError as half_exc:
                    logger.warning(
                        "CDX-Halbjahresquery {} [{}] fehlgeschlagen: {}",
                        target.raw,
                        half_from[:6],
                        half_exc,
                    )
                    continue
            if halves_ok > 0:
                combined.extend(half_rows)
                years_ok += 1
                logger.info(
                    "CDX {} Jahr {} via Halbjahres-Fallback: {} Zeilen ({}/2 Hälften)",
                    target.raw,
                    year,
                    len(half_rows),
                    halves_ok,
                )
            else:
                years_failed.append(year)
            continue

    logger.info(
        "CDX für {}: {} Rohzeilen (Fallback: {}/{} Jahre ok, {} fehlgeschlagen)",
        target.raw,
        len(combined),
        years_ok,
        len(list(years)),
        len(years_failed),
    )

    if years_ok == 0:
        raise CDXFetchError(
            f"Alle {len(list(years))} Jahr-für-Jahr-Queries für {target.raw} fehlgeschlagen"
        )

    # Auch teilweise erfolgreichen Cache schreiben (besser als nichts beim
    # nächsten Lauf). Wenn >50% der Jahre failed → nicht cachen, damit der
    # nächste Lauf es nochmal versucht.
    if years_failed and len(years_failed) > len(list(years)) / 2:
        logger.warning(
            "CDX-Ergebnis für {} unvollständig ({}/{} Jahre), "
            "wird NICHT gecacht",
            target.raw,
            years_ok,
            len(list(years)),
        )
    elif combined:
        _write_cdx_file_cache(cache_path, combined)

    return combined


# =============================== SAMPLING ====================================


def pick_closest_to_july2(captures: list[CDXRow], year: int) -> CDXRow:
    """Wählt den Capture, dessen Datum am nächsten zum 2. Juli des Jahres liegt.

    Args:
        captures: Liste von CDX-Captures (nicht leer).
        year: Zieljahr.

    Returns:
        Der Capture mit minimaler absoluter Tagesdistanz. Tie-Break: früherer
        Snapshot gewinnt.
    """
    target = date(year, 7, 2)

    def _key(row: CDXRow) -> tuple[int, str]:
        delta = abs((row.capture_date - target).days)
        # Tie-Break: früherer timestamp (= lexikalisch kleiner bei festem Format)
        return (delta, row.timestamp)

    return min(captures, key=_key)


def group_captures_by_url_and_year(
    rows: list[CDXRow], years: range, target: DomainTarget
) -> dict[str, dict[int, CDXRow]]:
    """Gruppiert CDX-Captures nach normalisierter URL und Jahr.

    Pro (URL, Jahr) wird genau ein Capture gewählt: der mit der geringsten
    Distanz zum 2. Juli des Jahres.

    URLs werden zusätzlich gegen das ``DomainTarget`` validiert — ohne
    Pfad-Präfix gilt nur Host/Subdomain-Gleichheit, mit Pfad-Präfix muss der
    URL-Pfad mit dem Präfix beginnen.

    Args:
        rows: Rohzeilen aus dem CDX-Index.
        years: Gültiger Jahresbereich; Captures außerhalb werden verworfen.
        target: Domain-Spezifikation.

    Returns:
        Dict normalized_url -> dict year -> CDXRow.
    """
    year_set = set(years)
    # Erst alle Captures pro (normalized_url, year) sammeln.
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
        except Exception:  # noqa: BLE001
            continue

        # Sicherheits-Recheck nach Normalisierung: bei matchType=domain kann
        # CDX URLs auf anderen Subdomains liefern, bei matchType=prefix kann
        # die Normalisierung den Pfad in Edge-Cases modifizieren.
        if not _url_matches_target(norm, target):
            continue

        bucket.setdefault(norm, {}).setdefault(year, []).append(row)

    # Pro (URL, Jahr) den besten Capture wählen.
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
    """Sampelt URLs pro Jahr unabhängig, max. ``limit_per_year`` pro Jahr.

    Args:
        url_year_map: Ergebnis von ``group_captures_by_url_and_year``.
        limit_per_year: Maximum URLs pro Jahr.
        seed: Seed für die lokale ``random.Random``-Instanz.

    Returns:
        Liste von (normalized_url, year, CDXRow), deterministisch sortiert.
    """
    rng = random.Random(seed)

    # Invertiere: year -> list of (url, row)
    per_year: dict[int, list[tuple[str, CDXRow]]] = {}
    for url, year_map in url_year_map.items():
        for year, row in year_map.items():
            per_year.setdefault(year, []).append((url, row))

    out: list[tuple[str, int, CDXRow]] = []
    for year in sorted(per_year.keys()):
        bucket = per_year[year]
        # Stabile Vor-Sortierung, damit Sampling deterministisch ist.
        bucket.sort(key=lambda x: x[0])
        if len(bucket) > limit_per_year:
            chosen = rng.sample(bucket, limit_per_year)
            chosen.sort(key=lambda x: x[0])  # stabile Ausgabe
        else:
            chosen = bucket
        for url, row in chosen:
            out.append((url, year, row))
    return out


# =============================== SNAPSHOT FETCH ==============================


def is_valid_wayback_response(html: str) -> bool:
    """Prüft, ob der Response-Body eine echte Seite und keine Wayback-Fehlerseite ist.

    Args:
        html: Rohes Response-Textfeld.

    Returns:
        True, wenn die Response als valide Seite gewertet wird.
    """
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
    """Lädt den Snapshot-HTML einer URL aus der Wayback Machine.

    Nutzt das ``id_``-Flag, um die Wayback-Toolbar aus dem HTML zu entfernen.

    Args:
        timestamp: Wayback-Timestamp (YYYYMMDDHHMMSS).
        url: Original-URL.
        session: Cached Requests-Session.
        rate_limit: Rate-Limit in Requests/s. Cache-Hits umgehen das Limit.

    Returns:
        Tupel (html, headers_dict) oder None bei 404 oder erkannter Fehlerseite.

    Raises:
        requests.HTTPError: Bei anderen HTTP-Fehlern nach Retry-Erschöpfung.
    """
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
            except Exception:  # noqa: BLE001
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
        logger.warning(
            "Snapshot {} lieferte Status {}", fetch_url, resp.status_code
        )
        return None

    html = resp.text
    if not is_valid_wayback_response(html):
        return None

    headers = {k: v for k, v in resp.headers.items()}
    return html, headers


# =============================== EXTRACTION ==================================


def _normalize_body(body: str) -> tuple[str, bool]:
    """Bereinigt einen Body: NFKC, Whitespace, Trimming, Längen-Cap.

    Returns:
        Tupel (cleaned_body, truncated).
    """
    if not body:
        return "", False
    nb = unicodedata.normalize("NFKC", body)
    # Whitespace: beliebige Whitespace-Sequenzen zu einfachem Leerzeichen,
    # Absätze (doppelte Newlines) behalten wir grob über eine zweistufige Regex.
    nb = re.sub(r"[ \t]+", " ", nb)
    nb = re.sub(r"\n{3,}", "\n\n", nb)
    nb = re.sub(r" *\n *", "\n", nb)
    nb = nb.strip()
    truncated = False
    if len(nb) > BODY_MAX_CHARS:
        # Am nächsten Wortgrenze schneiden.
        cut = nb.rfind(" ", 0, BODY_MAX_CHARS)
        if cut < BODY_MAX_CHARS // 2:
            cut = BODY_MAX_CHARS
        nb = nb[:cut].rstrip()
        truncated = True
    return nb, truncated


def extract_text(html: str) -> Optional[str]:
    """Extrahiert den Haupttext einer HTML-Seite.

    Zuerst ``trafilatura``, bei ``None`` Fallback auf BeautifulSoup
    (``<article>`` bevorzugt, sonst ``<main>``, sonst ``<body>``).

    Args:
        html: Der HTML-Quelltext.

    Returns:
        Extrahierter Body oder ``None``, wenn nichts Sinnvolles gefunden wurde.
    """
    try:
        body = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
            deduplicate=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("trafilatura.extract warf Exception: {}", exc)
        body = None
    if body and body.strip():
        return body

    # Fallback: BeautifulSoup
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:  # noqa: BLE001
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return None
    # Script/Style entfernen
    for tag in soup(["script", "style", "noscript", "nav", "header", "footer", "aside"]):
        tag.decompose()
    container = soup.find("article") or soup.find("main") or soup.find("body")
    if container is None:
        return None
    text = container.get_text(separator="\n", strip=True)
    return text or None


def _iter_jsonld_dicts(soup: BeautifulSoup):
    """Iteriert über alle JSON-LD-Objekte (inkl. @graph und Listen)."""
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string
        if not raw:
            # manche CMSe schreiben mehrere Kinder
            raw = script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Teils mit HTML-Kommentaren umgeben.
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
    """Extrahiert relevante Metadaten aus HTML (JSON-LD + meta-Tags).

    Args:
        html: Der HTML-Quelltext.

    Returns:
        Dict mit möglichen Keys: ``jsonld_datePublished``, ``jsonld_dateModified``,
        ``meta_article_published_time``, ``meta_article_modified_time``,
        ``meta_pubdate``, ``meta_last_modified``, ``time_tag``, ``trafilatura``.
    """
    out: dict[str, str] = {}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:  # noqa: BLE001
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return out

    # JSON-LD
    for obj in _iter_jsonld_dicts(soup):
        if not _matches_article_type(obj):
            continue
        if "jsonld_datePublished" not in out and obj.get("datePublished"):
            out["jsonld_datePublished"] = str(obj["datePublished"])
        if "jsonld_dateModified" not in out and obj.get("dateModified"):
            out["jsonld_dateModified"] = str(obj["dateModified"])

    # Meta-Tags
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

    # Varianten mit name=...
    for name in ("pubdate", "publishdate", "date", "DC.date.issued"):
        v = _meta({"name": re.compile(rf"^{re.escape(name)}$", re.IGNORECASE)})
        if v:
            out.setdefault("meta_pubdate", v)
            break

    lm = _meta({"name": re.compile(r"^last-modified$", re.IGNORECASE)})
    if lm:
        out["meta_last_modified"] = lm

    # trafilatura metadata
    if _trafi_extract_metadata is not None:
        try:
            meta_obj = _trafi_extract_metadata(html)
            if meta_obj is not None:
                md = getattr(meta_obj, "date", None)
                if md is None and isinstance(meta_obj, dict):
                    md = meta_obj.get("date")
                if md:
                    out["trafilatura"] = str(md)
        except Exception as exc:  # noqa: BLE001
            logger.debug("trafilatura metadata extract failed: {}", exc)

    # Erstes <time datetime> im Artikel-Hauptbereich
    container = soup.find("article") or soup.find("main") or soup
    t = container.find("time") if container else None
    if t and t.get("datetime"):
        out["time_tag"] = t["datetime"].strip()

    return out


# ============================== DATE RESOLUTION ==============================


_DATE_PATTERNS = [
    # ISO 8601 volle Form
    (
        re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$"),
        "day",
    ),
    # YYYY/MM/DD
    (re.compile(r"^(\d{4})/(\d{2})/(\d{2})"), "day"),
    # DD.MM.YYYY (deutsch)
    (re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})"), "day_de"),
    # YYYY-MM
    (re.compile(r"^(\d{4})-(\d{2})$"), "month"),
    # YYYY
    (re.compile(r"^(\d{4})$"), "year"),
    # RFC 1123 (HTTP Last-Modified): Thu, 01 Jan 2015 12:00:00 GMT
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


def _parse_date_string(
    raw: str,
) -> Optional[tuple[date, str]]:
    """Versucht, einen String in ein ``date`` + Präzision zu parsen.

    Returns:
        Tupel (date, "day"|"month"|"year") oder None bei Fehlschlag.
        Monats/Jahr-Präzisionen werden aufgefüllt (Tag=1, Monat=1).
    """
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
    # Fallback: datetime.fromisoformat mit Abschneiden
    try:
        core = s
        # manche Feeds liefern +0200 ohne Doppelpunkt -> fromisoformat ab 3.11 schluckt das nicht
        if len(core) >= 10:
            d = datetime.fromisoformat(core[:10])
            return d.date(), "day"
    except ValueError:
        pass
    return None


def resolve_publication_date(
    meta: dict[str, str], headers: dict[str, str]
) -> Optional[tuple[date, str, str]]:
    """Bestimmt das finale Content-Datum gemäß Spec (Published + Modified).

    Args:
        meta: Metadaten-Dict aus ``extract_metadata``.
        headers: HTTP-Header der Wayback-Response.

    Returns:
        Tupel (date, precision, source_label) oder ``None``, wenn weder
        Published noch Modified extrahiert werden konnte.
    """
    # Reihenfolgen genau gemäß Spec.
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
            headers.get("Last-Modified")
            or headers.get("X-Archive-Orig-Last-Modified"),
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
    """Erkennt die Sprache eines Textes robust und konservativ.

    Regeln (strikt deutsch):

    - ``len(body) >= 200``: ``langdetect.detect`` wird aufgerufen. Gibt den
      erkannten Code zurück (``"de"`` oder andere). Bei
      ``LangDetectException``: ``"lang_error"``.
    - ``len(body) < 200``: langdetect ist bei kurzen Texten unzuverlässig
      (würfelt). Wir vergeben ``"de_assumed"`` NUR, wenn der Text entweder
      (a) auf einem Host mit ``.de``-TLD liegt, ODER
      (b) auf einem Pfad mit eindeutigem DE-Marker (``/de``, ``/de-de``,
      ``/germany``, ``/deutschland`` …). Sonst ``"too_short_unknown"``.

    Diese zweite Regel ist strenger als der ursprüngliche Spec-Wortlaut, weil
    die Input-Excel nun multinationale Domains mit deutschem Länderpfad
    enthält — ein .com-Host liefert auch auf ``/en/``-Pfaden kurze Seiten, die
    nicht automatisch deutsch sind.

    Args:
        body: Der normalisierte Body.
        url: Die Quell-URL (für die Pfad-/TLD-Heuristik).

    Returns:
        ``"de"`` — langdetect hat Deutsch bestätigt.
        ``"de_assumed"`` — kurzer Text, aber Host/Pfad deutet auf Deutsch.
        ``"too_short_unknown"`` — kurzer Text ohne DE-Indikator → zu verwerfen.
        ``"lang_error"`` — langdetect hat eine Exception geworfen.
        anderer ISO-Code (``"en"``, ``"fr"`` …) — nicht deutsch, zu verwerfen.
    """
    if len(body) >= 200:
        try:
            return detect(body)
        except LangDetectException:
            return "lang_error"
    # Kurzer Text: nur behalten, wenn Host/Pfad deutsch.
    if _host_has_de_tld(url) or _url_has_german_marker(url):
        return "de_assumed"
    return "too_short_unknown"


def find_keywords(body: str) -> list[str]:
    """Findet alle getroffenen Keyword-Gruppen im Body.

    Args:
        body: Normalisierter Body.

    Returns:
        Liste von Keys aus ``AI_KEYWORDS``, in Definitionsreihenfolge.
    """
    hits: list[str] = []
    for key, pattern in AI_KEYWORDS.items():
        if pattern.search(body):
            hits.append(key)
    return hits


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
    """Lädt einen geparsten Snapshot aus dem Cache oder fetcht und parst ihn.

    Args:
        url: Normalisierte Original-URL.
        timestamp: Wayback-Timestamp.
        session: Cached Requests-Session für HTML.
        cfg: Laufzeit-Konfiguration.

    Returns:
        ``ParsedRecord`` bei Erfolg oder mit ``parse_success=False`` im
        Fehlerfall (gecacht, damit Re-Runs nicht erneut fetchen). ``None`` nur
        bei harten Netz-Fehlern, die nicht gecacht werden sollen.
    """
    cached = _read_parsed_cache(cfg, url, timestamp)
    if cached is not None:
        return cached

    wb_url = SNAPSHOT_TEMPLATE.format(timestamp=timestamp, url=url)

    try:
        fetched = fetch_snapshot(timestamp, url, session, cfg.rate_limit)
    except RetryError as exc:
        logger.warning("Retries erschöpft für {}: {}", wb_url, exc)
        return None
    except Exception as exc:  # noqa: BLE001
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
        # HTML-Cache-Eintrag löschen (falls aus requests-cache)
        _delete_http_cache_entry(session, wb_url)
        return record

    html, headers = fetched
    try:
        raw_body = extract_text(html)
    except Exception as exc:  # noqa: BLE001
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

    # Metadaten & Datum
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
            http_last_modified=headers.get("Last-Modified")
            or headers.get("X-Archive-Orig-Last-Modified"),
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
        http_last_modified=headers.get("Last-Modified")
        or headers.get("X-Archive-Orig-Last-Modified"),
    )
    _write_parsed_cache(cfg, record)
    _delete_http_cache_entry(session, wb_url)
    return record


def _delete_http_cache_entry(session: CachedSession, url: str) -> None:
    """Löscht einen Eintrag aus dem requests-cache (HTML-Cache).

    Die Delete-API von ``requests-cache`` hat sich zwischen Versionen
    geändert. Diese Funktion probiert beide bekannten Signaturen.
    """
    cache = getattr(session, "cache", None)
    if cache is None:
        return
    # 1.x API: session.cache.delete(urls=[...])
    try:
        cache.delete(urls=[url])
        return
    except TypeError:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.debug("HTML-Cache-Delete (1.x) fehlgeschlagen für {}: {}", url, exc)
    # 0.x API: session.cache.delete_url(url)
    delete_url = getattr(cache, "delete_url", None)
    if callable(delete_url):
        try:
            delete_url(url)
            return
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "HTML-Cache-Delete (0.x) fehlgeschlagen für {}: {}", url, exc
            )
    # 1.x API Alt-Form: session.cache.delete(url)
    try:
        cache.delete(url)
    except Exception as exc:  # noqa: BLE001
        logger.debug("HTML-Cache-Delete (Fallback) fehlgeschlagen für {}: {}", url, exc)


# ============================== OUTPUT RECORD ================================


def build_output_record(
    parsed: ParsedRecord,
    domain: str,
    tags: list[str],
) -> dict:
    """Baut das finale Output-Dict in der vorgegebenen Key-Reihenfolge.

    Args:
        parsed: Erfolgreicher ParsedRecord.
        domain: Quelldomain.
        tags: Liste der gematchten Keyword-Keys.

    Returns:
        Dict mit allen Feldern gemäß Output-Schema.
    """
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
        "tags": tags,
        "source_domain": domain,
        "language": parsed.language,
        "snapshot_timestamp": parsed.snapshot_timestamp,
        "wayback_url": parsed.wayback_url,
    }


# ============================== PROGRESS STATE ===============================


def load_progress(cfg: Config) -> dict:
    """Lädt ``state/progress.json`` (oder liefert leeres Dict)."""
    if not cfg.progress_path.exists():
        return {}
    try:
        with cfg.progress_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("progress.json kaputt, starte leer: {}", exc)
        return {}


def save_progress(cfg: Config, progress: dict) -> None:
    """Schreibt ``state/progress.json`` atomar."""
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
    """Aktualisiert den Eintrag für eine Domain in progress.json."""
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
    """Mergt ``state/<slug>.partial.jsonl`` nach ``output/<slug>.jsonl``.

    Args:
        cfg: Konfiguration.
        target: Domain-Spezifikation.

    Returns:
        Zahl der Zeilen im Output.
    """
    partial = cfg.state_dir / f"{target.file_slug}.partial.jsonl"
    final = cfg.output_dir / f"{target.file_slug}.jsonl"
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    if not partial.exists():
        # Kein Match gefunden: leere Output-Datei anlegen zur Markierung.
        final.touch()
        return 0
    # Atomar verschieben.
    os.replace(partial, final)
    # Zeilen zählen.
    count = 0
    with final.open("r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


# ============================== DOMAIN WORKER ================================


def _make_cdx_session(cfg: Config) -> requests.Session:
    """Baut eine plain ``requests.Session`` für CDX-Requests.

    CDX nutzt bewusst keinen ``requests-cache``-Backend, weil CDX-Responses
    für stark archivierte Domains das SQLite-BLOB-Limit sprengen können
    (>1 GB → ``sqlite3.DataError``). Stattdessen gibt es einen file-basierten
    Cache in ``.cache/cdx/`` (siehe ``_cdx_file_cache_path``).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": cfg.user_agent})
    return session


def _make_html_session(cfg: Config) -> CachedSession:
    """Baut eine CachedSession für HTML-Snapshots (ohne TTL, explizit geräumt)."""
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    session = CachedSession(
        cache_name=str(cfg.http_cache_path.with_suffix("")),
        backend="sqlite",
        expire_after=-1,  # kein automatisches Ablaufen
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
    """Verarbeitet ein einzelnes Domain-Target: CDX → Sampling → Fetch/Parse → Output.

    Schreibt laufend in ``state/<slug>.partial.jsonl``. Bei erfolgreichem
    Abschluss wird dieser atomar nach ``output/<slug>.jsonl`` gemerged.

    Args:
        target: Die zu verarbeitende Domain-Spezifikation.
        cfg: Konfiguration.
        progress: Globales progress-Dict (wird mutiert und gespeichert).
        cdx_session: Cached Session für CDX-Requests.
        html_session: Cached Session für Snapshot-Fetches.

    Returns:
        ``DomainStats`` mit Laufzeitmetriken.
    """
    raw = target.raw
    stats = DomainStats(domain=raw)
    stats.started_at = datetime.now().isoformat(timespec="seconds")
    update_domain_progress(cfg, progress, raw, stats, status="in_progress")

    # 1) CDX abrufen
    try:
        cdx_rows = fetch_cdx_index(
            target, cdx_session, cfg.years, cfg.rate_limit, cfg
        )
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        logger.error("CDX-Fehler für {}: {}\n{}", raw, err, traceback.format_exc())
        log_skip("fetch_errors", raw, raw, f"cdx_failed: {err}")
        stats.errors += 1
        stats.finished_at = datetime.now().isoformat(timespec="seconds")
        update_domain_progress(
            cfg, progress, raw, stats, status="failed", error=err
        )
        return stats

    # 2) Gruppieren + Sampling
    url_year_map = group_captures_by_url_and_year(cdx_rows, cfg.years, target)
    urls_sampled = sample_urls_per_year(
        url_year_map, cfg.limit_urls_per_year, cfg.sample_seed
    )
    stats.urls_total = len(urls_sampled)

    # Diagnostik: warum wurden Captures aussortiert?
    n_raw = len(cdx_rows)
    n_path_excl = sum(
        1
        for r in cdx_rows
        if is_excluded_path(r.original)
        or urlparse(r.original).scheme not in ("http", "https")
    )
    n_target_mismatch = sum(
        1 for r in cdx_rows if not _url_matches_target(r.original, target)
    )
    stats.skipped_path = n_path_excl
    logger.info(
        "Domain {}: cdx_rows={}, path_excl={}, target_mismatch={}, sampled={}",
        raw,
        n_raw,
        n_path_excl,
        n_target_mismatch,
        len(urls_sampled),
    )

    if cfg.dry_run:
        logger.info("Dry-run: {} URLs ermittelt, kein Fetch.", len(urls_sampled))
        stats.finished_at = datetime.now().isoformat(timespec="seconds")
        update_domain_progress(
            cfg, progress, raw, stats, status="done", last_url=None
        )
        return stats

    # 3) Resume: Index des letzten verarbeiteten URL-Jahr-Paars bestimmen.
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
                logger.info(
                    "Resume: überspringe {} bereits verarbeitete URLs", start_index
                )
    else:
        # No-resume: partial.jsonl löschen.
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
                        cfg,
                        progress,
                        raw,
                        stats,
                        status="in_progress",
                        last_url=progress.get(raw, {}).get("last_url"),
                    )
                    pbar.close()
                    return stats

                url, year, cdx_row = urls_sampled[idx]
                stats.urls_processed += 1

                try:
                    record = load_or_fetch_parsed(
                        url, cdx_row.timestamp, html_session, cfg
                    )
                except Exception as exc:  # noqa: BLE001
                    stats.errors += 1
                    log_skip(
                        "fetch_errors",
                        raw,
                        url,
                        f"unexpected: {type(exc).__name__}: {exc}",
                    )
                    logger.debug("Traceback: {}", traceback.format_exc())
                    record = None

                if record is None:
                    # Harter Netzfehler, kein Cache-Eintrag.
                    pass
                elif not record.parse_success:
                    reason = record.skip_reason or "unknown"
                    if reason == "no_body":
                        stats.skipped_no_body += 1
                        log_skip("no_body", raw, url, "textextraktion fehlgeschlagen")
                    elif reason == "no_date":
                        stats.skipped_no_date += 1
                        log_skip(
                            "no_date", raw, url, "kein datePublished/dateModified"
                        )
                    elif reason == "wayback_404_or_error_page":
                        stats.errors += 1
                        log_skip(
                            "fetch_errors",
                            raw,
                            url,
                            f"{reason} timestamp={cdx_row.timestamp}",
                        )
                    else:
                        log_skip("fetch_errors", raw, url, reason)
                else:
                    # Erfolg: weiter prüfen (Jahr-Range, Sprache, Keywords).
                    assert record.date_iso is not None
                    try:
                        rec_date = date.fromisoformat(record.date_iso)
                    except ValueError:
                        stats.skipped_no_date += 1
                        log_skip(
                            "no_date", raw, url, f"invalid date_iso {record.date_iso}"
                        )
                    else:
                        if rec_date.year not in cfg.years:
                            stats.skipped_no_date += 1
                            log_skip(
                                "no_date",
                                raw,
                                url,
                                f"date {record.date_iso} outside range",
                            )
                        elif record.language not in ("de", "de_assumed"):
                            # Alles andere (en, fr, …, too_short_unknown,
                            # lang_error) → verwerfen. So bleiben wir wirklich
                            # bei deutschen Artikeln, auch auf .com-Hosts.
                            stats.skipped_lang += 1
                            log_skip(
                                "lang_skipped",
                                raw,
                                url,
                                f"language={record.language}",
                            )
                        else:
                            tags = find_keywords(record.body or "")
                            if not tags:
                                stats.skipped_no_keywords += 1
                                log_skip(
                                    "no_keywords",
                                    raw,
                                    url,
                                    "keine AI_KEYWORDS im Body",
                                )
                            else:
                                out_rec = build_output_record(record, raw, tags)
                                partial_fh.write(
                                    json.dumps(out_rec, ensure_ascii=False) + "\n"
                                )
                                partial_fh.flush()
                                stats.hits += 1

                # Progress aktualisieren (nach jeder URL).
                last_key = f"{url}|{year}"
                update_domain_progress(
                    cfg,
                    progress,
                    raw,
                    stats,
                    status="in_progress",
                    last_url=last_key,
                )
                pbar.set_postfix(
                    year=year, hits=stats.hits, errors=stats.errors
                )
                pbar.update(1)
    finally:
        pbar.close()

    # 5) Abschluss: partial → output mergen.
    stats.finished_at = datetime.now().isoformat(timespec="seconds")
    try:
        lines = finalize_domain_output(cfg, target)
        logger.info(
            "Domain {} abgeschlossen: {} Treffer geschrieben", raw, lines
        )
        update_domain_progress(
            cfg,
            progress,
            raw,
            stats,
            status="done",
            last_url=None,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        logger.error(
            "Finalize für {} fehlgeschlagen: {}\n{}",
            raw,
            err,
            traceback.format_exc(),
        )
        update_domain_progress(
            cfg, progress, raw, stats, status="failed", error=err
        )
    return stats


# ================================ CLI ========================================


def parse_args(argv: Optional[list[str]] = None) -> Config:
    """Parst die CLI-Argumente und baut ein ``Config``-Objekt."""
    parser = argparse.ArgumentParser(
        description="Wayback-Crawler für deutsche KI-Berichterstattung"
    )
    parser.add_argument(
        "--input", type=Path, default=Path("last_root_domains.xlsx")
    )
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
    """Haupteinstieg. Gibt einen Exit-Code zurück (0=ok, nonzero=Fehler)."""
    cfg = parse_args(argv)

    # Ordner anlegen
    for p in (cfg.output_dir, cfg.state_dir, cfg.logs_dir, cfg.cache_dir, cfg.parsed_cache_dir):
        p.mkdir(parents=True, exist_ok=True)

    setup_logging(cfg)
    _install_signal_handlers()

    logger.info(
        "Starte Crawler | input={} | years={}-{} | rate_limit={} req/s | resume={}",
        cfg.input_path,
        cfg.years[0],
        cfg.years[-1],
        cfg.rate_limit,
        cfg.resume,
    )

    # Domains laden
    try:
        targets = load_domains(cfg.input_path)
    except Exception as exc:  # noqa: BLE001
        logger.error("Domains konnten nicht geladen werden: {}", exc)
        return 2

    if not targets:
        logger.error("Keine Domains gefunden in {}", cfg.input_path)
        return 2

    logger.info(
        "{} Domain-Targets geladen (davon {} mit explizitem Pfad-Präfix).",
        len(targets),
        sum(1 for t in targets if t.has_path_prefix),
    )

    # Sessions aufbauen
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
                stats = process_domain(
                    target, cfg, progress, cdx_session, html_session
                )
                global_stats.append(stats)
                # Pro-Domain-Summary
                logger.info(
                    "Summary {}: total={} processed={} hits={} errors={} "
                    "no_date={} lang={} no_body={} no_keywords={}",
                    target.raw,
                    stats.urls_total,
                    stats.urls_processed,
                    stats.hits,
                    stats.errors,
                    stats.skipped_no_date,
                    stats.skipped_lang,
                    stats.skipped_no_body,
                    stats.skipped_no_keywords,
                )
            except Exception as exc:  # noqa: BLE001
                exit_code = 1
                logger.error(
                    "Unerwarteter Fehler bei {}: {}\n{}",
                    target.raw,
                    exc,
                    traceback.format_exc(),
                )
                # Weiter mit nächstem Target (Einzelfehler dürfen Lauf nicht stoppen).
                continue
    finally:
        try:
            cdx_session.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            html_session.close()
        except Exception:  # noqa: BLE001
            pass

    # Gesamtsumme
    total_hits = sum(s.hits for s in global_stats)
    total_processed = sum(s.urls_processed for s in global_stats)
    logger.info(
        "Lauf beendet: {} Domains, {} URLs verarbeitet, {} Treffer geschrieben",
        len(global_stats),
        total_processed,
        total_hits,
    )
    if _shutdown_requested.is_set():
        return 130
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
    print("done")
