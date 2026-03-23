#!/usr/bin/env python3
"""
MKOSZ Play-by-Play parser.
Scrapes play-by-play event lists from mkosz.hu and stores them in SQLite.

Usage:
    # Single match
    python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_123749
    python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749
    python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749 --force

    # Batch mode — all matches for a competition
    python3 parse_pbp.py --season x2526 --comp hun2a
    python3 parse_pbp.py --season x2526 --comp hun2a --list-only
    python3 parse_pbp.py --season x2526 --comp hun2a --force
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag
from io import BytesIO

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PBP_URL_TEMPLATE = "https://mkosz.hu/merkozes-esemenylista/{season}/{comp}/{comp}_{game_id}"
PDF_URL_TEMPLATE = "https://hunbasketimg.webpont.com/pdf/{season}/{comp}_{game_id}.pdf"
SCHEDULE_URL = "https://mkosz.hu/bajnoksag-musor/{season}/{comp}/"
DEFAULT_DB = "pbp.sqlite"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
BATCH_DELAY = 0.3  # seconds between requests in batch mode

# Hungarian month names for date parsing
HU_MONTHS = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}

# Event type mapping: Hungarian → normalized code
# 4 dobástípus: közeli, középtávoli, hárompontos, büntető
EVENT_TYPES = {
    # Közeli (2 pont)
    "sikeres közeli": "CLOSE_MADE",
    "sikertelen közeli": "CLOSE_MISS",
    "sikeres zsákolás": "DUNK_MADE",       # zsákolás = közeli variáns (2 pont)
    "sikertelen zsákolás": "DUNK_MISS",     # sikertelen zsákolás
    # Középtávoli (2 pont)
    "sikeres középtávoli": "MID_MADE",
    "sikertelen középtávoli": "MID_MISS",
    # Hárompontos (3 pont)
    "sikeres hárompontos": "THREE_MADE",
    "sikertelen hárompontos": "THREE_MISS",
    # Büntető (1 pont)
    "sikeres büntető": "FT_MADE",
    "kihagyott büntető": "FT_MISS",
    # Lepattanó
    "támadólepattanó": "OREB",
    "védőlepattanó": "DREB",
    # Fault
    "foult": "FOUL",
    "kiharcolt fault": "FOUL_DRAWN",
    # Blokk
    "blokk": "BLK",
    "kapott blokk": "BLK_RECV",
    # Labda
    "eladott labda": "TOV",
    "szerzett labda": "STL",
    # Gólpassz
    "gólpassz": "AST",
}

# Points awarded for scoring event types
POINTS_MAP = {
    "CLOSE_MADE": 2,
    "DUNK_MADE": 2,
    "MID_MADE": 2,
    "THREE_MADE": 3,
    "FT_MADE": 1,
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class MatchInfo:
    match_id: str
    comp_code: str
    season: str
    comp_name: str
    round_name: str
    match_date: str  # YYYY-MM-DD
    match_time: str  # HH:MM
    venue: str
    team_a: str  # home team (short name from header)
    team_b: str  # away team (short name from header)
    team_a_full: str  # full name from tablepbp header
    team_b_full: str  # full name from tablepbp header
    score_a: int
    score_b: int
    quarter_scores: list  # [[a1,b1], [a2,b2], ...]
    referees: str
    source_url: str


@dataclass
class PBPEvent:
    event_seq: int
    quarter: int
    minute: Optional[int]
    team: str  # 'A' or 'B'
    player_name: Optional[str]
    event_type: str  # normalized code
    event_raw: str  # original Hungarian text
    counter: Optional[int]
    score_a: Optional[int]
    score_b: Optional[int]
    is_scoring: bool
    points: int


@dataclass
class Substitution:
    event_seq: int
    quarter: int
    minute: Optional[int]
    team: str
    player_in: str
    player_out: str


@dataclass
class Timeout:
    event_seq: int
    quarter: int
    minute: Optional[int]
    team: str


@dataclass
class RosterEntry:
    player_name: str
    jersey_number: Optional[int]
    is_starter: bool
    fouls_personal: int = 0
    fouls_technical: int = 0
    fouls_unsportsmanlike: int = 0
    license_number: Optional[str] = None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    match_id        TEXT PRIMARY KEY,
    comp_code       TEXT NOT NULL,
    season          TEXT NOT NULL,
    comp_name       TEXT,
    round_name      TEXT,
    match_date      TEXT,
    match_time      TEXT,
    venue           TEXT,
    team_a          TEXT NOT NULL,
    team_b          TEXT NOT NULL,
    team_a_full     TEXT,
    team_b_full     TEXT,
    score_a         INTEGER,
    score_b         INTEGER,
    quarter_scores  TEXT,
    referees        TEXT,
    source_url      TEXT NOT NULL,
    extracted_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    event_seq       INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    minute          INTEGER,
    team            TEXT NOT NULL CHECK (team IN ('A', 'B')),
    player_name     TEXT,
    event_type      TEXT NOT NULL,
    event_raw       TEXT NOT NULL,
    counter         INTEGER,
    score_a         INTEGER,
    score_b         INTEGER,
    is_scoring      INTEGER NOT NULL DEFAULT 0,
    points          INTEGER DEFAULT 0,
    UNIQUE(match_id, event_seq)
);

CREATE TABLE IF NOT EXISTS substitutions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    event_seq       INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    minute          INTEGER,
    team            TEXT NOT NULL CHECK (team IN ('A', 'B')),
    player_in       TEXT NOT NULL,
    player_out      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS timeouts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    event_seq       INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    minute          INTEGER,
    team            TEXT NOT NULL CHECK (team IN ('A', 'B'))
);

CREATE TABLE IF NOT EXISTS player_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team            TEXT NOT NULL CHECK (team IN ('A', 'B')),
    player_name     TEXT NOT NULL,
    is_starter      INTEGER NOT NULL DEFAULT 0,
    minutes         INTEGER NOT NULL DEFAULT 0,
    plus_minus      INTEGER NOT NULL DEFAULT 0,
    val             INTEGER NOT NULL DEFAULT 0,
    -- Összetett mutatók
    ts_pct          REAL,    -- True Shooting %
    efg_pct         REAL,    -- Effective FG %
    game_score      REAL,    -- Hollinger Game Score
    usg_pct         REAL,    -- Usage Rate %
    ast_to          REAL,    -- Assist/Turnover ratio
    tov_pct         REAL,    -- Turnover Rate %
    UNIQUE(match_id, team, player_name)
);

CREATE TABLE IF NOT EXISTS rosters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team            TEXT NOT NULL CHECK (team IN ('A', 'B')),
    player_name     TEXT NOT NULL,
    jersey_number   INTEGER,
    is_starter      INTEGER NOT NULL DEFAULT 0,
    fouls_personal  INTEGER NOT NULL DEFAULT 0,
    fouls_technical INTEGER NOT NULL DEFAULT 0,
    fouls_unsportsmanlike INTEGER NOT NULL DEFAULT 0,
    license_number  TEXT,
    UNIQUE(match_id, team, player_name)
);
"""


def create_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def match_exists(conn: sqlite3.Connection, match_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM matches WHERE match_id = ?", (match_id,)
    ).fetchone()
    return row is not None


def delete_match(conn: sqlite3.Connection, match_id: str):
    conn.execute("DELETE FROM rosters WHERE match_id = ?", (match_id,))
    conn.execute("DELETE FROM player_stats WHERE match_id = ?", (match_id,))
    conn.execute("DELETE FROM timeouts WHERE match_id = ?", (match_id,))
    conn.execute("DELETE FROM substitutions WHERE match_id = ?", (match_id,))
    conn.execute("DELETE FROM events WHERE match_id = ?", (match_id,))
    conn.execute("DELETE FROM matches WHERE match_id = ?", (match_id,))
    conn.commit()


def save_match(
    conn: sqlite3.Connection,
    info: MatchInfo,
    events: list[PBPEvent],
    subs: list[Substitution],
    timeouts: list[Timeout],
    roster: Optional[dict] = None,
):
    conn.execute(
        """INSERT INTO matches
           (match_id, comp_code, season, comp_name, round_name, match_date,
            match_time, venue, team_a, team_b, team_a_full, team_b_full,
            score_a, score_b, quarter_scores, referees, source_url)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            info.match_id, info.comp_code, info.season, info.comp_name,
            info.round_name, info.match_date, info.match_time, info.venue,
            info.team_a, info.team_b, info.team_a_full, info.team_b_full,
            info.score_a, info.score_b,
            json.dumps(info.quarter_scores), info.referees, info.source_url,
        ),
    )
    for ev in events:
        conn.execute(
            """INSERT INTO events
               (match_id, event_seq, quarter, minute, team, player_name,
                event_type, event_raw, counter, score_a, score_b,
                is_scoring, points)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                info.match_id, ev.event_seq, ev.quarter, ev.minute, ev.team,
                ev.player_name, ev.event_type, ev.event_raw, ev.counter,
                ev.score_a, ev.score_b, int(ev.is_scoring), ev.points,
            ),
        )
    for sub in subs:
        conn.execute(
            """INSERT INTO substitutions
               (match_id, event_seq, quarter, minute, team, player_in, player_out)
               VALUES (?,?,?,?,?,?,?)""",
            (
                info.match_id, sub.event_seq, sub.quarter, sub.minute,
                sub.team, sub.player_in, sub.player_out,
            ),
        )
    for to in timeouts:
        conn.execute(
            """INSERT INTO timeouts
               (match_id, event_seq, quarter, minute, team)
               VALUES (?,?,?,?,?)""",
            (info.match_id, to.event_seq, to.quarter, to.minute, to.team),
        )

    # Calculate and save player_stats (minutes, plus_minus, val, advanced)
    for side in ('A', 'B'):
        starters = set(get_starters(conn, info.match_id, side))
        minutes = get_playing_time(conn, info.match_id, side)
        pm = get_plus_minus(conn, info.match_id, side)
        val = get_val(conn, info.match_id, side)
        adv = get_advanced_stats(conn, info.match_id, side)
        # All players: union of all dicts
        all_players = set(minutes) | set(pm) | set(val) | set(adv)
        for player in all_players:
            a = adv.get(player, AdvancedStats())
            conn.execute(
                """INSERT INTO player_stats
                   (match_id, team, player_name, is_starter, minutes,
                    plus_minus, val,
                    ts_pct, efg_pct, game_score, usg_pct, ast_to, tov_pct)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    info.match_id, side, player,
                    int(player in starters),
                    minutes.get(player, 0),
                    pm.get(player, 0),
                    val.get(player, 0),
                    a.ts_pct, a.efg_pct, a.game_score,
                    a.usg_pct, a.ast_to, a.tov_pct,
                ),
            )

    # Save roster data from PDF scoresheet
    if roster:
        for side in ('A', 'B'):
            for entry in roster.get(side, []):
                conn.execute(
                    """INSERT OR IGNORE INTO rosters
                       (match_id, team, player_name, jersey_number, is_starter,
                        fouls_personal, fouls_technical, fouls_unsportsmanlike,
                        license_number)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        info.match_id, side, entry.player_name,
                        entry.jersey_number, int(entry.is_starter),
                        entry.fouls_personal, entry.fouls_technical,
                        entry.fouls_unsportsmanlike, entry.license_number,
                    ),
                )

    conn.commit()


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def fetch_pdf(season: str, comp: str, game_id: str) -> Optional[bytes]:
    """Download scoresheet PDF. Returns None if unavailable."""
    url = PDF_URL_TEMPLATE.format(season=season, comp=comp, game_id=game_id)
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        if len(resp.content) < 1000:
            return None
        return resp.content
    except requests.RequestException:
        return None


def _cluster_rows(chars: list, tol: float = 5.0) -> list:
    """Group PDF characters into rows by y-position proximity."""
    if not chars:
        return []
    sorted_chars = sorted(chars, key=lambda c: c['top'])
    rows = []
    current_row = [sorted_chars[0]]
    current_y = sorted_chars[0]['top']
    for c in sorted_chars[1:]:
        if abs(c['top'] - current_y) <= tol:
            current_row.append(c)
        else:
            rows.append(current_row)
            current_row = [c]
            current_y = c['top']
    rows.append(current_row)
    return rows


def _parse_team_roster(rows: list, y_min: float, y_max: float,
                       all_chars: list) -> list:
    """Extract roster entries from PDF character rows within y-range.

    Uses all_chars (full page) to find foul annotations (T/U) that appear
    in a sub-row ~13px below each player's main row.
    """
    entries = []
    for row_chars in rows:
        avg_y = sum(c['top'] for c in row_chars) / len(row_chars)
        if avg_y < y_min or avg_y > y_max:
            continue
        row_chars = sorted(row_chars, key=lambda c: c['x0'])

        # License number: x < 85, digits, at least 5 chars
        license_chars = [c for c in row_chars if c['x0'] < 85 and c['text'].isdigit()]
        if len(license_chars) < 5:
            continue
        license_num = ''.join(c['text'] for c in license_chars)

        # Player name: 85 < x < 270, non-digit chars
        name_chars = [c for c in row_chars if 85 < c['x0'] < 270 and not c['text'].isdigit()]
        name = ''.join(c['text'] for c in name_chars).strip()
        # Remove (KAP) suffix if present
        name = re.sub(r'\s*\(KAP\)\s*$', '', name)
        if not name:
            name_chars = [c for c in row_chars if 90 < c['x0'] < 280 and not c['text'].isdigit()]
            name = ''.join(c['text'] for c in name_chars).strip()
            name = re.sub(r'\s*\(KAP\)\s*$', '', name)

        # Jersey number: 265 < x < 330, digits
        jersey_chars = [c for c in row_chars if 265 < c['x0'] < 330 and c['text'].isdigit()]
        jersey_str = ''.join(c['text'] for c in jersey_chars)
        jersey = int(jersey_str) if jersey_str else None

        # Starter: X character at 325-370 with size >= 12 (larger font = starter)
        x_chars = [c for c in row_chars if c['text'] == 'X' and 325 < c['x0'] < 370]
        is_starter = any(c['size'] >= 12 for c in x_chars)

        # Foul analysis: foul columns at x=360-480
        # Main row has foul minute digits (size >= 12)
        foul_area = [c for c in row_chars if 360 < c['x0'] < 480]
        foul_main = [c for c in foul_area if c['text'].isdigit() and c['size'] >= 12]
        if foul_main:
            slot_positions = sorted(set(round(c['x0'] / 22) for c in foul_main))
            fouls_personal = len(slot_positions)
        else:
            fouls_personal = 0

        # Foul type annotations (T/U) appear ~13px BELOW the player row
        # in a sub-row with smaller font (size ~11). Scan all_chars in that zone.
        annot_y_min = avg_y + 8
        annot_y_max = avg_y + 20
        foul_annots = [
            c for c in all_chars
            if annot_y_min < c['top'] < annot_y_max
            and 360 < c['x0'] < 480
            and c['size'] < 12
            and c['text'] in ('T', 'U')
        ]
        fouls_technical = sum(1 for c in foul_annots if c['text'] == 'T')
        fouls_unsportsmanlike = sum(1 for c in foul_annots if c['text'] == 'U')

        entries.append(RosterEntry(
            player_name=name,
            jersey_number=jersey,
            is_starter=is_starter,
            fouls_personal=fouls_personal,
            fouls_technical=fouls_technical,
            fouls_unsportsmanlike=fouls_unsportsmanlike,
            license_number=license_num,
        ))
    return entries


def parse_roster_pdf(pdf_bytes: bytes) -> Optional[dict]:
    """Parse scoresheet PDF and extract roster data for both teams."""
    if not HAS_PDFPLUMBER:
        return None
    pdf = pdfplumber.open(BytesIO(pdf_bytes))
    if not pdf.pages:
        pdf.close()
        return None
    page = pdf.pages[0]
    chars = page.chars
    if not chars:
        pdf.close()
        return None

    rows = _cluster_rows(chars)

    # Find team section markers: '"A" Csapat' and '"B" Csapat' y-positions
    team_a_y = None
    team_b_y = None
    for row_chars in rows:
        text = ''.join(c['text'] for c in sorted(row_chars, key=lambda c: c['x0']))
        if '"A" Csapat' in text or '"A"Csapat' in text:
            team_a_y = sum(c['top'] for c in row_chars) / len(row_chars)
        elif '"B" Csapat' in text or '"B"Csapat' in text:
            team_b_y = sum(c['top'] for c in row_chars) / len(row_chars)

    if team_a_y is None or team_b_y is None:
        pdf.close()
        return None

    page_height = page.height or 1700

    # Parse rosters: Team A between its marker+150 and Team B marker-50
    # Team B between its marker+150 and page bottom-100
    roster_a = _parse_team_roster(rows, team_a_y + 150, team_b_y - 50, chars)
    roster_b = _parse_team_roster(rows, team_b_y + 150, page_height - 100, chars)

    pdf.close()
    return {'A': roster_a, 'B': roster_b}


# ---------------------------------------------------------------------------
# Match header parsing
# ---------------------------------------------------------------------------


def parse_date(raw: str) -> tuple[str, str]:
    """Parse '2026. március 4. | 17:30' → ('2026-03-04', '17:30')."""
    raw = raw.strip()
    m = re.match(
        r"(\d{4})\.\s+(\w+)\s+(\d{1,2})\.\s*\|\s*(\d{1,2}:\d{2})", raw
    )
    if not m:
        return ("", "")
    year = int(m.group(1))
    month = HU_MONTHS.get(m.group(2).lower(), 0)
    day = int(m.group(3))
    time_str = m.group(4)
    return (f"{year:04d}-{month:02d}-{day:02d}", time_str)


def parse_quarter_scores(raw: str) -> list[list[int]]:
    """Parse '(16-25, 18-21, 20-23, 17-24)' → [[16,25],[18,21],...]."""
    raw = raw.strip().strip("()")
    result = []
    for part in raw.split(","):
        part = part.strip()
        m = re.match(r"(\d+)\s*-\s*(\d+)", part)
        if m:
            result.append([int(m.group(1)), int(m.group(2))])
    return result


def parse_match_header(soup: BeautifulSoup, comp_code: str, season: str,
                       game_id: str, source_url: str) -> MatchInfo:
    match_id = f"{comp_code}_{game_id}"

    # Competition name from box-header
    comp_name_el = soup.select_one("#pbp .box-header")
    comp_name = comp_name_el.get_text(strip=True) if comp_name_el else ""

    # Header container
    head = soup.select_one(".pbp-head-cont")

    # Date & time
    date_el = head.select_one(".pbp-head-date")
    date_str, time_str = parse_date(date_el.get_text() if date_el else "")

    # Round
    round_el = head.select_one(".pbp-head-round")
    round_name = round_el.get_text(strip=True) if round_el else ""

    # Venue
    venue_el = head.select_one(".pbp-head-stadium")
    venue = venue_el.get_text(strip=True) if venue_el else ""

    # Team short names
    team_a_el = head.select_one(".pbp-head-name.home")
    team_b_el = head.select_one(".pbp-head-name.away")
    team_a = team_a_el.get_text(strip=True) if team_a_el else ""
    team_b = team_b_el.get_text(strip=True) if team_b_el else ""

    # Full team names from tablepbp header row
    table = soup.find("table", class_="tablepbp")
    header_row = table.find("tr") if table else None
    if header_row:
        header_cells = header_row.find_all("td")
        team_a_full = header_cells[0].get_text(strip=True) if len(header_cells) > 0 else team_a
        team_b_full = header_cells[-1].get_text(strip=True) if len(header_cells) > 2 else team_b
    else:
        team_a_full, team_b_full = team_a, team_b

    # Score
    score_el = head.select_one(".pbp-head-result-cont")
    score_text = score_el.get_text(strip=True) if score_el else "0-0"
    sm = re.match(r"(\d+)\s*-\s*(\d+)", score_text)
    score_a = int(sm.group(1)) if sm else 0
    score_b = int(sm.group(2)) if sm else 0

    # Quarter scores
    quarters_el = head.select_one(".pbp-head-quarters")
    quarter_scores = parse_quarter_scores(
        quarters_el.get_text() if quarters_el else ""
    )

    # Referees
    ref_el = head.select_one(".pbp-head-ref")
    referees = ""
    if ref_el:
        referees = ref_el.get_text(strip=True)
        # Remove prefix
        referees = re.sub(
            r"^Játékvezetők és MKOSZ ELLENŐR:\s*", "", referees
        )

    return MatchInfo(
        match_id=match_id,
        comp_code=comp_code,
        season=season,
        comp_name=comp_name,
        round_name=round_name,
        match_date=date_str,
        match_time=time_str,
        venue=venue,
        team_a=team_a,
        team_b=team_b,
        team_a_full=team_a_full,
        team_b_full=team_b_full,
        score_a=score_a,
        score_b=score_b,
        quarter_scores=quarter_scores,
        referees=referees,
        source_url=source_url,
    )


# ---------------------------------------------------------------------------
# Event text parsing helpers
# ---------------------------------------------------------------------------

# Regex for "PlayerName  -  \n EventType (counter)"
RE_PLAYER_EVENT = re.compile(
    r"^\s*(.+?)\s+-\s+(.+?)(?:\s*\((\d+)\))?\s*$", re.DOTALL
)

# Regex for substitution: "PlayerIn\ncsere -   PlayerOut"
RE_SUBSTITUTION = re.compile(
    r"^\s*(.+?)\s*\n\s*csere\s*-\s+(.+?)\s*$", re.DOTALL
)

# Regex for timeout: "TeamName időkérés"
RE_TIMEOUT = re.compile(r"^(.+?)\s+időkérés\s*$", re.DOTALL)

# Regex for score span
RE_SCORE_SPAN = re.compile(r"(\d+)\s*-\s*(\d+)")

# Regex for minute header
RE_MINUTE = re.compile(r"(\d+)\.\s*perc")


def normalize_event_type(raw: str) -> str:
    """Normalize Hungarian event type to code. Returns 'UNKNOWN' if not found."""
    cleaned = raw.strip().lower()
    # Remove trailing whitespace and newlines
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return EVENT_TYPES.get(cleaned, "UNKNOWN")


def parse_event_text(text: str) -> tuple[Optional[str], str, Optional[int]]:
    """Parse 'PlayerName - EventType (counter)' → (player, event_raw, counter)."""
    text = text.strip()
    m = RE_PLAYER_EVENT.match(text)
    if m:
        player = m.group(1).strip()
        event_raw = m.group(2).strip()
        counter = int(m.group(3)) if m.group(3) else None
        # Clean up multi-whitespace in event_raw
        event_raw = re.sub(r"\s+", " ", event_raw).strip()
        return (player, event_raw, counter)
    return (None, text, None)


def detect_team_event(text: str, team_a_names: list[str],
                      team_b_names: list[str]) -> Optional[tuple[str, str]]:
    """
    Detect team-level events like 'Peka Bau-MEAFC Wolves Támadólepattanó'.
    Returns (team 'A'/'B', event_type_raw) or None.
    """
    text_clean = text.strip()
    # Remove trailing newlines
    text_clean = re.sub(r"\s+$", "", text_clean)

    for name in team_b_names:
        if text_clean.startswith(name):
            remainder = text_clean[len(name):].strip()
            if remainder:
                return ("B", remainder)
    for name in team_a_names:
        if text_clean.startswith(name):
            remainder = text_clean[len(name):].strip()
            if remainder:
                return ("A", remainder)
    return None


# ---------------------------------------------------------------------------
# Main event parsing
# ---------------------------------------------------------------------------


def parse_events(
    soup: BeautifulSoup, match_info: MatchInfo
) -> tuple[list[PBPEvent], list[Substitution], list[Timeout]]:
    """Parse all play-by-play events from the tablepbp table."""

    table = soup.find("table", class_="tablepbp")
    if not table:
        print("HIBA: tablepbp tábla nem található!", file=sys.stderr)
        return [], [], []

    rows = table.find_all("tr")

    # Team name variants for detecting team-level events and timeouts
    team_a_names = [match_info.team_a, match_info.team_a_full]
    team_b_names = [match_info.team_b, match_info.team_b_full]
    # Deduplicate while preserving order (longer names first for matching)
    team_a_names = sorted(set(team_a_names), key=len, reverse=True)
    team_b_names = sorted(set(team_b_names), key=len, reverse=True)

    events: list[PBPEvent] = []
    subs: list[Substitution] = []
    timeouts: list[Timeout] = []

    quarter = 1
    minute = 1
    event_seq = 0
    running_score_a = 0
    running_score_b = 0
    warnings: list[str] = []

    for row in rows:
        if not isinstance(row, Tag):
            continue

        # --- Minute header ---
        bgcolor = (row.get("bgcolor") or "").lower()
        if bgcolor == "#dddddd":
            cells = row.find_all("td")
            for cell in cells:
                txt = cell.get_text(strip=True)
                mm = RE_MINUTE.match(txt)
                if mm:
                    minute = int(mm.group(1))
            continue

        # --- Quarter end ---
        if bgcolor == "#a00000":
            quarter += 1
            continue

        # --- Event row ---
        classes = row.get("class", [])
        if "rankttext" not in classes:
            continue

        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        left_cell = cells[0]
        center_cell = cells[1]
        right_cell = cells[2]

        # Get text with newlines preserved (for substitution detection)
        left_text = left_cell.get_text("\n", strip=False).strip()
        right_text = right_cell.get_text("\n", strip=False).strip()

        # Skip "Welcome to netcasting" row
        if "welcome to netcasting" in left_text.lower():
            continue

        # Parse center cell for score
        center_html = str(center_cell)
        score_match = None
        span = center_cell.find("span")
        if span:
            span_text = span.get_text(strip=True)
            score_match = RE_SCORE_SPAN.match(span_text)

        row_score_a = None
        row_score_b = None
        if score_match:
            row_score_a = int(score_match.group(1))
            row_score_b = int(score_match.group(2))

        # Process each side (left = team A, right = team B)
        for side, cell, text in [("A", left_cell, left_text),
                                  ("B", right_cell, right_text)]:
            if not text:
                continue

            event_seq += 1

            # Check substitution
            sub_match = RE_SUBSTITUTION.match(text)
            if sub_match:
                player_in = sub_match.group(1).strip()
                player_out = sub_match.group(2).strip()
                subs.append(Substitution(
                    event_seq=event_seq,
                    quarter=quarter,
                    minute=minute,
                    team=side,
                    player_in=player_in,
                    player_out=player_out,
                ))
                continue

            # Check timeout
            to_match = RE_TIMEOUT.match(text)
            if to_match:
                team_name = to_match.group(1).strip()
                # Determine which team
                to_team = side  # default to the side it's on
                for name in team_a_names:
                    if team_name == name:
                        to_team = "A"
                        break
                for name in team_b_names:
                    if team_name == name:
                        to_team = "B"
                        break
                timeouts.append(Timeout(
                    event_seq=event_seq,
                    quarter=quarter,
                    minute=minute,
                    team=to_team,
                ))
                continue

            # Check team-level event (no dash separator)
            team_ev = detect_team_event(text, team_a_names, team_b_names)
            if team_ev and " - " not in text:
                te_team, te_raw = team_ev
                te_code = normalize_event_type(te_raw)
                if te_code == "UNKNOWN":
                    warnings.append(
                        f"  FIGYELEM: Ismeretlen team event: '{te_raw}'"
                    )

                is_scoring = te_code in POINTS_MAP
                points = POINTS_MAP.get(te_code, 0)

                ev_score_a = None
                ev_score_b = None
                if is_scoring and row_score_a is not None:
                    ev_score_a = row_score_a
                    ev_score_b = row_score_b
                    running_score_a = row_score_a
                    running_score_b = row_score_b

                events.append(PBPEvent(
                    event_seq=event_seq,
                    quarter=quarter,
                    minute=minute,
                    team=side,  # use column side
                    player_name=None,
                    event_type=te_code,
                    event_raw=te_raw,
                    counter=None,
                    score_a=ev_score_a,
                    score_b=ev_score_b,
                    is_scoring=is_scoring,
                    points=points,
                ))
                continue

            # Regular player event
            player, event_raw, counter = parse_event_text(text)
            event_code = normalize_event_type(event_raw)

            if event_code == "UNKNOWN":
                warnings.append(
                    f"  FIGYELEM: Ismeretlen event: '{event_raw}' "
                    f"(seq={event_seq}, Q{quarter} {minute}. perc)"
                )

            # Determine if scoring
            has_bold = bool(cell.find("b"))
            is_scoring = event_code in POINTS_MAP and has_bold

            ev_score_a = None
            ev_score_b = None
            points = 0

            if is_scoring and row_score_a is not None:
                # Use score from center cell
                ev_score_a = row_score_a
                ev_score_b = row_score_b
                # Calculate points from delta
                if side == "A":
                    points = row_score_a - running_score_a
                else:
                    points = row_score_b - running_score_b
                # Fallback to POINTS_MAP if delta doesn't make sense
                if points <= 0:
                    points = POINTS_MAP.get(event_code, 0)
                running_score_a = row_score_a
                running_score_b = row_score_b
            elif is_scoring:
                # No score in center cell, use POINTS_MAP
                points = POINTS_MAP.get(event_code, 0)
                if side == "A":
                    running_score_a += points
                else:
                    running_score_b += points
                ev_score_a = running_score_a
                ev_score_b = running_score_b

            events.append(PBPEvent(
                event_seq=event_seq,
                quarter=quarter,
                minute=minute,
                team=side,
                player_name=player,
                event_type=event_code,
                event_raw=event_raw,
                counter=counter,
                score_a=ev_score_a,
                score_b=ev_score_b,
                is_scoring=is_scoring,
                points=points,
            ))

    # Print warnings
    for w in warnings:
        print(w, file=sys.stderr)

    return events, subs, timeouts


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_match(
    info: MatchInfo, events: list[PBPEvent]
) -> list[str]:
    errors = []

    scoring = [e for e in events if e.is_scoring]
    if not scoring:
        errors.append("Nincs pontozási esemény!")
        return errors

    # Final score
    last = scoring[-1]
    if last.score_a != info.score_a or last.score_b != info.score_b:
        errors.append(
            f"Végeredmény eltérés: PBP {last.score_a}-{last.score_b} "
            f"vs fejléc {info.score_a}-{info.score_b}"
        )

    # Quarter scores
    for q_idx, (exp_a, exp_b) in enumerate(info.quarter_scores):
        q = q_idx + 1
        q_events = [e for e in events if e.quarter == q and e.is_scoring]
        got_a = sum(e.points for e in q_events if e.team == "A")
        got_b = sum(e.points for e in q_events if e.team == "B")
        if got_a != exp_a or got_b != exp_b:
            errors.append(
                f"Q{q} eltérés: PBP {got_a}-{got_b} vs fejléc {exp_a}-{exp_b}"
            )

    # Unknown events
    unknown = [e for e in events if e.event_type == "UNKNOWN"]
    if unknown:
        errors.append(f"{len(unknown)} ismeretlen eseménytípus")

    return errors


# ---------------------------------------------------------------------------
# Starter detection
# ---------------------------------------------------------------------------


def get_starters(conn: sqlite3.Connection, match_id: str,
                 team: str) -> list[str]:
    """
    Detect starters for a team in a match.

    A player is a starter if:
    - They were subbed OUT before ever being subbed IN, OR
    - They appeared in events before ever being subbed IN, OR
    - They were never subbed IN but have events or were subbed OUT
    """
    rows = conn.execute("""
        WITH
        first_sub_in AS (
            SELECT player_in AS player, MIN(event_seq) AS seq
            FROM substitutions
            WHERE match_id = ? AND team = ?
            GROUP BY player_in
        ),
        first_sub_out AS (
            SELECT player_out AS player, MIN(event_seq) AS seq
            FROM substitutions
            WHERE match_id = ? AND team = ?
            GROUP BY player_out
        ),
        first_event AS (
            SELECT player_name AS player, MIN(event_seq) AS seq
            FROM events
            WHERE match_id = ? AND team = ? AND player_name IS NOT NULL
            GROUP BY player_name
        ),
        all_players AS (
            SELECT player FROM first_sub_in
            UNION
            SELECT player FROM first_sub_out
            UNION
            SELECT player FROM first_event
        )
        SELECT ap.player
        FROM all_players ap
        LEFT JOIN first_sub_in fsi ON fsi.player = ap.player
        LEFT JOIN first_sub_out fso ON fso.player = ap.player
        LEFT JOIN first_event fe ON fe.player = ap.player
        WHERE
            -- Never subbed IN but has events or was subbed out
            (fsi.seq IS NULL AND (fe.seq IS NOT NULL OR fso.seq IS NOT NULL))
            -- Subbed OUT before first sub IN
            OR (fso.seq IS NOT NULL AND (fsi.seq IS NULL OR fso.seq < fsi.seq))
            -- Event before first sub IN
            OR (fe.seq IS NOT NULL AND (fsi.seq IS NULL OR fe.seq < fsi.seq))
        ORDER BY COALESCE(fe.seq, fso.seq)
    """, (match_id, team, match_id, team, match_id, team)).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Playing time calculation
# ---------------------------------------------------------------------------


def get_playing_time(conn: sqlite3.Connection, match_id: str,
                     team: str) -> dict[str, int]:
    """
    Calculate approximate playing time (in minutes) for each player.

    Uses substitution data + starter detection. Precision: ±1 minute
    (PBP only has whole-minute markers, no seconds).

    Returns dict: player_name → minutes played.
    """
    # Get game length (40 for regulation, 45 for OT, etc.)
    row = conn.execute(
        "SELECT quarter_scores FROM matches WHERE match_id = ?",
        (match_id,)
    ).fetchone()
    quarters = json.loads(row[0]) if row else []
    game_end = len(quarters) * 10  # 10 min per quarter

    # Get starters
    starters = set(get_starters(conn, match_id, team))

    # Get all substitutions ordered by event_seq
    subs = conn.execute("""
        SELECT player_in, player_out, minute
        FROM substitutions
        WHERE match_id = ? AND team = ?
        ORDER BY event_seq
    """, (match_id, team)).fetchall()

    # Track who is on court and when they entered
    # on_court: player → minute they came on
    on_court: dict[str, int] = {}
    minutes_played: dict[str, int] = {}

    # Starters enter at minute 0 (game start, before "1. perc")
    for p in starters:
        on_court[p] = 0
        minutes_played[p] = 0

    # Process substitutions
    for player_in, player_out, minute in subs:
        # Player going out: accumulate time
        if player_out in on_court:
            entered = on_court.pop(player_out)
            minutes_played.setdefault(player_out, 0)
            minutes_played[player_out] += minute - entered
        # Player coming in: start tracking
        on_court[player_in] = minute
        minutes_played.setdefault(player_in, 0)

    # End of game: all remaining on-court players get time until game_end
    for p, entered in on_court.items():
        minutes_played.setdefault(p, 0)
        minutes_played[p] += game_end - entered

    return minutes_played


# ---------------------------------------------------------------------------
# Plus-minus calculation
# ---------------------------------------------------------------------------


def get_plus_minus(conn: sqlite3.Connection, match_id: str,
                   team: str) -> dict[str, int]:
    """
    Calculate plus-minus (+/-) for each player on a given team.

    PlusMinus = TeamPointsWhilePlayerOnCourt - OpponentPointsWhilePlayerOnCourt

    Tracks who is on court via starters + substitutions, then for every
    scoring event (from either team) adjusts each on-court player's +/-.
    """
    # Get starters
    starters = set(get_starters(conn, match_id, team))

    # Get substitutions for this team, ordered by event_seq
    subs = conn.execute("""
        SELECT event_seq, player_in, player_out
        FROM substitutions
        WHERE match_id = ? AND team = ?
        ORDER BY event_seq
    """, (match_id, team)).fetchall()

    # Get ALL scoring events (both teams), ordered by event_seq
    scoring = conn.execute("""
        SELECT event_seq, team, points
        FROM events
        WHERE match_id = ? AND is_scoring = 1
        ORDER BY event_seq
    """, (match_id,)).fetchall()

    # Build interleaved timeline: (event_seq, priority, type, data)
    # Subs get priority 0 so they are processed before scoring at same seq
    timeline: list[tuple[int, int, str, tuple]] = []
    for seq, p_in, p_out in subs:
        timeline.append((seq, 0, 'sub', (p_in, p_out)))
    for seq, t, pts in scoring:
        timeline.append((seq, 1, 'score', (t, pts)))
    timeline.sort(key=lambda x: (x[0], x[1]))

    # Track who is on court
    on_court = set(starters)
    plus_minus: dict[str, int] = {p: 0 for p in starters}

    for _seq, _pri, typ, data in timeline:
        if typ == 'sub':
            p_in, p_out = data
            on_court.discard(p_out)
            on_court.add(p_in)
            plus_minus.setdefault(p_in, 0)
        else:  # score
            scoring_team, pts = data
            for p in on_court:
                if scoring_team == team:
                    plus_minus[p] += pts
                else:
                    plus_minus[p] -= pts

    return plus_minus


# ---------------------------------------------------------------------------
# VAL (Performance Index Rating) calculation
# ---------------------------------------------------------------------------

# Event types that contribute positively or negatively to VAL
_VAL_POSITIVE = {
    # PTS: handled via POINTS_MAP
    "OREB", "DREB",       # REB
    "AST",                # AST
    "STL",                # STL
    "BLK",                # BLK
    "FOUL_DRAWN",         # FD (fouls drawn)
}
_VAL_NEGATIVE = {
    # Missed FG: CLOSE_MISS, MID_MISS, THREE_MISS, DUNK_MISS
    "CLOSE_MISS", "MID_MISS", "THREE_MISS", "DUNK_MISS",
    # Missed FT
    "FT_MISS",
    # Turnovers
    "TOV",
    # Blocked against (player's shot was blocked)
    "BLK_RECV",
    # Personal fouls committed (NB: MKOSZ official VAL does not penalise PF,
    # but we include it for a stricter evaluation)
    "FOUL",
}


def get_val(conn: sqlite3.Connection, match_id: str,
            team: str) -> dict[str, int]:
    """
    Calculate VAL (Performance Index Rating) for each player on a team.

    VAL = PTS + REB + AST + STL + BLK + FD
          - (MissedFG + MissedFT + TO + BLKAgainst + PF)
    """
    rows = conn.execute("""
        SELECT player_name, event_type, points
        FROM events
        WHERE match_id = ? AND team = ? AND player_name IS NOT NULL
        ORDER BY event_seq
    """, (match_id, team)).fetchall()

    val: dict[str, int] = {}
    for player, event_type, points in rows:
        val.setdefault(player, 0)
        # Points scored (PTS component)
        if points and points > 0:
            val[player] += points
        # Positive box-score stats
        if event_type in _VAL_POSITIVE:
            val[player] += 1
        # Negative box-score stats
        if event_type in _VAL_NEGATIVE:
            val[player] -= 1

    return val


# ---------------------------------------------------------------------------
# Advanced / composite stats (összetett mutatók)
# ---------------------------------------------------------------------------

# Shot event type sets for convenience
_FG_MADE = {"CLOSE_MADE", "DUNK_MADE", "MID_MADE", "THREE_MADE"}
_FG_MISS = {"CLOSE_MISS", "DUNK_MISS", "MID_MISS", "THREE_MISS"}
_THREE_MADE = {"THREE_MADE"}
_FT_MADE = {"FT_MADE"}
_FT_MISS = {"FT_MISS"}


@dataclass
class AdvancedStats:
    ts_pct: Optional[float] = None      # True Shooting %
    efg_pct: Optional[float] = None     # Effective FG %
    game_score: float = 0.0             # Hollinger Game Score
    usg_pct: Optional[float] = None     # Usage Rate %
    ast_to: Optional[float] = None      # AST/TO ratio
    tov_pct: Optional[float] = None     # Turnover Rate %


def get_advanced_stats(conn: sqlite3.Connection, match_id: str,
                       team: str) -> dict[str, AdvancedStats]:
    """
    Calculate advanced/composite stats for each player on a team.

    TS%  = PTS / (2 × (FGA + 0.44 × FTA))
    eFG% = (FGM + 0.5 × 3PM) / FGA
    GmSc = PTS + 0.4×FGM − 0.7×FGA − 0.4×(FTA−FTM) + 0.7×OREB
           + 0.3×DREB + STL + 0.7×AST + 0.7×BLK − 0.4×PF − TOV
    USG% = 100 × ((FGA + 0.44×FTA + TOV) × TeamMin/5)
                / (PlayerMin × TeamPoss)
    AST/TO = AST / TOV
    TOV% = 100 × TOV / (FGA + 0.44×FTA + TOV)
    """
    # Gather per-player raw counts
    rows = conn.execute("""
        SELECT player_name, event_type, points
        FROM events
        WHERE match_id = ? AND team = ? AND player_name IS NOT NULL
        ORDER BY event_seq
    """, (match_id, team)).fetchall()

    # Accumulate raw stats per player
    raw: dict[str, dict[str, int]] = {}
    for player, etype, pts in rows:
        if player not in raw:
            raw[player] = {
                "pts": 0, "fgm": 0, "fga": 0, "three_m": 0,
                "ftm": 0, "fta": 0, "oreb": 0, "dreb": 0,
                "ast": 0, "stl": 0, "blk": 0, "tov": 0, "pf": 0,
            }
        r = raw[player]
        if pts and pts > 0:
            r["pts"] += pts
        if etype in _FG_MADE:
            r["fgm"] += 1
            r["fga"] += 1
        elif etype in _FG_MISS:
            r["fga"] += 1
        if etype in _THREE_MADE:
            r["three_m"] += 1
        if etype in _FT_MADE:
            r["ftm"] += 1
            r["fta"] += 1
        elif etype in _FT_MISS:
            r["fta"] += 1
        if etype == "OREB":
            r["oreb"] += 1
        elif etype == "DREB":
            r["dreb"] += 1
        elif etype == "AST":
            r["ast"] += 1
        elif etype == "STL":
            r["stl"] += 1
        elif etype == "BLK":
            r["blk"] += 1
        elif etype == "TOV":
            r["tov"] += 1
        elif etype == "FOUL":
            r["pf"] += 1

    # Team totals for USG% calculation
    team_fga = sum(r["fga"] for r in raw.values())
    team_fta = sum(r["fta"] for r in raw.values())
    team_tov = sum(r["tov"] for r in raw.values())
    team_poss = team_fga + 0.44 * team_fta + team_tov

    # Get playing time for USG%
    minutes = get_playing_time(conn, match_id, team)
    team_minutes = sum(minutes.values())

    result: dict[str, AdvancedStats] = {}
    for player, r in raw.items():
        s = AdvancedStats()
        fga = r["fga"]
        fta = r["fta"]

        # TS% = PTS / (2 × (FGA + 0.44 × FTA))
        ts_denom = 2 * (fga + 0.44 * fta)
        if ts_denom > 0:
            s.ts_pct = r["pts"] / ts_denom

        # eFG% = (FGM + 0.5 × 3PM) / FGA
        if fga > 0:
            s.efg_pct = (r["fgm"] + 0.5 * r["three_m"]) / fga

        # Game Score
        s.game_score = (
            r["pts"]
            + 0.4 * r["fgm"]
            - 0.7 * fga
            - 0.4 * (fta - r["ftm"])
            + 0.7 * r["oreb"]
            + 0.3 * r["dreb"]
            + r["stl"]
            + 0.7 * r["ast"]
            + 0.7 * r["blk"]
            - 0.4 * r["pf"]
            - r["tov"]
        )

        # USG% = 100 × ((FGA + 0.44×FTA + TOV) × TeamMin/5) / (PlayerMin × TeamPoss)
        player_min = minutes.get(player, 0)
        player_poss = fga + 0.44 * fta + r["tov"]
        if player_min > 0 and team_poss > 0:
            s.usg_pct = 100 * (player_poss * (team_minutes / 5)) / (player_min * team_poss)

        # AST/TO
        if r["tov"] > 0:
            s.ast_to = r["ast"] / r["tov"]
        elif r["ast"] > 0:
            s.ast_to = float(r["ast"])  # infinite ratio → just show AST count

        # TOV% = 100 × TOV / (FGA + 0.44×FTA + TOV)
        tov_denom = fga + 0.44 * fta + r["tov"]
        if tov_denom > 0:
            s.tov_pct = 100 * r["tov"] / tov_denom

        result[player] = s

    return result


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


def parse_url(url: str) -> tuple[str, str, str]:
    """Extract (season, comp, game_id) from a PBP URL."""
    m = re.search(
        r"/merkozes-esemenylista/([^/]+)/([^/]+)/\2_(\d+)", url
    )
    if not m:
        print(f"HIBA: Nem sikerült parse-olni az URL-t: {url}", file=sys.stderr)
        sys.exit(1)
    return m.group(1), m.group(2), m.group(3)


def build_url(season: str, comp: str, game_id: str) -> str:
    return PBP_URL_TEMPLATE.format(season=season, comp=comp, game_id=game_id)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def process_match(
    season: str, comp: str, game_id: str, db_path: str, force: bool = False
):
    match_id = f"{comp}_{game_id}"
    url = build_url(season, comp, game_id)

    print(f"Feldolgozás: {url}")

    # Check existing
    conn = create_db(db_path)
    if match_exists(conn, match_id):
        if force:
            print(f"  Meglévő adat törlése: {match_id}")
            delete_match(conn, match_id)
        else:
            print(f"  Már feldolgozva: {match_id} (használd --force-t az újrafeldolgozáshoz)")
            conn.close()
            return

    # Fetch
    t0 = time.time()
    html = fetch_html(url)
    fetch_ms = int((time.time() - t0) * 1000)
    print(f"  Letöltve ({fetch_ms} ms, {len(html)} byte)")

    # Parse
    soup = BeautifulSoup(html, "html.parser")
    info = parse_match_header(soup, comp, season, game_id, url)
    events, subs, timeouts = parse_events(soup, info)

    # Stats
    scoring_count = sum(1 for e in events if e.is_scoring)
    print(
        f"  Meccs: {info.team_a} vs {info.team_b} ({info.score_a}-{info.score_b})"
    )
    print(f"  Dátum: {info.match_date} {info.match_time}")
    print(f"  Helyszín: {info.venue}")
    print(
        f"  {len(events)} esemény ({scoring_count} pontozás, "
        f"{len(subs)} csere, {len(timeouts)} időkérés)"
    )

    # Validate
    errors = validate_match(info, events)
    if errors:
        print("  VALIDÁCIÓ HIBÁK:")
        for err in errors:
            print(f"    - {err}")
    else:
        qs = " | ".join(
            f"Q{i+1} {a}-{b} OK"
            for i, (a, b) in enumerate(info.quarter_scores)
        )
        print(f"  Validáció: OK ({info.score_a}-{info.score_b})")
        print(f"  Negyedek: {qs}")

    # PDF scoresheet (roster, jersey numbers, foul types)
    roster = None
    if info.score_a and info.score_a > 0:
        pdf_bytes = fetch_pdf(season, comp, game_id)
        if pdf_bytes:
            try:
                roster = parse_roster_pdf(pdf_bytes)
                if roster:
                    n = sum(len(v) for v in roster.values())
                    print(f"  Jegyzőkönyv: {n} játékos")
            except Exception as e:
                print(f"  Jegyzőkönyv hiba: {e}")

    # Save
    save_match(conn, info, events, subs, timeouts, roster=roster)
    print(f"  Mentve: {db_path}")
    conn.close()


# ---------------------------------------------------------------------------
# Batch processing — discover all game IDs for a competition
# ---------------------------------------------------------------------------


def discover_game_ids(season: str, comp: str) -> list[int]:
    """Fetch the MKOSZ schedule page and extract all game IDs."""
    url = SCHEDULE_URL.format(season=season, comp=comp)
    print(f"Műsor oldal letöltése: {url}")

    html = fetch_html(url)
    pattern = rf"{re.escape(comp)}_(\d+)"
    ids = sorted(set(int(m) for m in re.findall(pattern, html)))
    print(f"  {len(ids)} meccs azonosító találva")
    return ids


def process_batch(
    season: str, comp: str, db_path: str, force: bool = False
):
    """Process all matches for a competition in batch mode."""
    game_ids = discover_game_ids(season, comp)
    if not game_ids:
        print("Nem található meccs azonosító.")
        return

    conn = create_db(db_path)
    total = len(game_ids)
    processed = 0
    skipped = 0
    errors = 0

    for i, gid in enumerate(game_ids, 1):
        game_id = str(gid)
        match_id = f"{comp}_{game_id}"

        if not force and match_exists(conn, match_id):
            skipped += 1
            print(f"  [{i:>{len(str(total))}}/{total}] {match_id} — már feldolgozva")
            if i < total:
                time.sleep(BATCH_DELAY)
            continue

        try:
            conn.close()  # process_match opens its own connection
            process_match(season, comp, game_id, db_path, force=force)
            processed += 1
        except Exception as e:
            print(f"  [{i:>{len(str(total))}}/{total}] {match_id} — HIBA: {e}",
                  file=sys.stderr)
            errors += 1

        conn = create_db(db_path)

        if i < total:
            time.sleep(BATCH_DELAY)

    conn.close()

    print()
    print(f"Összesen: {total} meccs")
    print(f"  Feldolgozva:  {processed}")
    print(f"  Már megvolt:  {skipped}")
    if errors:
        print(f"  Hibás:        {errors}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="MKOSZ Play-by-Play parser — eseménylista feldolgozó"
    )
    parser.add_argument(
        "--url", help="Teljes MKOSZ eseménylista URL"
    )
    parser.add_argument(
        "--season", help="Szezon kód (pl. x2526)"
    )
    parser.add_argument(
        "--comp", help="Bajnokság kód (pl. hun2a)"
    )
    parser.add_argument(
        "--game-id", help="Meccs azonosító (pl. 123749). Ha nincs megadva --season + --comp mellett, batch mód."
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"SQLite adatbázis elérési út (alapértelmezett: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Meglévő adat felülírása"
    )
    parser.add_argument(
        "--starters", action="store_true",
        help="Kezdőötösök megjelenítése (már feldolgozott meccshez)"
    )
    parser.add_argument(
        "--list-only", action="store_true",
        help="Csak a meccs azonosítók kilistázása (batch módban)"
    )

    args = parser.parse_args()

    # Batch mode: --season + --comp without --game-id
    if args.season and args.comp and not args.game_id and not args.url:
        if args.list_only:
            ids = discover_game_ids(args.season, args.comp)
            if ids:
                for gid in ids:
                    print(f"  {args.comp}_{gid}")
                print(f"\nÖsszesen: {len(ids)} meccs")
            return
        process_batch(args.season, args.comp, args.db, force=args.force)
        return

    if args.url:
        season, comp, game_id = parse_url(args.url)
    elif args.season and args.comp and args.game_id:
        season, comp, game_id = args.season, args.comp, args.game_id
    else:
        parser.error(
            "Add meg a --url paramétert, a --season + --comp + --game-id "
            "hármast, vagy --season + --comp (batch mód)!"
        )
        return

    if args.starters:
        match_id = f"{comp}_{game_id}"
        conn = create_db(args.db)
        if not match_exists(conn, match_id):
            print(f"HIBA: {match_id} nincs az adatbázisban!", file=sys.stderr)
            conn.close()
            return
        row = conn.execute(
            "SELECT team_a, team_b FROM matches WHERE match_id = ?",
            (match_id,)
        ).fetchone()
        for side, name in [("A", row[0]), ("B", row[1])]:
            starters = get_starters(conn, match_id, side)
            print(f"\n{name} kezdőötös:")
            for i, p in enumerate(starters, 1):
                print(f"  {i}. {p}")
        conn.close()
        return

    process_match(season, comp, game_id, args.db, force=args.force)


if __name__ == "__main__":
    main()
