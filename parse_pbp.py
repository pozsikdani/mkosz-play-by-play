#!/usr/bin/env python3
"""
MKOSZ Play-by-Play parser.
Scrapes play-by-play event lists from mkosz.hu and stores them in SQLite.

Usage:
    python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_123749
    python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749
    python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749 --db custom.sqlite
    python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749 --force
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PBP_URL_TEMPLATE = "https://mkosz.hu/merkozes-esemenylista/{season}/{comp}/{comp}_{game_id}"
DEFAULT_DB = "pbp.sqlite"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Hungarian month names for date parsing
HU_MONTHS = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}

# Event type mapping: Hungarian → normalized code
EVENT_TYPES = {
    # Scoring (made)
    "sikeres közeli": "FG2_MADE",
    "sikeres középtávoli": "FG2M_MADE",
    "sikeres hárompontos": "FG3_MADE",
    "sikeres büntető": "FT_MADE",
    "sikeres zsákolás": "DUNK_MADE",
    # Missed
    "sikertelen közeli": "FG2_MISS",
    "sikertelen középtávoli": "FG2M_MISS",
    "sikertelen hárompontos": "FG3_MISS",
    "kihagyott büntető": "FT_MISS",
    # Rebounds
    "támadólepattanó": "OREB",
    "védőlepattanó": "DREB",
    # Fouls
    "foult": "FOUL",
    "kiharcolt fault": "FOUL_DRAWN",
    # Blocks
    "blokk": "BLK",
    "kapott blokk": "BLK_RECV",
    # Turnovers / steals
    "eladott labda": "TOV",
    "szerzett labda": "STL",
    # Playmaking
    "gólpassz": "AST",
}

# Points awarded for scoring event types
POINTS_MAP = {
    "FG2_MADE": 2,
    "FG2M_MADE": 2,
    "FG3_MADE": 3,
    "FT_MADE": 1,
    "DUNK_MADE": 2,
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
    conn.commit()


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


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

    # Save
    save_match(conn, info, events, subs, timeouts)
    print(f"  Mentve: {db_path}")
    conn.close()


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
        "--game-id", help="Meccs azonosító (pl. 123749)"
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"SQLite adatbázis elérési út (alapértelmezett: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Meglévő adat felülírása"
    )

    args = parser.parse_args()

    if args.url:
        season, comp, game_id = parse_url(args.url)
    elif args.season and args.comp and args.game_id:
        season, comp, game_id = args.season, args.comp, args.game_id
    else:
        parser.error(
            "Add meg a --url paramétert, vagy a --season, --comp, "
            "--game-id hármast!"
        )
        return

    process_match(season, comp, game_id, args.db, force=args.force)


if __name__ == "__main__":
    main()
