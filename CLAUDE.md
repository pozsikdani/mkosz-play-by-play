# MKOSZ Play-by-Play Parser & Analyzer

## Project Overview
MKOSZ kosárlabda meccsek play-by-play eseménylistáinak feldolgozása SQLite adatbázisba,
majd részletes elemzések készítése: box score, összetett mutatók, lineup variációk,
matchup elemzés, timeout hatékonyság.

## Key Files
- `parse_pbp.py` — Fő script: HTTP fetch + HTML parse + PDF scoresheet parse + SQLite storage + player stats + advanced stats + CLI
- `pbp.sqlite` — Adatbázis (gitignored)
- `requirements.txt` — `requests`, `beautifulsoup4`, `pdfplumber`

## CLI Használat
```bash
# Meccs feldolgozása URL-ből
python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_123749

# Vagy komponensekből
python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749

# Újrafeldolgozás (meglévő adat felülírása)
python3 parse_pbp.py --url ... --force

# Kezdőötösök megjelenítése
python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749 --starters

# Egyedi DB
python3 parse_pbp.py --url ... --db custom.sqlite
```

## URL Pattern
```
PBP:        https://mkosz.hu/merkozes-esemenylista/{season}/{comp}/{comp}_{game_id}
Meccs oldal: https://mkosz.hu/merkozes/{season}/{comp}/{comp}_{game_id}
```
- `season`: pl. `x2526` (2025/2026)
- `comp`: pl. `hun2a` (NB1 B Piros), `hun2b` (NB1 B Zöld)
- `game_id`: numerikus meccs azonosító

---

## Database Schema (6 tábla)

### matches
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id | TEXT PK | `{comp}_{game_id}` |
| comp_code | TEXT | Bajnokság kód (pl. hun2a) |
| season | TEXT | Szezon (pl. x2526) |
| comp_name | TEXT | Bajnokság neve |
| round_name | TEXT | Forduló |
| match_date | TEXT | YYYY-MM-DD |
| match_time | TEXT | HH:MM |
| venue | TEXT | Helyszín |
| team_a / team_b | TEXT | Hazai / vendég csapat rövid neve |
| team_a_full / team_b_full | TEXT | Teljes név |
| score_a / score_b | INTEGER | Végeredmény |
| quarter_scores | TEXT | JSON: `[[16,25],[18,21],...]` |
| referees | TEXT | Játékvezetők |
| source_url | TEXT | Forrás URL |

### events
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id | TEXT FK | Meccs referencia |
| event_seq | INTEGER | Kronológiai sorrend |
| quarter | INTEGER | Negyed (1-4, OT: 5+) |
| minute | INTEGER | Perc |
| team | TEXT | 'A' (hazai) vagy 'B' (vendég) |
| player_name | TEXT | Játékos neve (NULL = csapatszintű) |
| event_type | TEXT | Normalizált kód (lásd lent) |
| event_raw | TEXT | Eredeti magyar szöveg |
| counter | INTEGER | Számláló (pl. fault szám) |
| score_a / score_b | INTEGER | Futó eredmény |
| is_scoring | INTEGER | 1 ha pontozó esemény |
| points | INTEGER | 0, 1, 2 vagy 3 |

### substitutions
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id | TEXT FK | Meccs |
| event_seq | INTEGER | Sorrend |
| quarter / minute | INTEGER | Mikor |
| team | TEXT | A/B |
| player_in / player_out | TEXT | Ki jön be / ki megy ki |

### timeouts
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id, event_seq, quarter, minute, team | | Időkérés adatok |

### player_stats (automatikusan számolt a save_match()-ban)
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id | TEXT FK | Meccs |
| team | TEXT | A/B |
| player_name | TEXT | Játékos |
| is_starter | INTEGER | 1 = kezdő |
| minutes | INTEGER | Játszott percek |
| plus_minus | INTEGER | +/- mutató |
| val | INTEGER | VAL (Performance Index) |
| **Összetett mutatók:** | | |
| ts_pct | REAL | True Shooting % |
| efg_pct | REAL | Effective FG % |
| game_score | REAL | Hollinger Game Score |
| usg_pct | REAL | Usage Rate % |
| ast_to | REAL | AST/TO arány |
| tov_pct | REAL | Turnover Rate % |

### rosters (PDF jegyzőkönyvből, a save_match()-ban)
| Oszlop | Típus | Leírás |
|--------|-------|--------|
| match_id | TEXT FK | Meccs |
| team | TEXT | A/B |
| player_name | TEXT | Játékos neve (NAGYBETŰ, PDF-ből) |
| jersey_number | INTEGER | Mezszám |
| is_starter | INTEGER | 1 = kezdő (font size alapján) |
| fouls_personal | INTEGER | Személyi hibák száma |
| fouls_technical | INTEGER | Technikai hibák ("T" annotáció) |
| fouls_unsportsmanlike | INTEGER | Sportszerűtlen hibák ("U" annotáció) |
| license_number | TEXT | MKOSZ játékengedély szám |

> A PDF URL: `https://hunbasketimg.webpont.com/pdf/{season}/{comp}_{game_id}.pdf`
> pdfplumber karakter-szintű feldolgozással: x/y koordináták + font size alapján szedi ki az adatokat.
> Ha a PDF nem elérhető (404, jövőbeli meccs), gracefully kihagyja.

---

## Event Types (20 típus)

### Dobástípusok
| Típus | Sikeres kód | Sikertelen kód | Pont |
|-------|-------------|----------------|------|
| Közeli | CLOSE_MADE | CLOSE_MISS | 2 |
| Zsákolás | DUNK_MADE | DUNK_MISS | 2 |
| Középtávoli | MID_MADE | MID_MISS | 2 |
| Hárompontos | THREE_MADE | THREE_MISS | 3 |
| Büntető | FT_MADE | FT_MISS | 1 |

> DUNK statisztikailag a Közeli dobástípusba számít (box score-ban együtt)

### Egyéb események
| Kód | Magyar | Kategória |
|-----|--------|-----------|
| OREB | támadólepattanó | Lepattanó |
| DREB | védőlepattanó | Lepattanó |
| AST | gólpassz | Labda |
| STL | szerzett labda | Labda |
| TOV | eladott labda | Labda |
| FOUL | foult | Fault |
| FOUL_DRAWN | kiharcolt fault | Fault |
| BLK | blokk | Védekezés |
| BLK_RECV | kapott blokk | Védekezés |

---

## Kulcsfüggvények a parse_pbp.py-ban

### Feldolgozás
- `fetch_html(url)` → HTML letöltés
- `parse_match_header(soup, ...)` → MatchInfo (csapatok, eredmény, dátum, helyszín)
- `parse_events(soup, info)` → (events[], subs[], timeouts[])
- `validate_match(info, events)` → hibák listája (negyedek, végeredmény ellenőrzés)
- `fetch_pdf(season, comp, game_id)` → PDF bytes | None
- `parse_roster_pdf(pdf_bytes)` → {'A': [RosterEntry], 'B': [RosterEntry]} | None
- `process_match(season, comp, game_id, db, force)` → teljes pipeline (PBP + PDF)

### Számított mutatók (a save_match automatikusan hívja mindkét csapatra)
- `get_starters(conn, match_id, team)` → kezdőötös (SQL CTE: subbed OUT before IN)
- `get_playing_time(conn, match_id, team)` → {player: perc} (starters @ minute 0)
- `get_plus_minus(conn, match_id, team)` → {player: +/-} (on-court scoring differential)
- `get_val(conn, match_id, team)` → {player: VAL}
- `get_advanced_stats(conn, match_id, team)` → {player: AdvancedStats}

### Formulák

**VAL** = PTS + REB + AST + STL + BLK + FD − (MissedFG + MissedFT + TO + BLKAgainst + **PF**)
> Mi a PF-et is levonjuk. Az MKOSZ hivatalos VAL NEM vonja le a PF-et.

**TS%** = PTS / (2 × (FGA + 0.44 × FTA))

**eFG%** = (FGM + 0.5 × 3PM) / FGA

**Game Score** = PTS + 0.4×FGM − 0.7×FGA − 0.4×(FTA−FTM) + 0.7×OREB + 0.3×DREB + STL + 0.7×AST + 0.7×BLK − 0.4×PF − TOV

**USG%** = 100 × ((FGA + 0.44×FTA + TOV) × TeamMin/5) / (PlayerMin × TeamPoss)

**AST/TO** = AST / TOV (ha TOV=0 és AST>0, akkor AST értéke)

**TOV%** = 100 × TOV / (FGA + 0.44×FTA + TOV)

---

## Elemzési Playbook

### 1. Meccs feldolgozása
```bash
python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_XXXXX
```

### 2. Box Score — Alap statisztikák
```sql
SELECT
    CASE WHEN ps.is_starter=1 THEN '*' ELSE ' ' END || ps.player_name AS Játékos,
    ps.minutes AS Perc,
    -- Közeli (incl. dunk)
    (SELECT SUM(CASE WHEN e.event_type IN ('CLOSE_MADE','DUNK_MADE') THEN 1 ELSE 0 END)
     FROM events e WHERE e.match_id=ps.match_id AND e.team=ps.team AND e.player_name=ps.player_name)
    || '/' ||
    (SELECT SUM(CASE WHEN e.event_type IN ('CLOSE_MADE','DUNK_MADE','CLOSE_MISS','DUNK_MISS') THEN 1 ELSE 0 END)
     FROM events e WHERE e.match_id=ps.match_id AND e.team=ps.team AND e.player_name=ps.player_name)
    AS Köz,
    -- Hasonlóan: Köz.t (MID), 3P (THREE), BT (FT)
    -- Pont, TL (OREB), VL (DREB), GÓL (AST), SZL (STL), EL (TOV), BLK, FLT (FOUL), KF (FOUL_DRAWN)
    printf('%+d', ps.plus_minus) AS '+/-',
    ps.val AS VAL
FROM player_stats ps
WHERE ps.match_id = ? AND ps.team = ?
ORDER BY ps.is_starter DESC, ps.minutes DESC;
```

### 3. Box Score — Összetett mutatók (külön szekció)
```sql
SELECT
    CASE WHEN ps.is_starter=1 THEN '*' ELSE ' ' END || ps.player_name AS Játékos,
    ps.minutes AS Perc,
    CASE WHEN ps.ts_pct IS NOT NULL THEN printf('%.1f%%', ps.ts_pct * 100) ELSE '-' END AS 'TS%',
    CASE WHEN ps.efg_pct IS NOT NULL THEN printf('%.1f%%', ps.efg_pct * 100) ELSE '-' END AS 'eFG%',
    printf('%.1f', ps.game_score) AS GmSc,
    CASE WHEN ps.usg_pct IS NOT NULL THEN printf('%.1f%%', ps.usg_pct) ELSE '-' END AS 'USG%',
    CASE WHEN ps.ast_to IS NOT NULL THEN printf('%.2f', ps.ast_to) ELSE '-' END AS 'AST/TO',
    CASE WHEN ps.tov_pct IS NOT NULL THEN printf('%.1f%%', ps.tov_pct) ELSE '-' END AS 'TOV%',
    printf('%+d', ps.plus_minus) AS '+/-',
    ps.val AS VAL
FROM player_stats ps
WHERE ps.match_id = ? AND ps.team = ?
ORDER BY ps.is_starter DESC, ps.minutes DESC;
```

### 4. Ötös variációk (Lineup Analysis)
Python logika (NEM SQL, mert frozenset kulcs kell):
```python
# Percszintű követés: starters + substitutions timeline
# on_court = set(starters), cserénél frissül
# Minden percváltáskor: lineups[frozenset(on_court)] += 1
# Rendezés: percek szerint csökkenő
# FONTOS: frozenset kulcs → kumulálja ha ugyanaz az ötös visszaáll
```

Támadás/védekezés bontás ötösönként:
```python
# Pontozó eseményeket a percszintű ötöshöz rendelni (NEM event_seq-hez!)
# Az adott percben lévő ötöshöz: team scoring → Dobott, opponent scoring → Kapott
# Dob/perc, Kap/perc, Nettó/perc kiszámítása
```

> ⚠️ FIGYELEM: NE keverd a pontozó eseményeket a substitution timeline-ba event_seq szerint,
> mert az "phantom" 0 perces ötösöket generál. Használj percszintű hozzárendelést.

### 5. Matchup elemzés (ki kivel szemben)
```python
# Mindkét csapat on_court-ját egyszerre követjük
# Bármely csere (bármelyik oldalon) → új matchup periódus
# matchup_key = (frozenset(on_court_A), frozenset(on_court_B))
# Perceket és pontokat kumuláljuk matchup-onként
```

### 6. Timeout elemzés
```python
# 1. Minden időkérésnél: 3 perc előtte vs 3 perc utána összehasonlítás
#    - Dobott/Kapott pontok az ablakban
#    - Pozitív/Semleges/Negatív változás az időkérő szempontjából

# 2. Megválaszolatlan futások (unanswered runs):
#    - Egymás utáni pontozó események ugyanattól a csapattól
#    - Ha ≥7 pont megválaszolatlanul → volt-e időkérés az ellenfél részéről?

# 3. Elszalasztott timeout-ok:
#    - Csúszó 3 perces ablak az egész meccsen
#    - Legrosszabb nettó szakasz csapatonként
#    - Ha nem volt időkérés abban az ablakban → "itt kellett volna!"
```

---

## Shot Chart API (külső adat, nem a PBP-ből)
```bash
curl -X POST "https://mkosz.hu/ajax/film.php" \
  -d "f=getShootchart&gamecode=hun2a_123749&lea=hun2a&year=x2526"
```
Válasz: JSON tömb, minden dobás:
```json
{"x": 92, "y": 55, "is_successfull": false, "wbname": "Radics Gerzson",
 "team_id": "40002", "side": "0", "period": "1", "playercode": "A92227"}
```
- x, y: 0-100% skála a pálya méretéhez képest
- Publikus API, nincs auth

---

## Conventions
- Language: Hungarian UI, mixed hu/en code
- Team side: 'A' = home (bal oszlop a PBP-ben), 'B' = away (jobb oszlop)
- Dependencies: requests, beautifulsoup4 (Python 3.9+)
- Box score kirajzolás: két szekció (Alap + Összetett mutatók)

## Known Issues
- **Ékezet hiba**: néhány név `?`-kel az MKOSZ forrásban (pl. "Heimann Gerg?" → "Heimann Gergő")
- **Team-level események**: player_name = NULL (pl. csapat lepattanó)
- **0 perces phantom lineup-ok**: ha pontozó eseményeket event_seq szerint keverünk a substitution timeline-ba, "fantom" ötösök keletkeznek. Megoldás: percszintű ötös-követés.
- **Perc pontosság**: PBP csak egész perceket ad, nincs másodperc → ±1 perc pontosság
