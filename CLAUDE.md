# MKOSZ Play-by-Play Parser

## Project Overview
MKOSZ kosárlabda meccsek play-by-play eseménylistáinak feldolgozása SQLite adatbázisba.
Az mkosz.hu oldalról scrapeli a statikus HTML eseménylistát.

## Key Files
- `parse_pbp.py` — Fő script: HTTP fetch + HTML parse + SQLite storage + validáció + CLI
- `pbp.sqlite` — Adatbázis (gitignored)

## Running
```bash
# Egy meccs feldolgozása URL-ből
python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_123749

# Vagy komponensekből
python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749

# Egyedi DB útvonal
python3 parse_pbp.py --url ... --db custom.sqlite

# Újrafeldolgozás
python3 parse_pbp.py --url ... --force
```

## Database Schema
- **matches**: meccs metaadatok (csapatok, dátum, helyszín, végeredmény, negyedenkénti pontok)
- **events**: play-by-play események (464 sor/meccs tipikusan)
- **substitutions**: cserék (player_in, player_out)
- **timeouts**: időkérések

## Event Types — 4 dobástípus
| Dobástípus | Sikeres | Sikertelen | Pont |
|------------|---------|------------|------|
| Közeli | CLOSE_MADE | CLOSE_MISS | 2 |
| Középtávoli | MID_MADE | MID_MISS | 2 |
| Hárompontos | THREE_MADE | THREE_MISS | 3 |
| Büntető | FT_MADE | FT_MISS | 1 |
| Zsákolás | DUNK_MADE | - | 2 |

## Egyéb event types
| Kód | Magyar |
|-----|--------|
| OREB | támadólepattanó |
| DREB | védőlepattanó |
| FOUL | foult |
| FOUL_DRAWN | kiharcolt fault |
| BLK | blokk |
| BLK_RECV | kapott blokk |
| TOV | eladott labda |
| STL | szerzett labda |
| AST | gólpassz |

## URL Pattern
```
https://mkosz.hu/merkozes-esemenylista/{season}/{comp}/{comp}_{game_id}
```
- `season`: pl. `x2526` (2025/2026)
- `comp`: pl. `hun2a` (NB1 B)
- `game_id`: numerikus meccs azonosító

## Conventions
- Language: Hungarian UI, mixed hu/en code
- Dependencies: requests, beautifulsoup4
- Team side: 'A' = home (bal oszlop), 'B' = away (jobb oszlop)

## Known Issues
- Ékezetes karakter hiba: néhány név `?`-kel jelenik meg (pl. "Heimann Gerg?" → "Heimann Gergő")
- Team-level események (pl. csapat lepattanó): player_name = NULL
