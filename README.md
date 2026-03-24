# mkosz-play-by-play

MKOSZ kosárlabda meccsek play-by-play eseménylistáinak feldolgozása SQLite adatbázisba.

## Telepítés

```bash
pip install -r requirements.txt
```

## Használat

```bash
# Egy meccs feldolgozása
python3 parse_pbp.py --url https://mkosz.hu/merkozes-esemenylista/x2526/hun2a/hun2a_123749

# Vagy komponensekből
python3 parse_pbp.py --season x2526 --comp hun2a --game-id 123749

# Újrafeldolgozás
python3 parse_pbp.py --url ... --force
```

## Kimenet

Az adatok `pbp.sqlite` SQLite adatbázisba kerülnek (6 tábla: matches, events, substitutions, timeouts, player_stats, rosters).

## Batch feldolgozás

```bash
# Teljes bajnokság letöltése
python3 parse_pbp.py --season x2526 --comp hun2a

# Csak listázás (letöltés nélkül)
python3 parse_pbp.py --season x2526 --comp hun2a --list-only
```

## PDF jegyzőkönyv

A PBP feldolgozás mellett automatikusan letölti és feldolgozza a meccs PDF jegyzőkönyvét is (`rosters` tábla):
- Mezszámok, kezdőötös, faultípus bontás (személyi/technikai/sportszerűtlen)
- PDF URL: `hunbasketimg.webpont.com/pdf/{season}/{comp}_{game_id}.pdf`

## Támogatott bajnokságok

| Bajnokság | Comp kód | Meccsek |
|-----------|----------|---------|
| NB1 B Piros | hun2a | 182 |
| NB1 B Zöld | hun2b | 132 |
| MEFOB Férfi | hun_univn | 72 |
