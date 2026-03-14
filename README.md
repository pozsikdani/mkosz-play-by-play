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

Az adatok `pbp.sqlite` SQLite adatbázisba kerülnek (4 tábla: matches, events, substitutions, timeouts).
