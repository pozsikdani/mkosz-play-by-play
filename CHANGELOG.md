# Changelog

## 2026-03-21

### Added
- **Batch processing mode**: `--season X --comp Y` (without `--game-id`) discovers all game IDs from the MKOSZ schedule page and processes them sequentially with rate limiting (0.3s delay). Skips already-processed matches.
- `--list-only` flag for batch mode — lists discovered game IDs without processing.
- `discover_game_ids()` function — fetches the schedule page and extracts game IDs using the same regex pattern as `mkosz-scoresheet/download_scoresheets.py`.
- `SCHEDULE_URL` and `BATCH_DELAY` constants.

### Changed
- `.gitignore`: removed `*.sqlite` so pre-computed databases can be committed to git.

### Data
- Populated `pbp.sqlite` with NB1 B data: 314 matches (283 played, 31 future), 119,842 events, 14,863 substitutions, 1,851 timeouts.
  - hun2a (NB1 B Piros): 182 matches
  - hun2b (NB1 B Zold): 132 matches
