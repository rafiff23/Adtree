# Adtree TikTok Go Leaderboard — Complete Architecture & Developer Guide

## Table of Contents
1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Database Schema](#database-schema)
4. [Calculation Logic](#calculation-logic)
5. [Data Flow & Processing](#data-flow--processing)
6. [Key Design Decisions](#key-design-decisions)
7. [Adding New Events](#adding-new-events)
8. [USD ↔ IDR Conversion](#usd--idr-conversion)

---

## Overview

A Streamlit multi-page application that renders **dynamic leaderboards** for TikTok Go creator challenge programs. All configuration, rules, and settings are stored in PostgreSQL — no code changes required to add or modify events.

**Key Features:**
- ✅ Dynamic event configuration (no code redeploy)
- ✅ Multi-period support (weekly, monthly)
- ✅ Multiple ranking modes (GMV, post count, viral views)
- ✅ Creator level tiers (Lv.1, Lv.2, Lv.3, Lv.4)
- ✅ Industry filtering (Accommodations, Dining, Things to Do)
- ✅ Real-time XLSX data import
- ✅ Responsive podium rendering

**Current Data:**
- 3,216+ creators across 7 agencies
- 24,259+ video transactions
- Weekly & monthly aggregations
- Real-time transaction tracking

---

## Directory Structure

```
leaderboard/
├── app.py                       # Streamlit entrypoint, sidebar nav, page routing
├── base.py                      # Core DB queries, CSS engine, HTML/podium renderers
├── event_page.py                # Dynamic event renderer (GMV/Posting/Viral)
├── home_page.py                 # Home page with banner carousel
├── settings_page.py             # Admin UI for event/banner management
├── leaderboard_import.py        # XLSX upload → database transaction pipeline
├── db.py                        # PostgreSQL connection factory (RealDictCursor)
├── migration_rank_by.sql        # DB migration: rank_by column
├── migration_levels_desc.sql    # DB migration: level tiers + description
└── README.md                    # This file
```

---

## Database Schema

### Core Configuration Tables

#### `leaderboard.events`
Master config table. Each row = one leaderboard page in the sidebar.

| Column | Type | Range | Description |
|--------|------|-------|-------------|
| `id` | SERIAL PK | — | Auto-incrementing ID |
| `event_key` | VARCHAR(50) | — | Unique slug: `acc_monthly`, `dnng_weekly`, `viral_monthly` |
| `event_name` | VARCHAR(255) | — | Sidebar navigation label |
| `industry` | VARCHAR(50) | `Accommodations`, `Dining`, `Things to Do`, `all` | Industry filter—must match `industry_source` in data |
| `period` | VARCHAR(20) | `weekly`, `monthly` | Reporting period |
| `challenge_type` | VARCHAR(50) | `gmv`, `posting`, `viral` | UI theme (affects colors, icons, wording) |
| `rank_by` | VARCHAR(50) | `gmv`, `post_count`, `max_post_views`, `total_views` | **Computation driver** — determines ranking column |
| `min_gmv_idr` | BIGINT | 0–999,999,999 | Minimum GMV threshold for eligibility (IDR) |
| `min_posts` | INT | 0–1000 | Minimum post count threshold |
| `min_views` | BIGINT | 0–999,999,999 | Minimum views threshold (viral only) |
| `max_slots` | INT | 1–100 | Number of winners in global leaderboard |
| `prize_idr` | INT | 0–99,999,999 | Prize per winner (IDR) |
| `prize_label` | VARCHAR(255) | — | Custom prize text (e.g., "Rp 500K + Voucher") |
| `title_main` | VARCHAR(255) | — | Main title part |
| `title_accent` | VARCHAR(255) | — | Colored/accented title part |
| `description` | TEXT | — | Optional subtitle/rules explanation |
| `icon` | VARCHAR(10) | — | Winner banner emoji (🏆, 👑, 🎁, etc.) |
| `accent` | VARCHAR(7) | — | Primary hex color (#FF6B6B) |
| `accent_dark` | VARCHAR(7) | — | Secondary/gradient hex color |
| `shell_bg` | VARCHAR(7) | — | Shell background hex color |
| `use_levels` | BOOLEAN | true/false | Enable creator level tier mode |
| `level3_slots` | INT | 0–100 | Winner slots for Lv.3 creators |
| `level3_prize_idr` | INT | 0–99,999,999 | Prize for Lv.3 winners (IDR) |
| `level3_min_gmv` | BIGINT | 0–999,999,999 | Min GMV for Lv.3 eligibility |
| `level2_slots` | INT | 0–100 | Winner slots for Lv.2 creators |
| `level2_prize_idr` | INT | 0–99,999,999 | Prize for Lv.2 winners (IDR) |
| `level2_min_gmv` | BIGINT | 0–999,999,999 | Min GMV for Lv.2 eligibility |
| `level1_slots` | INT | 0–100 | Winner slots for Lv.1 creators |
| `level1_prize_idr` | INT | 0–99,999,999 | Prize for Lv.1 winners (IDR) |
| `level1_min_gmv` | BIGINT | 0–999,999,999 | Min GMV for Lv.1 eligibility |
| `sort_order` | INT | 1–100 | Sidebar navigation order |
| `is_active` | BOOLEAN | true/false | Show in sidebar if true |
| `created_at` | TIMESTAMP | — | Record creation timestamp |
| `updated_at` | TIMESTAMP | — | Last modification timestamp |

**Example Event Rows:**
```sql
-- Monthly GMV Challenge
INSERT INTO leaderboard.events (event_key, event_name, industry, period, rank_by, ...)
VALUES ('acc_monthly', 'Accommodations Monthly', 'Accommodations', 'monthly', 'gmv', ...);

-- Weekly Viral Challenge with Level Tiers
INSERT INTO leaderboard.events (event_key, event_name, use_levels, level3_slots, level2_slots, ...)
VALUES ('viral_weekly', 'Viral Views Weekly', true, 10, 15, 20, ...);
```

---

#### `leaderboard.banners`
Homepage carousel images.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | |
| `image_url` | TEXT | Public image URL (JPG/PNG) |
| `sort_order` | INT | Display order (ascending) |
| `created_at` | TIMESTAMP | Upload timestamp |

---

#### `leaderboard.leaderboard_rules`
**Legacy table** — used by hardcoded accommodation/fnb/attraction pages only. Still referenced by `render_monthly()` / `render_weekly()` in `base.py` for backward compatibility.

| Column | Type | Description |
|--------|------|-------------|
| `program_key` | VARCHAR(50) PK | `accomodation`, `fnb`, `attraction` |
| `min_gmv_idr` | BIGINT | Min GMV threshold |
| `min_videos` | INT | Min post count |
| `min_merchants` | INT | Min unique merchants |
| `max_slots` | INT | Winner count |
| `prize_idr` | INT | Prize per winner |
| `prize_label` | VARCHAR(255) | Prize text |
| `title_full` | VARCHAR(255) | Full title |
| `title_main` | VARCHAR(255) | Main part |
| `title_accent` | VARCHAR(255) | Accent part |
| `updated_at` | TIMESTAMP | Last update |

---

### Data Tables (Read-Only)

#### `leaderboard.tiktok_go_video_summary`
**Weekly aggregated creator performance metrics.** Source: XLSX import via `leaderboard_import.py`.

| Column | Type | Description | Sample Value |
|--------|------|-------------|--------------|
| `id` | INTEGER PK | Unique row ID | 12345 |
| `uniq_id` | TEXT | Creator username | `dindappdinda` |
| `author_id` | TEXT | Creator TikTok numeric ID | `7385647294` |
| `poi_id` | TEXT | Merchant/POI ID | `M123456` |
| `poi_vv` | NUMERIC | POI views (aggregated) | 45000.00 |
| `industry_source` | TEXT | Industry category | `Accommodations` |
| `report_month` | DATE | Report month (yyyy-mm-01) | 2026-04-01 |
| `report_week` | INT | Week number (1-4/5) | 2 |
| `start_date` | DATE | Period start date | 2026-04-08 |
| `cutoff_date` | DATE | Data cutoff date | 2026-04-14 |
| `total_post` | INT | Total posts this period | 5 |
| `creator_level` | TEXT | Creator tier | `Lv.3` |
| `creator_city` | TEXT | City of residence | `Jakarta` |
| `creator_binding_status` | TEXT | Account status | `Bound` |
| `fulfill_amount_usd` | NUMERIC | Total GMV (USD) | 1250.50 |
| `fulfill_amount_usd_weekly` | NUMERIC | Weekly GMV breakdown (USD) | 312.63 |
| `order_count` | NUMERIC | Transaction count | 25.00 |
| `aov` | NUMERIC | Average order value (USD) | 50.02 |
| `ctr` | NUMERIC | Click-through rate | 0.045 |
| `cvr` | NUMERIC | Conversion rate | 0.032 |
| `video_completion` | NUMERIC | Video completion rate | 0.78 |
| `like_rate` | NUMERIC | Engagement rate (likes) | 0.125 |
| `comment_rate` | NUMERIC | Engagement rate (comments) | 0.042 |
| `redemption_amount` | NUMERIC | Voucher redemption (USD) | 450.00 |
| `redeemed_orders` | INT | Orders using vouchers | 9 |

**Key Insight:** This table is **pre-aggregated by week**. Each row represents one creator's performance in one specific week across all merchants in an industry.

---

#### `leaderboard.tiktok_go_video_transactions`
**Raw daily transaction data.** One row per day per post per creator. Used for viral view calculations and detailed transaction history.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT PK | Unique transaction ID |
| `item_id` | TEXT | Post ID (TikTok item ID) |
| `item_create_date` | TIMESTAMP | Post creation date |
| `author_id` | TEXT | Creator TikTok numeric ID |
| `uniq_id` | TEXT | Creator username |
| `poi_id` | TEXT | Merchant/POI ID |
| `industry_source` | TEXT | Industry: `Accommodations`, `Dining`, `Things to Do` |
| `report_month` | DATE | Report month (yyyy-mm-01) |
| `report_week` | INT | Week number (1-4) |
| `poi_vv` | NUMERIC | Daily post views for this POI |
| `creator_level` | TEXT | Creator level: `Lv.1`, `Lv.2`, `Lv.3`, `Lv.4` |
| `fulfill_amount_usd` | NUMERIC | GMV for this transaction (USD) |
| `order_count` | NUMERIC | Orders in this transaction |
| `aov` | NUMERIC | Average order value (USD) |
| `ctr` | NUMERIC | Click-through rate |
| `cvr` | NUMERIC | Conversion rate |
| `video_completion` | NUMERIC | Video completion % |
| `like_rate` | NUMERIC | Like engagement rate |
| `comment_rate` | NUMERIC | Comment engagement rate |
| `redemption_amount` | NUMERIC | Voucher redemption (USD) |
| `redeemed_orders` | INT | Voucher-redeemed orders |

**Important:** NOT cumulative. Each row = one day's snapshot. For viral calculations, views are SUM'd across days per post.

---

## Calculation Logic

### Weekly vs Monthly Aggregation

#### **Monthly Leaderboard Calculation**

**Data Source:** `leaderboard.tiktok_go_video_summary` (already aggregated by week)

**Algorithm:**
```
FOR each creator IN report_month:
  IF industry_source matches event.industry:
    IF creator_level matches selected level (or no level filter):
      IF fulfill_amount_usd >= event.min_gmv_idr / 16000:
        total_gmv = SUM(fulfill_amount_usd) across ALL weeks in month
        total_posts = SUM(total_post) across ALL weeks in month
        total_views = SUM(poi_vv) across ALL weeks in month
        
        STORE: (creator, total_gmv, total_posts, total_views, ...)

SORT by rank_by metric (gmv, post_count, total_views, max_post_views)
RANK: 1st, 2nd, 3rd, ... top N
ASSIGN: prize_idr based on rank
```

**SQL Example (GMV Ranking):**
```sql
SELECT 
  author_id,
  SUM(fulfill_amount_usd) as total_gmv_usd,
  SUM(total_post) as total_posts,
  COUNT(DISTINCT poi_id) as merchant_count
FROM leaderboard.tiktok_go_video_summary
WHERE 
  report_month = '2026-04-01'
  AND TRIM(industry_source) = 'Accommodations'
  AND creator_level = 'Lv.3'
  AND fulfill_amount_usd >= (10000000 / 16000)  -- min GMV in USD
GROUP BY author_id
ORDER BY total_gmv_usd DESC
LIMIT 10;  -- max_slots for Lv.3
```

**Output Example:**
```
author_id     | total_gmv_usd | total_posts | rank | prize_idr
--------------+---------------+-------------+------+-----------
7385647294    | 5250.00       | 25          | 1    | 5000000
8294615847    | 4820.50       | 22          | 2    | 3000000
9107264854    | 4120.75       | 20          | 3    | 2000000
```

---

#### **Weekly Leaderboard Calculation**

**Data Source:** `leaderboard.tiktok_go_video_summary` (one week per row)

**Algorithm:**
```
FOR each creator IN report_week:
  IF industry_source matches event.industry:
    IF creator_level matches selected level:
      IF fulfill_amount_usd >= event.min_gmv_idr / 16000:
        STORE: (creator, fulfill_amount_usd, total_posts, poi_vv, ...)

SORT by rank_by metric
RANK: 1st, 2nd, 3rd, ... top N
ASSIGN: prize_idr based on rank
```

**Key Difference:** Weekly uses individual week rows; monthly sums across all weeks in the month.

---

### Ranking Modes

#### **1. GMV Ranking** (`rank_by = 'gmv'`)
Sort by **gross merchandise value** (revenue).

```
Ranking Column: fulfill_amount_usd (summed if monthly)
Threshold: min_gmv_idr (converted to USD: divide by 16000)
Winner Prize: prize_idr (fixed per rank)
```

**Formula:**
```
gmv_idr_display = fulfill_amount_usd * 16000
pass_threshold = (fulfill_amount_usd >= min_gmv_idr / 16000)
```

---

#### **2. Posting Ranking** (`rank_by = 'post_count'`)
Sort by **number of posts created**.

```
Ranking Column: total_post (summed if monthly)
Threshold: min_posts
Winner Prize: prize_idr
```

---

#### **3. Viral Ranking — Max Single Post Views** (`rank_by = 'max_post_views'`)
Sort by **highest-performing single post**.

```
Data Source: leaderboard.tiktok_go_video_transactions (raw daily data)

Algorithm:
FOR each creator:
  FOR each post (item_id):
    post_views = SUM(poi_vv) across ALL days
  max_post_views = MAX(post_views) per creator
  
SORT by max_post_views DESC
RANK: 1st, 2nd, 3rd, ... top N
```

**SQL:**
```sql
SELECT 
  author_id,
  item_id,
  SUM(poi_vv) as total_post_views
FROM leaderboard.tiktok_go_video_transactions
WHERE report_week = 2 AND industry_source = 'Things to Do'
GROUP BY author_id, item_id
ORDER BY author_id, total_post_views DESC;

-- Then MAX per creator:
WITH post_views AS (
  ... above query ...
)
SELECT 
  author_id,
  MAX(total_post_views) as max_post_views
FROM post_views
GROUP BY author_id
ORDER BY max_post_views DESC
LIMIT 10;
```

---

#### **4. Viral Ranking — Total All Posts** (`rank_by = 'total_views'`)
Sort by **sum of all post views**.

```
Algorithm:
FOR each creator:
  total_views = SUM(poi_vv) across ALL posts ALL days
  
SORT by total_views DESC
```

---

### Creator Level Tier Filtering

When `use_levels = true` in an event:

**Data Flow:**
```
User selects level (Lv.3, Lv.2, Lv.1)
  ↓
Filter data: creator_level = 'Lv.3'
  ↓
Override rules:
  max_slots = level3_slots (10)
  prize_idr = level3_prize_idr (5000000)
  min_gmv_idr = level3_min_gmv (500000000)
  ↓
Rank and award prizes per level-specific slots
```

**Example:**
```sql
-- Lv.3 creators only
SELECT * FROM leaderboard.tiktok_go_video_summary
WHERE creator_level = 'Lv.3'
  AND report_week = 2
  AND industry_source = 'Accommodations'
ORDER BY fulfill_amount_usd DESC
LIMIT 10;  -- level3_slots
```

---

### Industry Filtering

**Supported Industries:**
- `Accommodations` (hotels, resorts, homestays)
- `Dining` (restaurants, cafes, food delivery)
- `Things to Do` (attractions, tours, experiences)
- `all` (no filter)

**Important:** Values must match exactly in `industry_source` column (case-sensitive, post-trim).

```python
# In base.py
def _ind_clause(industry):
    if industry.lower() == 'all':
        return ""
    return f"AND TRIM(industry_source) = TRIM('{industry}')"
```

---

## Data Flow & Processing

### Import Pipeline

```
User uploads XLSX file (leaderboard_import.py)
  ↓
pd.read_excel() with specific column mapping
  ↓
Transform:
  - Parse dates: "20260401" → datetime(2026, 4, 1)
  - Convert currency: all amounts in USD
  - Map creator_level: "Lv.3" text format
  - Map industry: "Accommodations" etc.
  ↓
Dedup check: ON CONFLICT (uniq_id, report_week, poi_id)
  ↓
UPSERT into leaderboard.tiktok_go_video_summary (weekly agg)
     or leaderboard.tiktok_go_video_transactions (raw daily)
  ↓
Database updated
Streamlit cache invalidated → page refreshes with new data
```

---

### Render Pipeline

```
app.py loads active events from leaderboard.events
  ↓
User selects event from sidebar
  ↓
event_page.render(event_config) called
  ↓
_make_style(cfg) → PageStyle (colors, fonts, layout)
_make_rules(cfg) → Dict with thresholds, slots, prizes
_make_css(style) → Inject scoped CSS (prevents collisions)
  ↓
Widget filters: month/week selector, creator filter, level selector
  ↓
Route on rank_by:
  - "gmv"             → _render_gmv()    → load_gmv_monthly/weekly()
  - "post_count"      → _render_posting() → load_posting_data()
  - "max_post_views"  → _render_viral()   → load_viral_data()
  - "total_views"     → _render_viral()   → load_viral_data()
  ↓
_aggregate() computes final rankings
  ↓
Render components:
  - _header()
  - Filter pills
  - _winner_banner()
  - Summary metric cards
  - _podium() (1st/2nd/3rd medal display)
  - Leaderboard table (4th–10th+)
```

---

## Key Design Decisions

### Why `rank_by` vs `challenge_type`?

- **`challenge_type`** = UI/theme only (colors, icons, wording: "GMV Challenge", "Viral Leaderboard")
- **`rank_by`** = actual computation (which column to sort by)

**Decoupled design** allows:
- A "viral themed" page that ranks by `total_views` instead of `max_post_views`
- A "GMV themed" page that filters by minimum posts instead of minimum GMV
- Flexible, data-driven ranking without code changes

---

### Creator Identification

**All displays use `author_id`** (TikTok numeric user ID) — not `uniq_id` (username).

- `author_id`: Unique numeric ID (e.g., `7385647294`) — stable, official TikTok ID
- `uniq_id`: Username string (e.g., `dindappdinda`) — can change, human-readable

Internal joins use `uniq_id` for aggregation, but winner display shows `author_id` for accuracy.

---

### CSS Scoping

All CSS classes prefixed by `event_key` to prevent collisions:
```
Event key: acc_monthly → CSS classes: .acc-shell, .acc-title, .acc-winner
Event key: viral_weekly → CSS classes: .viral-shell, .viral-title, .viral-winner
```

Template uses `__P__` placeholder replaced at render time:
```python
css_template = css_template.replace("__P__", event_key)
```

---

## Adding New Events

### Via Settings Page (No Code Required)

1. Open Streamlit app → **Settings** tab
2. Fill form:
   - **Event Name:** "Dining Weekly Challenge"
   - **Industry:** Dining
   - **Period:** weekly
   - **Challenge Type:** gmv
   - **Rank By:** gmv
   - **Min GMV (IDR):** 10000000
   - **Max Slots:** 10
   - **Prize (IDR):** 500000
   - **Title:** "Dining Weekly"
   - **Icon:** 🍽️
   - **Colors:** Primary: #FF6B6B, Dark: #C92A2A

3. **[Optional]** Enable level tiers:
   - ✅ Use Levels
   - Lv.3: 5 slots, Rp 500K, min 10M GMV
   - Lv.2: 7 slots, Rp 300K, min 5M GMV
   - Lv.1: 10 slots, Rp 100K, min 0 GMV

4. Click **Save**
5. Event appears in sidebar immediately (cache TTL: 60s)

---

## USD ↔ IDR Conversion

**Fixed Rate:** `1 USD = 16,000 IDR`

```python
# In base.py
USD_TO_IDR = 16_000

# Display conversion
gmv_idr = fulfill_amount_usd * USD_TO_IDR
min_gmv_usd = min_gmv_idr / USD_TO_IDR

# Example
fulfill_amount_usd = 500.00
gmv_idr = 500 * 16000 = 8,000,000 IDR
```

All thresholds and prizes in DB are stored in IDR. At render time, converted to USD for display:
```
Min GMV: Rp 10,000,000 (= $625 USD)
Prize: Rp 5,000,000 per winner (= $312.50 USD)
```

---

## Troubleshooting

### Event Not Appearing in Sidebar

- ✅ Check `is_active = true` in `leaderboard.events`
- ✅ Verify `industry` matches data exactly: `Accommodations` (not `accommodation`)
- ✅ Clear Streamlit cache: Refresh page or click ↺ in app

### Leaderboard Empty or Wrong Rankings

- ✅ Verify `report_month` or `report_week` matches selected period
- ✅ Check `industry_source` in data matches event config
- ✅ Ensure `creator_level` matches if level filter enabled
- ✅ Confirm data was imported via leaderboard_import.py (check `cutoff_date`)

### Wrong USD/IDR Display

- ✅ Verify `USD_TO_IDR = 16_000` in base.py
- ✅ Check if values in DB are actually USD (should be)

---

## Performance Notes

- **Weekly aggregation:** Typically <500ms (pre-aggregated in `tiktok_go_video_summary`)
- **Monthly aggregation:** Typically <1s (SUM across 4–5 weeks)
- **Viral calculation:** Typically <2s (raw transaction JOIN + grouping)
- **Import speed:** ~5MB XLSX → ~10s to upsert

For large datasets (>100K transactions), consider:
- Materializing weekly views (CREATE MATERIALIZED VIEW)
- Partitioning data by `report_month` + `industry_source`

---

## Database Maintenance

**Regular maintenance tasks:**

```sql
-- Verify data consistency
SELECT COUNT(*), industry_source, report_month 
FROM leaderboard.tiktok_go_video_summary 
GROUP BY industry_source, report_month;

-- Check for orphaned records (missing creators)
SELECT author_id, COUNT(*) FROM leaderboard.tiktok_go_video_summary
WHERE author_id NOT IN (SELECT tiktok_id FROM public.creator_registry)
GROUP BY author_id;

-- Backup active events
SELECT * FROM leaderboard.events WHERE is_active = true;
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | 2026-04-15 | Added detailed calculation docs, level tiers, multi-ranking modes |
| 1.5 | 2026-03-01 | Added viral views ranking |
| 1.0 | 2026-01-15 | Initial release (GMV + posting modes only) |

