import os
import psycopg2
from psycopg2.extras import RealDictCursor


# =====================================================
# DATABASE CONNECTION
# =====================================================

def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
        cursor_factory=RealDictCursor,
    )
    return conn

def bulk_update_content_submissions(updates: list):
    """
    Bulk update content_submissions for status_id and reason.
    
    This function updates multiple rows at once, which is more efficient
    than calling update_content_submission_row() in a loop.
    
    Args:
        updates: List of dicts, each containing:
            - id: Submission ID to update
            - status_id: New status ID (must match status_map table)
            - reason: New reason text (can be None for empty)
    
    Example:
        updates = [
            {"id": 10, "status_id": 2, "reason": "Pending review"},
            {"id": 15, "status_id": 3, "reason": "Low engagement"},
        ]
        bulk_update_content_submissions(updates)
    
    Raises:
        Exception: If database connection fails or SQL execution errors occur
    """
    if not updates:
        # Nothing to update
        return
    
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Process each update
                for update in updates:
                    cur.execute(
                        """
                        UPDATE public.content_submissions
                        SET status_id = %(status_id)s,
                            reason = %(reason)s
                        WHERE id = %(id)s
                        """,
                        update
                    )
                # Commit happens automatically when exiting the 'with conn:' block
    except Exception as e:
        # Re-raise with more context
        raise Exception(f"Failed to bulk update submissions: {str(e)}")
    finally:
        conn.close()

# =====================================================
# HELPER: GET AGENCY ID FROM agency_map
# =====================================================

def get_agency_id_by_name(agency_name: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM public.agency_map WHERE agency_name = %s LIMIT 1",
                (agency_name,)
            )
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


# =====================================================
# INSERT CREATOR (WITH agency_id)
# =====================================================

def insert_creator_registry_row(
    agency_id,
    tiktok_id,
    followers,
    full_name,
    domicile,
    uid,
    phone_number,
    tiktok_link,
    binding_status,
    onboarding_date,
    month_label,
    level=None,
):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.creator_registry (
                    tiktok_id,
                    followers,
                    full_name,
                    domicile,
                    uid,
                    phone_number,
                    tiktok_link,
                    binding_status,
                    onboarding_date,
                    month_label,
                    notes,
                    level,
                    created_at,
                    updated_at,
                    agency_id
                )
                VALUES (
                    %(tiktok_id)s,
                    %(followers)s,
                    %(full_name)s,
                    %(domicile)s,
                    %(uid)s,
                    %(phone_number)s,
                    %(tiktok_link)s,
                    %(binding_status)s,
                    %(onboarding_date)s,
                    %(month_label)s,
                    NULL,
                    %(level)s,
                    NOW(),
                    NOW(),
                    %(agency_id)s
                )
                RETURNING id;
                """,
                {
                    "tiktok_id": tiktok_id,
                    "followers": followers,
                    "full_name": full_name,
                    "domicile": domicile,
                    "uid": uid,
                    "phone_number": phone_number,
                    "tiktok_link": tiktok_link,
                    "binding_status": binding_status,
                    "onboarding_date": onboarding_date,
                    "month_label": month_label,
                    "level": level,
                    "agency_id": agency_id,
                },
            )

            new_id = cur.fetchone()["id"]

        conn.commit()
        return new_id

    finally:
        conn.close()


# =====================================================
# FETCH CREATOR LIST (JOIN agency_map)
# =====================================================

def fetch_creator_registry(tiktok_id_filter=None, start_date=None, end_date=None):
    sql = """
        SELECT
            cr.id,
            cr.agency_id,
            am.agency_name,
            cr.tiktok_id,
            cr.followers,
            cr.full_name,
            cr.domicile,
            cr.uid,
            cr.phone_number,
            cr.tiktok_link,
            cr.level,
            cr.binding_status,
            cr.onboarding_date,
            cr.month_label,
            cr.notes,
            cr.created_at
        FROM public.creator_registry cr
        LEFT JOIN public.agency_map am
            ON cr.agency_id = am.id
        WHERE 1=1
    """

    params = {}

    if tiktok_id_filter:
        sql += " AND cr.tiktok_id ILIKE %(tiktok_id)s"
        params["tiktok_id"] = f"%{tiktok_id_filter}%"

    if start_date:
        sql += " AND cr.created_at::date >= %(start_date)s"
        params["start_date"] = start_date

    if end_date:
        sql += " AND cr.created_at::date <= %(end_date)s"
        params["end_date"] = end_date

    sql += " ORDER BY cr.created_at DESC"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


# =====================================================
# UPDATE CREATOR ROW
# =====================================================

def fetch_all_leaderboard_rules() -> list:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM leaderboard.leaderboard_rules ORDER BY program_key")
            return cur.fetchall()
    finally:
        conn.close()


def upsert_leaderboard_rule(program_key: str, fields: dict):
    cols   = list(fields.keys())
    vals   = list(fields.values())
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
        INSERT INTO leaderboard.leaderboard_rules (program_key, {", ".join(cols)}, updated_at)
        VALUES (%s, {placeholders}, NOW())
        ON CONFLICT (program_key) DO UPDATE
        SET {set_clause}, updated_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [program_key] + vals)
        conn.commit()
    finally:
        conn.close()


def update_creator_registry_row(row_id, updated_fields):
    """
    Now supports updating agency_id instead of agency_name.
    If UI sends agency_name, convert it.
    """
    if not updated_fields:
        return

    # Convert agency_name → agency_id if needed
    if "agency_name" in updated_fields:
        agency_id = get_agency_id_by_name(updated_fields["agency_name"])
        updated_fields["agency_id"] = agency_id
        del updated_fields["agency_name"]

    # Normal update
    set_clause = ", ".join([f"{key} = %({key})s" for key in updated_fields.keys()])
    sql = f"""
        UPDATE public.creator_registry
        SET {set_clause}
        WHERE id = %(id)s
    """

    params = updated_fields.copy()
    params["id"] = row_id

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    finally:
        conn.close()


# =====================================================
# CONTENT QC
# =====================================================

import re as _re
import pandas as _pd

_LOCK_TIMEOUT_MIN = 10

_QC_WEIGHTS = {
    "qc_hook":     3.0,
    "qc_usp":      2.0,
    "qc_product":  2.0,
    "qc_cta":      2.0,
    "qc_engaging": 2.0,
}


def _calc_qc_score(qc_data: dict) -> float:
    score = 0.0
    for field, weight in _QC_WEIGHTS.items():
        if qc_data.get(field):
            score += weight
    quality = qc_data.get("qc_quality")
    if quality:
        score += float(quality)
    return score


def _calc_final_status(score: float, qc_data: dict):
    reviewed = (
        qc_data.get("qc_type") is not None
        or any(qc_data.get(f) for f in _QC_WEIGHTS)
        or qc_data.get("qc_quality") is not None
    )
    if not reviewed:
        return None
    if score > 10:
        return "Very Good"
    if score >= 7:
        return "Good"
    if score >= 5:
        return "Fair"
    return "Poor"

_METRIC_COLS = [
    "creator_level", "sales_value", "orders", "redemption_amount",
    "redeemed_orders", "video_views", "ctr", "cvr", "aov",
    "video_completion_rate", "like_rate", "comment_rate",
]

_ALL_DATA_COLS = [
    "post_id",
    "location_industry", "post_type", "creator_type", "post_title",
    "post_date", "duration", "task_type", "location_id", "location_name",
    "location_city", "merchant_name", "creator_name", "creator_id",
    "creator_binding", "creator_city",
] + _METRIC_COLS

# Maps normalised CSV header → DB column name
CSV_TO_DB = {
    "location_indu":          "location_industry",
    "location_industry":      "location_industry",
    "post_type":              "post_type",
    "creator_type":           "creator_type",
    "post_id":                "post_id",
    "post_title":             "post_title",
    "post_date":              "post_date",
    "duration":               "duration",
    "task_type":              "task_type",
    "location_id":            "location_id",
    "location_nam":           "location_name",
    "location_name":          "location_name",
    "location_city":          "location_city",
    "merchant_nan":           "merchant_name",
    "merchant_name":          "merchant_name",
    "creator_name":           "creator_name",
    "creator_id":             "creator_id",
    "creator_bindi":                 "creator_binding",
    "creator_binding":               "creator_binding",
    "creator_binding_status":        "creator_binding",
    "creator_city":           "creator_city",
    "creator_level":          "creator_level",
    "sales_value":            "sales_value",
    "orders":                 "orders",
    "redemption_a":           "redemption_amount",
    "redemption_amount":      "redemption_amount",
    "redeemed_ord":           "redeemed_orders",
    "redeemed_orders":        "redeemed_orders",
    "video_views":            "video_views",
    "ctr":                    "ctr",
    "cvr":                    "cvr",
    "aov":                    "aov",
    "video_comple":           "video_completion_rate",
    "video_completion_rate":  "video_completion_rate",
    "like_rate":              "like_rate",
    "comment_rate":           "comment_rate",
}


def _norm_col(col: str) -> str:
    return col.lower().strip().replace(" ", "_").replace("-", "_")


def parse_post_date(val) -> str | None:
    s = str(val).strip()
    if _re.match(r"^\d{8}$", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return _pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_creator_level(val) -> int | None:
    if _pd.isna(val) or str(val).strip() == "":
        return None
    m = _re.search(r"\d+", str(val))
    return int(m.group()) if m else None


def prepare_content_qc_csv(df_raw: _pd.DataFrame) -> tuple:
    """
    Normalise CSV column names, parse dates and creator levels, coerce numerics.
    Returns (df_clean, unmapped_columns).
    """
    rename = {}
    unmapped = []
    for col in df_raw.columns:
        norm = _norm_col(col)
        if norm in CSV_TO_DB:
            rename[col] = CSV_TO_DB[norm]
        else:
            unmapped.append(col)

    df = df_raw.rename(columns=rename)
    known = set(CSV_TO_DB.values())
    df = df[[c for c in df.columns if c in known]].copy()

    if "post_date" in df.columns:
        df["post_date"] = df["post_date"].apply(parse_post_date)

    if "creator_level" in df.columns:
        df["creator_level"] = df["creator_level"].apply(parse_creator_level)

    num_cols = [c for c in _METRIC_COLS if c in df.columns and c != "creator_level"]
    for col in num_cols:
        df[col] = _pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("%", "").str.strip(),
            errors="coerce",
        )

    return df, unmapped


def upsert_content_qc_posts(rows: list) -> tuple:
    """
    Upsert posts: insert new rows, update only metric columns on conflict.
    Returns (inserted_count, updated_count).
    """
    if not rows:
        return 0, 0

    cols = [c for c in _ALL_DATA_COLS if c in rows[0]]
    metrics_present = [c for c in _METRIC_COLS if c in cols]

    col_list = ", ".join(cols)
    placeholders = ", ".join([f"%({c})s" for c in cols])

    if metrics_present:
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in metrics_present)
        update_set += ", metrics_updated_at = NOW()"
    else:
        update_set = "metrics_updated_at = NOW()"

    sql = f"""
        INSERT INTO public.content_qc_posts ({col_list}, imported_at, metrics_updated_at)
        VALUES ({placeholders}, NOW(), NOW())
        ON CONFLICT (post_id) DO UPDATE
            SET {update_set}
        RETURNING (xmax = 0) AS is_insert
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                inserted = updated = 0
                for row in rows:
                    clean = {
                        k: (None if isinstance(v, float) and _pd.isna(v) else v)
                        for k, v in row.items() if k in cols
                    }
                    cur.execute(sql, clean)
                    result = cur.fetchone()
                    if result and result["is_insert"]:
                        inserted += 1
                    else:
                        updated += 1
        return inserted, updated
    finally:
        conn.close()


def fetch_content_qc_posts(qc_filter=None, date_from=None, date_to=None, search=None) -> list:
    """Fetch posts for QC review."""
    conditions = ["1=1"]
    params: dict = {}

    if qc_filter == "Unreviewed":
        conditions.append("p.qc_updated_at IS NULL")
    elif qc_filter:
        conditions.append("p.qc_final_status = %(qc_filter)s")
        params["qc_filter"] = qc_filter

    if date_from:
        conditions.append("p.post_date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        conditions.append("p.post_date <= %(date_to)s")
        params["date_to"] = date_to
    if search:
        conditions.append(
            "(p.creator_name ILIKE %(search)s OR p.post_id ILIKE %(search)s)"
        )
        params["search"] = f"%{search}%"

    sql = f"""
        SELECT
            p.post_id, p.creator_id, p.post_date, p.creator_name,
            p.qc_type, p.qc_hook, p.qc_usp, p.qc_product, p.qc_review,
            p.qc_cta, p.qc_engaging, p.qc_quality,
            p.qc_total_score, p.qc_issue, p.qc_final_status,
            p.qc_updated_by, p.qc_updated_at
        FROM public.content_qc_posts p
        WHERE {" AND ".join(conditions)}
        ORDER BY p.post_date DESC, p.post_id
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def get_content_qc_post_state(post_id: str) -> dict | None:
    """Return qc_status + qc_updated_at for a single post (conflict detection)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT qc_status, qc_updated_at, qc_updated_by "
                "FROM public.content_qc_posts WHERE post_id = %s",
                (post_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def acquire_content_qc_lock(post_id: str, username: str) -> tuple:
    """
    Try to acquire (or refresh) the edit lock for post_id.
    Returns (success: bool, message: str).
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Remove stale locks
                cur.execute(
                    "DELETE FROM public.content_qc_locks "
                    "WHERE post_id = %s AND locked_at < NOW() - INTERVAL %s",
                    (post_id, f"{_LOCK_TIMEOUT_MIN} minutes"),
                )
                # 2. Insert our lock; refresh timestamp only if we already own it
                cur.execute(
                    """
                    INSERT INTO public.content_qc_locks (post_id, locked_by, locked_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (post_id) DO UPDATE
                        SET locked_at = NOW()
                        WHERE public.content_qc_locks.locked_by = EXCLUDED.locked_by
                    """,
                    (post_id, username),
                )
                # 3. Check who holds the lock
                cur.execute(
                    "SELECT locked_by FROM public.content_qc_locks WHERE post_id = %s",
                    (post_id,),
                )
                row = cur.fetchone()

        if row and row["locked_by"] == username:
            return True, "ok"
        elif row:
            return False, f"Currently being edited by **{row['locked_by']}**"
        else:
            return False, "Could not acquire lock, please try again."
    finally:
        conn.close()


def release_content_qc_lock(post_id: str, username: str):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.content_qc_locks "
                    "WHERE post_id = %s AND locked_by = %s",
                    (post_id, username),
                )
    finally:
        conn.close()


def save_content_qc_issue(post_id: str, qc_issue, username: str) -> tuple:
    """Update only the qc_issue field for a post."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.content_qc_posts
                    SET qc_issue = %s, qc_updated_by = %s, qc_updated_at = NOW()
                    WHERE post_id = %s
                    """,
                    (qc_issue or None, username, post_id),
                )
        return True, "ok"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def save_content_qc_review(post_id: str, qc_data: dict, username: str, expected_updated_at) -> tuple:
    """
    Save all QC scoring fields with optimistic conflict detection.
    Computes qc_total_score and qc_final_status automatically.
    Returns (success: bool, message: str).
    """
    total_score  = _calc_qc_score(qc_data)
    final_status = _calc_final_status(total_score, qc_data)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qc_updated_at, qc_updated_by "
                    "FROM public.content_qc_posts WHERE post_id = %s",
                    (post_id,),
                )
                current = cur.fetchone()
                if not current:
                    return False, "Post not found."

                if current["qc_updated_at"] != expected_updated_at:
                    who = current["qc_updated_by"] or "seseorang"
                    return False, (
                        f"Conflict: this post was already updated by **{who}** "
                        "while you were editing. Refresh and try again."
                    )

                cur.execute(
                    """
                    UPDATE public.content_qc_posts
                    SET qc_type         = %s,
                        qc_hook         = %s,
                        qc_usp          = %s,
                        qc_product      = %s,
                        qc_review       = %s,
                        qc_cta          = %s,
                        qc_engaging     = %s,
                        qc_quality      = %s,
                        qc_total_score  = %s,
                        qc_issue        = %s,
                        qc_final_status = %s,
                        qc_updated_by   = %s,
                        qc_updated_at   = NOW()
                    WHERE post_id = %s
                    """,
                    (
                        qc_data.get("qc_type") or None,
                        bool(qc_data.get("qc_hook")),
                        bool(qc_data.get("qc_usp")),
                        bool(qc_data.get("qc_product")),
                        bool(qc_data.get("qc_review")),
                        bool(qc_data.get("qc_cta")),
                        bool(qc_data.get("qc_engaging")),
                        qc_data.get("qc_quality") or None,
                        total_score,
                        ", ".join(qc_data["qc_issue"]) if qc_data.get("qc_issue") else None,
                        final_status,
                        username,
                        post_id,
                    ),
                )
        return True, "ok"
    finally:
        conn.close()


# =====================================================
# AGENCY TARGET
# =====================================================

def fetch_all_agencies():
    """Fetch all agencies from public.agency_map"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, agency_name FROM public.agency_map ORDER BY agency_name")
            return cur.fetchall()
    finally:
        conn.close()


def fetch_distinct_industries():
    """Fetch distinct industries from leaderboard.tiktok_go_video_summary"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT industry_source FROM leaderboard.tiktok_go_video_summary WHERE industry_source IS NOT NULL ORDER BY industry_source"
            )
            return [row["industry_source"] for row in cur.fetchall()]
    finally:
        conn.close()


def fetch_agency_targets(agency_id: int = None) -> list:
    """Fetch agency targets, optionally filtered by agency_id"""
    sql = """
        SELECT
            at.id,
            at.agency_id,
            am.agency_name,
            at.industry,
            at.target_number,
            at.week_1,
            at.week_2,
            at.week_3,
            at.week_4,
            at.created_at,
            at.updated_at
        FROM target.agency_target at
        JOIN public.agency_map am ON at.agency_id = am.id
        WHERE 1=1
    """
    params = []

    if agency_id:
        sql += " AND at.agency_id = %s"
        params.append(agency_id)

    sql += " ORDER BY am.agency_name, at.industry"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def upsert_agency_target(agency_id: int, industry: str, target_number: int, week_1: int, week_2: int, week_3: int, week_4: int):
    """Insert or update an agency target"""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO target.agency_target (agency_id, industry, target_number, week_1, week_2, week_3, week_4, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (agency_id, industry)
                    DO UPDATE SET
                        target_number = EXCLUDED.target_number,
                        week_1 = EXCLUDED.week_1,
                        week_2 = EXCLUDED.week_2,
                        week_3 = EXCLUDED.week_3,
                        week_4 = EXCLUDED.week_4,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (agency_id, industry, target_number, week_1, week_2, week_3, week_4)
                )
                result = cur.fetchone()
                return result["id"] if result else None
    finally:
        conn.close()


def delete_agency_target(target_id: int):
    """Delete an agency target by ID"""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM target.agency_target WHERE id = %s", (target_id,))
    finally:
        conn.close()
