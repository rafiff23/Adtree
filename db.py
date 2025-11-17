import os
import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "Adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
        cursor_factory=RealDictCursor,
    )
    return conn


def insert_creator_registry_row(
    agency_name,
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
):
    sql = """
        INSERT INTO creator_registry (
            agency_name,
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
            notes
        )
        VALUES (
            %(agency_name)s,
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
            %(notes)s
        )
        RETURNING id;
    """

    params = {
        "agency_name": agency_name,
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
        "notes": notes,
    }

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                new_id = cur.fetchone()["id"]
        return new_id
    finally:
        conn.close()


def fetch_creator_registry(tiktok_id_filter=None, start_date=None, end_date=None):
    """
    Read rows from creator_registry with optional filters:
      - tiktok_id_filter: substring match on tiktok_id
      - start_date, end_date: filter on created_at::date
    """
    base_sql = """
        SELECT
            id,
            agency_name,
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
            created_at
        FROM creator_registry
        WHERE 1 = 1
    """

    params = {}

    if tiktok_id_filter:
        base_sql += " AND tiktok_id ILIKE %(tiktok_id)s"
        params["tiktok_id"] = f"%{tiktok_id_filter}%"

    if start_date:
        base_sql += " AND created_at::date >= %(start_date)s"
        params["start_date"] = start_date

    if end_date:
        base_sql += " AND created_at::date <= %(end_date)s"
        params["end_date"] = end_date

    base_sql += " ORDER BY created_at DESC"

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(base_sql, params)
                rows = cur.fetchall()
        return rows
    finally:
        conn.close()

def update_creator_registry_row(row_id, updated_fields):
    """
    Update only the fields provided in updated_fields dict.
    Example:
    update_creator_registry_row(5, {"full_name": "New Name", "followers": 5000})
    """
    if not updated_fields:
        return

    set_clause = ", ".join([f"{key} = %({key})s" for key in updated_fields.keys()])
    sql = f"""
        UPDATE creator_registry
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
