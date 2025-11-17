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
        dbname=os.getenv("PG_DB", "Adtree_Final"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "123"),
        cursor_factory=RealDictCursor,
    )
    return conn


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
    # Convert agency_name → agency_id
    agency_id = get_agency_id_by_name(agency_name)

    sql = """
        INSERT INTO public.creator_registry (
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
            notes
        )
        VALUES (
            %(agency_id)s,
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
        "agency_id": agency_id,
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
