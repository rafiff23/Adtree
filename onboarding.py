import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import io
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ── DB ────────────────────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
        cursor_factory=RealDictCursor,
    )

# ── XLSX template helpers ──────────────────────────────────────────────────────
REGISTRY_COLUMNS = [
    "tiktok_id", "followers", "full_name", "domicile",
    "uid", "phone_number", "tiktok_link", "binding_status",
    "onboarding_date", "level",
]

HEADER_FILL   = PatternFill("solid", start_color="1E293B", end_color="1E293B")
HEADER_FONT   = Font(bold=True, color="FFFFFF", name="Arial", size=10)
CELL_FONT     = Font(name="Arial", size=10)
BORDER_SIDE   = Side(style="thin", color="CBD5E1")
CELL_BORDER   = Border(left=BORDER_SIDE, right=BORDER_SIDE, top=BORDER_SIDE, bottom=BORDER_SIDE)
CENTER        = Alignment(horizontal="center", vertical="center")
LEFT          = Alignment(horizontal="left", vertical="center")

def _apply_header(ws, columns):
    ws.row_dimensions[1].height = 30
    for ci, col in enumerate(columns, 1):
        cell = ws.cell(row=1, column=ci, value=col)
        cell.fill   = HEADER_FILL
        cell.font   = HEADER_FONT
        cell.border = CELL_BORDER
        cell.alignment = CENTER
        ws.column_dimensions[cell.column_letter].width = 22


def make_registry_template_bytes() -> bytes:
    """Blank template for Creator Registry import."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Creator Registry"
    _apply_header(ws, REGISTRY_COLUMNS)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def make_unmatched_template_bytes(unmatched_ids: list) -> bytes:
    """
    Template pre-filled with unmatched tiktok_ids.
    All other DB columns are blank for the user to fill in.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Unmatched Creators"
    _apply_header(ws, REGISTRY_COLUMNS)

    for ri, tid in enumerate(unmatched_ids, 2):
        ws.row_dimensions[ri].height = 20
        for ci, col in enumerate(REGISTRY_COLUMNS, 1):
            val = tid if col == "tiktok_id" else ""
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.font   = CELL_FONT
            cell.border = CELL_BORDER
            cell.alignment = LEFT

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── ONBOARDING IMPORTER ───────────────────────────────────────────────────────
def run_onboarding_importer():
    st.markdown("### 📅 Onboarding Date Importer")
    st.caption(
        "Upload an XLSX that contains **'Unique ID'** and **'Date Joined'** columns. "
        "Matched creators will have their `onboarding_date` and `month_label` updated in the DB."
    )

    uploaded = st.file_uploader("Upload XLSX", type=["xlsx"], key="onboarding_upload")
    if not uploaded:
        return

    # Read xlsx
    try:
        df = pd.read_excel(uploaded, dtype=str)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        return

    required = {"Unique ID", "Date joined"}
    missing = required - set(df.columns)
    if missing:
        st.error(f"Missing required columns: {missing}")
        return

    df["_uid_norm"] = df["Unique ID"].str.strip().str.lower()
    df["_date_raw"] = pd.to_datetime(df["Date joined"], errors="coerce")

    st.markdown(f"**Rows in file:** {len(df)}")

    if st.button("▶ Run Import", key="onboarding_run"):
        try:
            conn = get_connection()
        except Exception as e:
            st.error(f"DB connection failed: {e}")
            return

        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tiktok_id FROM public.creator_registry")
                db_ids = {row["tiktok_id"].strip().lower() for row in cur.fetchall()}

        matched_rows   = df[df["_uid_norm"].isin(db_ids)]
        unmatched_rows = df[~df["_uid_norm"].isin(db_ids)]

        updated, skipped_bad_date = 0, 0

        if len(matched_rows):
            with conn:
                with conn.cursor() as cur:
                    for _, row in matched_rows.iterrows():
                        if pd.isna(row["_date_raw"]):
                            skipped_bad_date += 1
                            continue
                        date_val   = row["_date_raw"].date()
                        month_label = row["_date_raw"].strftime("%B %Y")
                        cur.execute(
                            """
                            UPDATE public.creator_registry
                               SET onboarding_date = %s,
                                   month_label     = %s
                             WHERE LOWER(TRIM(tiktok_id)) = %s
                            """,
                            (date_val, month_label, row["_uid_norm"]),
                        )
                        updated += 1

        # ── Results ──
        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Updated",   updated)
        col2.metric("⚠️ Unmatched", len(unmatched_rows))
        col3.metric("🗓 Bad dates",  skipped_bad_date)

        if updated:
            st.success(f"{updated} creator(s) updated successfully.")

        if len(unmatched_rows):
            st.warning(
                f"{len(unmatched_rows)} ID(s) not found in DB. "
                "Download the template below, fill in the missing details, "
                "then use the **Creator Registry** importer to add them."
            )
            unmatched_ids = unmatched_rows["Unique ID"].str.strip().tolist()
            xlsx_bytes = make_unmatched_template_bytes(unmatched_ids)
            st.download_button(
                label="⬇ Download Unmatched Template",
                data=xlsx_bytes,
                file_name=f"unmatched_creators_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_unmatched",
            )

            with st.expander("Preview unmatched IDs"):
                st.dataframe(unmatched_rows[["Unique ID", "Date joined"]], use_container_width=True)

        conn.close()


# ── CREATOR REGISTRY IMPORTER ─────────────────────────────────────────────────
def run_registry_importer():
    st.markdown("### 👤 Creator Registry Importer")
    st.caption(
        "Use this tab to insert **new** creators into the DB. "
        "Download the template, fill in the details (one row per creator), then upload."
    )

    # Template download
    st.markdown("#### Step 1 — Download Template")
    st.download_button(
        label="⬇ Download Blank Template",
        data=make_registry_template_bytes(),
        file_name="creator_registry_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_registry_template",
    )

    st.markdown("#### Step 2 — Upload Filled Template")
    uploaded = st.file_uploader("Upload filled template", type=["xlsx"], key="registry_upload")
    if not uploaded:
        return

    try:
        df = pd.read_excel(uploaded, dtype=str)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        return

    # Validate columns
    missing = set(REGISTRY_COLUMNS) - set(df.columns)
    if missing:
        st.error(f"Template columns missing: {missing}. Please use the provided template.")
        return

    df = df[REGISTRY_COLUMNS].copy()
    df["tiktok_id"] = df["tiktok_id"].str.strip()
    df = df[df["tiktok_id"].notna() & (df["tiktok_id"] != "")]

    # Parse onboarding_date → also derive month_label
    df["_date"] = pd.to_datetime(df["onboarding_date"], errors="coerce")

    st.markdown(f"**Rows ready to insert:** {len(df)}")

    bad_dates = df[df["_date"].isna() & df["onboarding_date"].notna() & (df["onboarding_date"] != "")]
    if len(bad_dates):
        st.warning(f"{len(bad_dates)} row(s) have unparseable dates — they will be inserted with NULL onboarding_date.")

    with st.expander("Preview data"):
        st.dataframe(df[REGISTRY_COLUMNS].head(20), use_container_width=True)

    if st.button("▶ Insert into DB", key="registry_run"):
        try:
            conn = get_connection()
        except Exception as e:
            st.error(f"DB connection failed: {e}")
            return

        # Check for existing tiktok_ids to avoid duplicates
        with conn.cursor() as cur:
            cur.execute("SELECT tiktok_id FROM public.creator_registry")
            existing = {row["tiktok_id"].strip().lower() for row in cur.fetchall()}

        inserted, skipped_dup = 0, 0
        errors = []

        with conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    if row["tiktok_id"].lower() in existing:
                        skipped_dup += 1
                        continue

                    date_val    = row["_date"].date() if pd.notna(row["_date"]) else None
                    month_label = row["_date"].strftime("%B %Y") if pd.notna(row["_date"]) else None

                    def _val(col):
                        v = row.get(col, "")
                        return v if (pd.notna(v) and str(v).strip() != "") else None

                    try:
                        cur.execute(
                            """
                            INSERT INTO public.creator_registry
                                (tiktok_id, followers, full_name, domicile, uid,
                                 phone_number, tiktok_link, binding_status,
                                 onboarding_date, month_label, agency_id, notes)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                row["tiktok_id"],
                                _val("followers"),
                                _val("full_name"),
                                _val("domicile"),
                                _val("uid"),
                                _val("phone_number"),
                                _val("tiktok_link"),
                                _val("binding_status"),
                                date_val,
                                month_label,
                                1,      # agency_id auto-fill
                                None,   # notes left blank
                            ),
                        )
                        inserted += 1
                    except Exception as e:
                        errors.append(f"Row {row['tiktok_id']}: {e}")

        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Inserted",   inserted)
        col2.metric("⏭ Duplicates", skipped_dup)
        col3.metric("❌ Errors",     len(errors))

        if inserted:
            st.success(f"{inserted} creator(s) inserted successfully.")
        if skipped_dup:
            st.info(f"{skipped_dup} row(s) skipped — tiktok_id already exists in DB.")
        if errors:
            with st.expander("Error details"):
                for e in errors:
                    st.error(e)

        conn.close()


# ── PAGE ──────────────────────────────────────────────────────────────────────
def render():
    st.title("🎬 Creator Importer")

    tab_onboarding, tab_registry = st.tabs(["📅 Onboarding Date", "👤 Creator Registry"])

    with tab_onboarding:
        run_onboarding_importer()

    with tab_registry:
        run_registry_importer()


if __name__ == "__main__":
    render()