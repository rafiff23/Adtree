import streamlit as st
import psycopg2
from db import get_connection, fetch_all_leaderboard_rules, upsert_leaderboard_rule


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_agencies():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, agency_name FROM public.agency_map ORDER BY id")
            return cur.fetchall()
    finally:
        conn.close()


def insert_agency(agency_name: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.agency_map (agency_name) VALUES (%s) RETURNING id",
                (agency_name,),
            )
            new_id = cur.fetchone()["id"]
        conn.commit()
        return new_id
    finally:
        conn.close()


def update_agency(agency_id: int, agency_name: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.agency_map SET agency_name = %s WHERE id = %s",
                (agency_name, agency_id),
            )
        conn.commit()
    finally:
        conn.close()


# ── Page ──────────────────────────────────────────────────────────────────────

def _render_agency_section_body():
    st.markdown("### Agency / Vendor List")
    st.caption("Manage the agency list used across Creator Registry and Vendor Import.")

    agencies = fetch_agencies()

    if agencies:
        st.dataframe(
            [{"ID": a["id"], "Agency Name": a["agency_name"]} for a in agencies],
            use_container_width=True,
        )
    else:
        st.info("No agencies found. Add one below.")

    st.divider()

    st.markdown("#### Add New Agency")
    with st.form("add_agency_form", clear_on_submit=True):
        new_name = st.text_input("Agency Name", placeholder="e.g. Vendor ABC")
        submitted = st.form_submit_button("Add")
        if submitted:
            if not new_name.strip():
                st.error("Agency name cannot be empty.")
            else:
                try:
                    new_id = insert_agency(new_name.strip())
                    st.success(f"Agency added with ID {new_id}.")
                    st.rerun()
                except psycopg2.errors.UniqueViolation:
                    st.error("An agency with that name already exists.")
                except Exception as e:
                    st.error(f"Failed to add agency: {e}")

    st.divider()

    if agencies:
        st.markdown("#### Edit Agency Name")
        options = {f"{a['id']} — {a['agency_name']}": a for a in agencies}
        choice = st.selectbox("Select agency to edit", list(options.keys()))
        selected = options[choice]

        with st.form("edit_agency_form"):
            edited_name = st.text_input("New Name", value=selected["agency_name"])
            save = st.form_submit_button("Save")
            if save:
                if not edited_name.strip():
                    st.error("Agency name cannot be empty.")
                elif edited_name.strip() == selected["agency_name"]:
                    st.info("No changes made.")
                else:
                    try:
                        update_agency(selected["id"], edited_name.strip())
                        st.success("Agency updated.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to update agency: {e}")


def _render_leaderboard_rules():
    st.markdown("### Leaderboard Rules")
    st.caption("Thresholds and prize labels for each leaderboard program. Changes take effect within 10 minutes.")

    _ALL_PROGRAMS = [
        "accommodation_monthly", "accommodation_weekly",
        "fnb_monthly",           "fnb_weekly",
        "attraction_monthly",    "attraction_weekly",
    ]

    try:
        existing = {r["program_key"]: dict(r) for r in fetch_all_leaderboard_rules()}
    except Exception as e:
        st.error(f"Could not load rules: {e}")
        st.code(
            "CREATE TABLE IF NOT EXISTS public.leaderboard_rules (\n"
            "    program_key   VARCHAR(50) PRIMARY KEY,\n"
            "    min_gmv_idr   BIGINT      NOT NULL DEFAULT 0,\n"
            "    min_videos    INT         NOT NULL DEFAULT 0,\n"
            "    min_merchants INT         NOT NULL DEFAULT 0,\n"
            "    max_slots     INT         NOT NULL DEFAULT 10,\n"
            "    prize_idr     INT         NOT NULL DEFAULT 0,\n"
            "    prize_label   VARCHAR(200),\n"
            "    title_full    VARCHAR(200),\n"
            "    title_main    VARCHAR(100),\n"
            "    title_accent  VARCHAR(100),\n"
            "    updated_at    TIMESTAMP   DEFAULT NOW()\n"
            ");",
            language="sql",
        )
        return

    selected_program = st.selectbox("Program", _ALL_PROGRAMS, format_func=lambda k: k.replace("_", " ").title())
    row = existing.get(selected_program, {})

    with st.form(f"rules_form_{selected_program}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            min_gmv_idr   = st.number_input("Min GMV (IDR)",   value=int(row.get("min_gmv_idr",   0)), step=1_000_000)
            min_videos    = st.number_input("Min Videos",       value=int(row.get("min_videos",    0)), step=1)
        with col2:
            min_merchants = st.number_input("Min Merchants",    value=int(row.get("min_merchants", 0)), step=1)
            max_slots     = st.number_input("Max Slots",        value=int(row.get("max_slots",    10)), step=1)
        with col3:
            prize_idr     = st.number_input("Prize (IDR)",      value=int(row.get("prize_idr",     0)), step=50_000)
            prize_label   = st.text_input( "Prize Label",       value=row.get("prize_label") or "")

        st.markdown("**Titles**")
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            title_full   = st.text_input("Full Title",   value=row.get("title_full")   or "")
        with tc2:
            title_main   = st.text_input("Title Main",   value=row.get("title_main")   or "")
        with tc3:
            title_accent = st.text_input("Title Accent", value=row.get("title_accent") or "")

        if st.form_submit_button("Save"):
            try:
                upsert_leaderboard_rule(selected_program, {
                    "min_gmv_idr":   min_gmv_idr,
                    "min_videos":    min_videos,
                    "min_merchants": min_merchants,
                    "max_slots":     max_slots,
                    "prize_idr":     prize_idr,
                    "prize_label":   prize_label.strip() or None,
                    "title_full":    title_full.strip()   or None,
                    "title_main":    title_main.strip()   or None,
                    "title_accent":  title_accent.strip() or None,
                })
                st.success(f"Rules saved for **{selected_program}**.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Failed to save: {e}")

    if existing:
        with st.expander("All current rules"):
            import pandas as pd
            st.dataframe(pd.DataFrame(list(existing.values())).drop(columns=["updated_at"], errors="ignore"),
                         use_container_width=True)


def render():
    st.title("⚙️ Settings")

    tab_agency, tab_rules = st.tabs(["Agency / Vendor List", "Leaderboard Rules"])

    with tab_agency:
        _render_agency_section_body()

    with tab_rules:
        _render_leaderboard_rules()


if __name__ == "__main__":
    render()

