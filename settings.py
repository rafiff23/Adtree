import streamlit as st
import psycopg2
from db import get_connection


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

def render():
    st.title("⚙️ Settings")
    st.markdown("### Agency / Vendor List")
    st.caption("Manage the agency list used across Creator Registry and Vendor Import.")

    agencies = fetch_agencies()

    # ── Current list ──────────────────────────────────────────────────────────
    if agencies:
        st.dataframe(
            [{"ID": a["id"], "Agency Name": a["agency_name"]} for a in agencies],
            use_container_width=True,
        )
    else:
        st.info("No agencies found. Add one below.")

    st.divider()

    # ── Add new agency ────────────────────────────────────────────────────────
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

    # ── Edit existing agency ──────────────────────────────────────────────────
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


if __name__ == "__main__":
    render()
