import streamlit as st
import pandas as pd

try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

from db import (
    prepare_content_qc_csv,
    upsert_content_qc_posts,
    fetch_content_qc_posts,
    get_content_qc_post_state,
    acquire_content_qc_lock,
    release_content_qc_lock,
    save_content_qc_status,
)

_QC_OPTIONS = ["", "Good", "Bad"]


# ─────────────────────────────────────────────────────────────────────────────
# Username gate
# ─────────────────────────────────────────────────────────────────────────────

def _username_gate() -> str | None:
    """
    Show a name-input screen if the user hasn't identified themselves yet.
    Returns the username once set, or None to stop rendering the rest of the page.
    """
    if st.session_state.get("cqc_username"):
        return st.session_state["cqc_username"]

    st.title("📹 Content QC")
    st.info("Masukkan nama kamu untuk mulai.")
    name = st.text_input("Nama:", key="cqc_name_input")
    if st.button("Mulai", type="primary") and name.strip():
        st.session_state["cqc_username"] = name.strip()
        st.rerun()
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Import tab
# ─────────────────────────────────────────────────────────────────────────────

def _render_import_tab():
    st.subheader("Import CSV Data")
    st.markdown(
        "Upload CSV yang didownload dari platform. Sistem akan:\n"
        "- Standarisasi format tanggal (`20260315` → `2026-03-15`)\n"
        "- Standarisasi Creator Level (`Lv. 2` → `2`)\n"
        "- **Insert** jika Post ID baru; **update hanya metrics** jika sudah ada\n"
        "- QC Status **tidak akan pernah tertimpa** oleh import"
    )

    uploaded = st.file_uploader("Upload CSV", type=["csv"])
    if not uploaded:
        return

    try:
        df_raw = pd.read_csv(uploaded, dtype=str)
    except Exception as e:
        st.error(f"Gagal membaca file: {e}")
        return

    st.caption(f"File loaded: **{len(df_raw):,} rows** · **{len(df_raw.columns)} columns**")

    df, unmapped = prepare_content_qc_csv(df_raw)

    if unmapped:
        st.warning(f"Kolom tidak dikenali dan akan diabaikan: {unmapped}")

    if "post_id" not in df.columns:
        st.error("Kolom **Post ID** tidak ditemukan. Pastikan CSV memiliki kolom tersebut.")
        return

    # Drop empty post_ids
    before = len(df)
    df = df[df["post_id"].notna() & (df["post_id"].str.strip() != "")].copy()
    df["post_id"] = df["post_id"].str.strip()
    if len(df) < before:
        st.warning(f"{before - len(df)} baris dibuang karena Post ID kosong.")

    if df.empty:
        st.error("Tidak ada data valid untuk diimport.")
        return

    st.write(f"Preview ({min(10, len(df))} baris pertama):")
    st.dataframe(df.head(10), use_container_width=True)

    if st.button("🚀 Import ke Database", type="primary"):
        with st.spinner("Mengimport data…"):
            rows = [
                {k: (None if isinstance(v, float) and pd.isna(v) else v)
                 for k, v in row.items()}
                for row in df.to_dict("records")
            ]
            try:
                inserted, updated = upsert_content_qc_posts(rows)
                st.success(
                    f"✅ Import selesai! "
                    f"**{inserted}** baris baru · **{updated}** baris diupdate."
                )
            except Exception as e:
                st.error(f"Import gagal: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# QC Review tab
# ─────────────────────────────────────────────────────────────────────────────

def _render_qc_tab(username: str):
    # ── Filters ──────────────────────────────────────────────────────────────
    with st.expander("🔍 Filter", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            qc_filter = st.selectbox(
                "QC Status", ["All", "Unreviewed", "Good", "Bad"], key="cqc_qc_filter"
            )
        with c2:
            date_from = st.date_input("Dari Tanggal", value=None, key="cqc_date_from")
        with c3:
            date_to = st.date_input("Sampai Tanggal", value=None, key="cqc_date_to")
        with c4:
            search = st.text_input("Cari (Post ID / Judul / Creator)", key="cqc_search")

    # ── Fetch data ────────────────────────────────────────────────────────────
    try:
        rows = fetch_content_qc_posts(
            qc_filter=qc_filter if qc_filter != "All" else None,
            date_from=date_from or None,
            date_to=date_to or None,
            search=search.strip() or None,
        )
    except Exception as e:
        st.error(f"Gagal memuat data: {e}")
        return

    if not rows:
        st.info("Tidak ada data yang sesuai filter.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    st.caption(f"Menampilkan **{len(df):,}** post")

    # ── Display table ─────────────────────────────────────────────────────────
    display = df[
        ["post_id", "post_title", "post_date", "creator_name",
         "creator_level", "video_views", "ctr", "cvr",
         "qc_status", "qc_updated_by", "locked_by"]
    ].copy()

    display["locked_by"] = display["locked_by"].apply(
        lambda v: f"🔒 {v}" if pd.notna(v) and v else ""
    )
    display["qc_status"] = display["qc_status"].fillna("—")

    st.dataframe(
        display.rename(columns={
            "post_id":       "Post ID",
            "post_title":    "Judul",
            "post_date":     "Tanggal",
            "creator_name":  "Creator",
            "creator_level": "Level",
            "video_views":   "Views",
            "ctr":           "CTR",
            "cvr":           "CVR",
            "qc_status":     "QC Status",
            "qc_updated_by": "Diupdate Oleh",
            "locked_by":     "Lock",
        }),
        use_container_width=True,
        height=380,
    )

    # ── Edit panel ────────────────────────────────────────────────────────────
    st.divider()
    _render_edit_panel(username, df)


# ─────────────────────────────────────────────────────────────────────────────
# Edit panel
# ─────────────────────────────────────────────────────────────────────────────

def _render_edit_panel(username: str, df: pd.DataFrame):
    st.subheader("Edit QC Status")

    post_ids = df["post_id"].tolist()

    # Keep selectbox on the post we're editing (if still visible after filter)
    editing_id = st.session_state.get("cqc_editing_id")
    default_idx = post_ids.index(editing_id) if editing_id in post_ids else 0

    def _label(pid):
        title = df.loc[df["post_id"] == pid, "post_title"].values
        return f"{pid}  —  {title[0] if len(title) else ''}"

    selected_id = st.selectbox(
        "Pilih Post:",
        options=post_ids,
        index=default_idx,
        format_func=_label,
        key="cqc_post_select",
    )

    selected_row = df[df["post_id"] == selected_id].iloc[0]
    locked_by_other = (
        pd.notna(selected_row.get("locked_by"))
        and selected_row.get("locked_by") != username
    )

    # ── Locked by someone else ────────────────────────────────────────────────
    if locked_by_other:
        st.warning(
            f"🔒 Post ini sedang diedit oleh **{selected_row['locked_by']}**. "
            "Tunggu sebentar atau pilih post lain."
        )
        # If we were editing this exact post (race condition), clean up our state
        if st.session_state.get("cqc_editing_id") == selected_id:
            st.session_state.pop("cqc_editing_id", None)
        return

    is_editing = st.session_state.get("cqc_editing_id") == selected_id

    # ── View mode ─────────────────────────────────────────────────────────────
    if not is_editing:
        current_qc = selected_row.get("qc_status")
        display_qc = current_qc if pd.notna(current_qc) and current_qc else "Belum diisi"
        st.metric("QC Status Saat Ini", display_qc)

        if st.button("✏️ Edit QC Status", type="primary", key="cqc_start_edit"):
            ok, msg = acquire_content_qc_lock(selected_id, username)
            if ok:
                state = get_content_qc_post_state(selected_id)
                st.session_state["cqc_editing_id"] = selected_id
                st.session_state["cqc_edit_expected_at"] = (
                    state["qc_updated_at"] if state else None
                )
                # Pre-fill selectbox with current value
                cur_val = state["qc_status"] if state and state["qc_status"] else ""
                st.session_state["cqc_status_select"] = cur_val
                st.rerun()
            else:
                st.error(f"🔒 {msg}")
        return

    # ── Edit mode ─────────────────────────────────────────────────────────────
    # On every rerun while editing, refresh our lock (keeps it alive)
    ok, msg = acquire_content_qc_lock(selected_id, username)
    if not ok:
        st.error(f"Lock hilang: {msg}")
        st.session_state.pop("cqc_editing_id", None)
        st.rerun()
        return

    st.info(f"Sedang mengedit: **{selected_id}**")

    new_status = st.selectbox(
        "QC Status",
        options=_QC_OPTIONS,
        index=_QC_OPTIONS.index(st.session_state.get("cqc_status_select", "")),
        format_func=lambda x: "— Belum diisi —" if x == "" else x,
        key="cqc_status_select",
    )

    col_save, col_cancel = st.columns(2)

    with col_save:
        if st.button("💾 Simpan", type="primary", key="cqc_save"):
            ok, msg = save_content_qc_status(
                selected_id,
                new_status,
                username,
                st.session_state.get("cqc_edit_expected_at"),
            )
            release_content_qc_lock(selected_id, username)
            st.session_state.pop("cqc_editing_id", None)
            if ok:
                st.success(f"✅ QC Status disimpan: **{new_status or '(kosong)'}**")
            else:
                st.error(msg)
            st.rerun()

    with col_cancel:
        if st.button("❌ Batal", key="cqc_cancel"):
            release_content_qc_lock(selected_id, username)
            st.session_state.pop("cqc_editing_id", None)
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def render():
    username = _username_gate()
    if not username:
        return

    st.title("📹 Content QC")

    col_title, col_user = st.columns([6, 1])
    with col_user:
        if st.button("🔄 Ganti Nama", key="cqc_reset_name"):
            editing = st.session_state.get("cqc_editing_id")
            if editing:
                release_content_qc_lock(editing, username)
            for key in ["cqc_username", "cqc_editing_id", "cqc_edit_expected_at"]:
                st.session_state.pop(key, None)
            st.rerun()
    with col_title:
        st.caption(f"Logged in as: **{username}**")

    # Auto-refresh every 30 s – paused while editing to avoid wiping the form
    if _HAS_AUTOREFRESH and not st.session_state.get("cqc_editing_id"):
        st_autorefresh(interval=30_000, key="cqc_autorefresh")
    elif not _HAS_AUTOREFRESH:
        if st.button("🔄 Refresh Data", key="cqc_manual_refresh"):
            st.rerun()

    tab_import, tab_qc = st.tabs(["📥 Import CSV", "✅ QC Review"])

    with tab_import:
        _render_import_tab()

    with tab_qc:
        _render_qc_tab(username)
