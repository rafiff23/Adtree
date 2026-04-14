import streamlit as st
import pandas as pd
from db import (
    fetch_all_agencies,
    fetch_agency_targets,
    upsert_agency_target,
    delete_agency_target,
    fetch_distinct_industries,
    get_agency_target_by_id,
    update_agency_target,
)


def render():
    """Render the Agency Target page"""
    st.title("Agency Target Management")
    st.write("Manage and track industry targets for each agency by week.")

    # Fetch agencies, industries, and targets
    agencies = fetch_all_agencies()
    agency_dict = {agency["agency_name"]: agency["id"] for agency in agencies}
    agency_options = sorted(list(agency_dict.keys()))

    industries = fetch_distinct_industries()

    all_targets = fetch_agency_targets()

    # Create two columns: form and view
    col1, col2 = st.columns([1, 2])

    # ============== LEFT COLUMN: FORM ==============
    with col1:
        st.subheader("Add/Edit Target")

        # Mode selector
        mode = st.radio("Mode", options=["Add New", "Edit Existing"], horizontal=True)

        # For edit mode, select which target to edit
        edit_target = None
        if mode == "Edit Existing":
            if all_targets:
                target_options = [
                    f"{t['agency_name']} - {t['industry']} (Target: {t['target_number']})"
                    for t in all_targets
                ]
                selected_edit = st.selectbox("Select target to edit", options=target_options, key="edit_select")

                # Find the selected target
                selected_idx = target_options.index(selected_edit)
                edit_target = all_targets[selected_idx]
            else:
                st.warning("No targets available to edit.")
                edit_target = None

        with st.form("agency_target_form", clear_on_submit=mode == "Add New"):
            # Select Agency
            selected_agency = st.selectbox(
                "Agency Name *",
                options=agency_options,
                value=edit_target["agency_name"] if edit_target else agency_options[0],
                disabled=mode == "Edit Existing",
                help="Select the agency to set targets for",
            )

            # Select Industry
            if industries:
                industry = st.selectbox(
                    "Industry *",
                    options=industries,
                    value=edit_target["industry"] if edit_target else industries[0],
                    disabled=mode == "Edit Existing",
                    help="Select the industry or category",
                )
            else:
                st.warning("No industries found in the database. Please ensure leaderboard.tiktok_go_video_summary has data.")
                industry = None

            # Target Number
            target_number = st.number_input(
                "Number Target *",
                min_value=0,
                step=1,
                value=int(edit_target["target_number"]) if edit_target else 0,
                help="Total target for this industry",
            )

            # Weekly Targets
            st.write("**Weekly Targets:**")
            col_w1, col_w2, col_w3, col_w4 = st.columns(4)

            with col_w1:
                week_1 = st.number_input("Week 1", min_value=0, step=1, key="w1", value=int(edit_target["week_1"]) if edit_target else 0)

            with col_w2:
                week_2 = st.number_input("Week 2", min_value=0, step=1, key="w2", value=int(edit_target["week_2"]) if edit_target else 0)

            with col_w3:
                week_3 = st.number_input("Week 3", min_value=0, step=1, key="w3", value=int(edit_target["week_3"]) if edit_target else 0)

            with col_w4:
                week_4 = st.number_input("Week 4", min_value=0, step=1, key="w4", value=int(edit_target["week_4"]) if edit_target else 0)

            button_label = "Update Target" if mode == "Edit Existing" else "Save Target"
            submitted = st.form_submit_button(button_label, use_container_width=True)

            if submitted:
                if not industries:
                    st.error("No industries available. Please check the database.")
                    st.stop()

                if not industry:
                    st.error("Industry is required.")
                    st.stop()

                if target_number <= 0:
                    st.error("Number Target must be greater than 0.")
                    st.stop()

                try:
                    if mode == "Edit Existing":
                        if not edit_target:
                            st.error("Please select a target to edit.")
                            st.stop()

                        update_agency_target(
                            target_id=edit_target["id"],
                            target_number=int(target_number),
                            week_1=int(week_1),
                            week_2=int(week_2),
                            week_3=int(week_3),
                            week_4=int(week_4),
                        )
                        st.success(f"Target updated successfully! ID: {edit_target['id']}")
                    else:
                        agency_id = agency_dict[selected_agency]
                        target_id = upsert_agency_target(
                            agency_id=agency_id,
                            industry=industry,
                            target_number=int(target_number),
                            week_1=int(week_1),
                            week_2=int(week_2),
                            week_3=int(week_3),
                            week_4=int(week_4),
                        )
                        st.success(f"Target saved successfully! ID: {target_id}")

                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving target: {e}")

    # ============== RIGHT COLUMN: VIEW ==============
    with col2:
        st.subheader("All Targets")

        if all_targets:
            # Prepare data for display
            display_data = []
            for target in all_targets:
                display_data.append({
                    "ID": target["id"],
                    "Agency": target["agency_name"],
                    "Industry": target["industry"],
                    "Target": target["target_number"],
                    "Week 1": target["week_1"],
                    "Week 2": target["week_2"],
                    "Week 3": target["week_3"],
                    "Week 4": target["week_4"],
                })

            df = pd.DataFrame(display_data)

            # Display table
            st.dataframe(
                df.drop("ID", axis=1),
                use_container_width=True,
                hide_index=True,
            )

            # Delete functionality
            st.write("---")
            st.write("**Delete Target:**")

            col_del1, col_del2 = st.columns([2, 1])
            with col_del1:
                target_to_delete = st.selectbox(
                    "Select target to delete",
                    options=[
                        f"{t['Agency']} - {t['Industry']} (Target: {t['Target']})"
                        for t in display_data
                    ],
                    key="delete_select",
                )

            with col_del2:
                if st.button("Delete", use_container_width=True, type="secondary"):
                    # Find the ID of the selected target
                    selected_idx = [
                        f"{t['Agency']} - {t['Industry']} (Target: {t['Target']})"
                        for t in display_data
                    ].index(target_to_delete)
                    target_id = display_data[selected_idx]["ID"]

                    try:
                        delete_agency_target(target_id)
                        st.success("Target deleted successfully!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting target: {e}")
        else:
            st.info("No targets found. Add one to get started!")

    # Filter by Agency Section
    st.divider()
    st.subheader("View by Agency")

    selected_filter_agency = st.selectbox(
        "Filter by Agency",
        options=["All Agencies"] + agency_options,
        key="filter_agency",
    )

    if selected_filter_agency == "All Agencies":
        filtered_targets = all_targets
    else:
        agency_id = agency_dict[selected_filter_agency]
        filtered_targets = fetch_agency_targets(agency_id)

    if filtered_targets:
        # Prepare filtered data
        filtered_data = []
        for target in filtered_targets:
            filtered_data.append({
                "Agency": target["agency_name"],
                "Industry": target["industry"],
                "Target": target["target_number"],
                "Week 1": target["week_1"],
                "Week 2": target["week_2"],
                "Week 3": target["week_3"],
                "Week 4": target["week_4"],
                "Total": target["week_1"] + target["week_2"] + target["week_3"] + target["week_4"],
            })

        df_filtered = pd.DataFrame(filtered_data)

        # Display metrics
        if selected_filter_agency != "All Agencies":
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            total_target = df_filtered["Target"].sum()
            total_weekly = df_filtered["Total"].sum()
            avg_target_per_industry = total_target / len(df_filtered) if len(df_filtered) > 0 else 0

            with col_m1:
                st.metric("Total Industries", len(df_filtered))
            with col_m2:
                st.metric("Total Target", total_target)
            with col_m3:
                st.metric("Total Weekly", total_weekly)
            with col_m4:
                st.metric("Avg per Industry", f"{avg_target_per_industry:.0f}")

        # Display table
        st.dataframe(
            df_filtered,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(f"No targets found for {selected_filter_agency}.")
