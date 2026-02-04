import streamlit as st
import pandas as pd
from io import BytesIO
from allocation_engine import run_allocation

st.set_page_config(page_title="Fair Allocation Tool", layout="wide")

st.title("📦 Fair Allocation Tool")

st.info("Upload ** The Replenishment Excel** file. BOM is fixed and managed internally ")

repl_file = st.file_uploader(
    "Upload Replenishment Excel",
    type=["xlsx"]
)

if repl_file:

    xls = pd.ExcelFile(repl_file)

    repl_sheet = st.selectbox(
        "Select Replenishment Sheet",
        xls.sheet_names
    )

    stage_sheet = st.selectbox(
        "Select Stage Sheet",
        xls.sheet_names,
        index=1 if len(xls.sheet_names) > 1 else 0
    )

    if st.button("🚀 Run Allocation"):
        with st.spinner("Processing..."):
            try:
                alloc_df, stock_df, usage_df = run_allocation(
                    repl_file=repl_file,
                    bom_file="BOM.xlsx",
                    repl_sheet=repl_sheet,
                    stage_sheet=stage_sheet
                )

                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    alloc_df.to_excel(writer, sheet_name="Product_Allocation", index=False)
                    stock_df.to_excel(writer, sheet_name="Stock_Report", index=False)
                    usage_df.to_excel(writer, sheet_name="Component_Usage", index=False)

                st.success("Allocation completed successfully!")

                st.download_button(
                    label="⬇️ Download Output Excel",
                    data=output.getvalue(),
                    file_name="fair_allocation.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"❌ Error: {e}")


