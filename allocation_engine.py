import pandas as pd
import copy

# ============================================================
# ID NORMALIZATION (TEXT SAFE)
# ============================================================
def normalize_id(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    if x.endswith(".0"):
        x = x[:-2]
    return x

# ============================================================
# MEDIAN FORECAST
# ============================================================
def calculate_median_forecast(row):
    forecasts = [
        row.get('model_a_oos_suggested_qty'),
        row.get('model_b_oos_suggested_qty'),
        row.get('model_c_oos_suggested_qty'),
        row.get('model_new_oos_suggested_qty')
    ]
    valid = [f for f in forecasts if pd.notnull(f)]
    return int(pd.Series(valid).median()) if valid else 0

# ============================================================
# BUILD NON-BOM STOCK (BY PRODUCT ID)
# ============================================================
def build_stock(df):
    stock = {}
    df = df[df['Is_BOM'] == 0].copy()

    df['productId'] = df['productId'].apply(normalize_id)
    df['inventory_wh_bp'] = pd.to_numeric(
        df.get('inventory_wh_bp', 0),
        errors='coerce'
    ).fillna(0).astype(int)

    for _, r in df.iterrows():
        pid = r['productId']
        if pid:
            stock[pid] = stock.get(pid, 0) + r['inventory_wh_bp']
    return stock

# ============================================================
# BUILD BOM MAPPING
# ============================================================
def create_bom_mapping(bom_file):
    bom = pd.read_excel(bom_file, sheet_name="BOM")

    bom['Product ID'] = bom['Product ID'].apply(normalize_id)
    bom['Component Product ID'] = bom['Component Product ID'].apply(normalize_id)

    bom = bom[
        bom['Product ID'].notna() &
        bom['Component Product ID'].notna() &
        bom['Quantity'].notna()
    ]

    bom_mapping = {}
    for pid, grp in bom.groupby('Product ID'):
        comps = {}
        for _, r in grp.iterrows():
            try:
                qty = int(float(r['Quantity']))
                if qty > 0:
                    comps[r['Component Product ID']] = qty
            except (ValueError, TypeError):
                continue
        bom_mapping[pid] = comps

    return bom_mapping

# ============================================================
# ALLOCATION HELPERS
# ============================================================
def can_allocate(components, stock):
    return all(stock.get(c, 0) >= q for c, q in components.items())

def allocate_one(components, stock):
    for c, q in components.items():
        stock[c] -= q

# ============================================================
# ===================== MAIN FUNCTION ========================
# ============================================================
def run_allocation(repl_file, bom_file, repl_sheet, stage_sheet):

    # -------- Load data --------
    df = pd.read_excel(repl_file, sheet_name=repl_sheet)
    df_stage = pd.read_excel(repl_file, sheet_name=stage_sheet)

    df['productId'] = df['productId'].apply(normalize_id)
    df_stage['productId'] = df_stage['productId'].apply(normalize_id)

    df['median_forecast'] = df.apply(calculate_median_forecast, axis=1)

    # -------- Stock --------
    stock_repl = build_stock(df)
    stock_stage = build_stock(df_stage)

    merged_stock_initial = copy.deepcopy(stock_repl)
    stage_only_keys = []

    for k, v in stock_stage.items():
        if k not in merged_stock_initial:
            merged_stock_initial[k] = v
            stage_only_keys.append(k)

    merged_stock = copy.deepcopy(merged_stock_initial)

    # -------- BOM --------
    bom_mapping = create_bom_mapping(bom_file)

    # -------- Products --------
    products = []

    for _, row in df.iterrows():
        pid = row['productId']

        if row['Is_BOM'] == 1:
            components = bom_mapping.get(pid, {})
            ptype = 'BOM'
        else:
            components = {pid: 1}
            ptype = 'Non-BOM'

        products.append({
            'Product_ID': pid,
            'Product': row['Description'],
            'Type': ptype,
            'Forecast': row['median_forecast'],
            'Target': abs(row['median_forecast']),
            'Allocated': 0,
            'Components': components,
            'Components_Used': {},
            'Extra': row.to_dict()   # 👈 keep full original row
        })

    # -------- Fair Round-Robin Allocation --------
    while True:
        any_alloc = False
        for p in products:
            if p['Allocated'] >= p['Target']:
                continue
            if not p['Components']:
                continue
            if can_allocate(p['Components'], merged_stock):
                allocate_one(p['Components'], merged_stock)
                for c, q in p['Components'].items():
                    p['Components_Used'][c] = p['Components_Used'].get(c, 0) + q
                p['Allocated'] += 1
                any_alloc = True
        if not any_alloc:
            break

    # ============================================================
    # PRODUCT ALLOCATION OUTPUT (ENRICHED)
    # ============================================================
    allocation_rows = []

    for p in products:
        row = {
            'Product_ID': p['Product_ID'],
            'Product': p['Product'],
            'Type': p['Type'],
            'Forecast': p['Forecast'],
            'Allocated': p['Allocated'],
            'Components_Used': "; ".join(
                f"{k}:{v}" for k, v in p['Components_Used'].items()
            )
        }

        # 👇 ADD ALL ORIGINAL REPLENISHMENT COLUMNS
        row.update(p['Extra'])

        allocation_rows.append(row)

    allocation_df = pd.DataFrame(allocation_rows)

    # -------- Stock Report --------
    stock_rows = []
    for pid in merged_stock_initial:
        stock_rows.append({
            'Component_ID': pid,
            'Initial_Stock': merged_stock_initial.get(pid, 0),
            'Remaining_Stock': merged_stock.get(pid, 0),
            'Source': 'StageOnly' if pid in stage_only_keys else 'Replenishment'
        })

    stock_df = pd.DataFrame(stock_rows)

    # -------- Component Usage --------
    usage_rows = []
    for p in products:
        for c, q in p['Components_Used'].items():
            usage_rows.append({
                'Component_ID': c,
                'Product_ID': p['Product_ID'],
                'Product': p['Product'],
                'Qty_Used': q
            })

    usage_df = pd.DataFrame(usage_rows)
    if not usage_df.empty:
        usage_df['Total_Used_Component'] = (
            usage_df.groupby('Component_ID')['Qty_Used'].transform('sum')
        )

    return allocation_df, stock_df, usage_df
