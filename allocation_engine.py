import pandas as pd
import copy

# ============================================================
SHEET_REPL  = "Dec 15, 2025 - Replenishment"
SHEET_STAGE = "stage_reordering_rpt"
# ============================================================


def calculate_median_forecast(row):
    forecasts = [
        row.get('model_a_oos_suggested_qty'),
        row.get('model_b_oos_suggested_qty'),
        row.get('model_c_oos_suggested_qty'),
        row.get('model_new_oos_suggested_qty')
    ]
    valid = [f for f in forecasts if pd.notnull(f)]
    return pd.Series(valid).median() if len(valid) else 0


def get_non_bom_inventory_view(source_df):
    tmp = source_df.copy()
    for c in ['productId', 'Description', 'Is_BOM']:
        if c not in tmp.columns:
            tmp[c] = None

    inv_candidates = [c for c in tmp.columns if c.lower() == 'inventory_wh_bp']
    if inv_candidates:
        tmp.rename(columns={inv_candidates[0]: 'inventory_wh_bp'}, inplace=True)
    else:
        tmp['inventory_wh_bp'] = 0

    out = tmp[tmp['Is_BOM'] == 0][['productId', 'Description', 'inventory_wh_bp']].copy()
    out = out[pd.notnull(out['Description'])]
    out['Description'] = out['Description'].astype(str).str.strip()
    out['inventory_wh_bp'] = pd.to_numeric(out['inventory_wh_bp'], errors='coerce').fillna(0).astype(int)
    return out


def build_stock_dict(non_bom_df):
    d = {}
    for _, r in non_bom_df.iterrows():
        desc = r['Description']
        d[desc] = d.get(desc, 0) + int(r['inventory_wh_bp'])
    return d


def create_bom_mapping_from_excel(bom_file):
    df_bom = pd.read_excel(bom_file, sheet_name='BOM')
    bom_mapping = {}
    grouped = df_bom.groupby(['Product ID', 'Description'])
    for (product_id, description), group in grouped:
        components = {}
        for _, r in group.iterrows():
            comp_desc = r.get('Description3')
            if pd.isna(comp_desc):
                continue
            comp_desc = str(comp_desc).strip()
            qty = int(r.get('Quantity'))
            components[comp_desc] = qty
        bom_mapping[(description, product_id)] = components
    return bom_mapping


def has_stock_for_one(components, merged_stock):
    for comp, qty in components.items():
        if merged_stock.get(comp, 0) < qty:
            return False
    return True


def consume_one(components, merged_stock):
    for comp, qty in components.items():
        merged_stock[comp] = merged_stock.get(comp, 0) - qty
    return merged_stock


# ============================================================
# ===================== MAIN FUNCTION ========================
# ============================================================

def run_allocation(repl_file, bom_file, repl_sheet, stage_sheet):

    df = pd.read_excel(repl_file, sheet_name=repl_sheet)
    df['median_forecast'] = df.apply(calculate_median_forecast, axis=1)

    df_stage = pd.read_excel(repl_file, sheet_name=stage_sheet)

    non_bom_repl  = get_non_bom_inventory_view(df)
    non_bom_stage = get_non_bom_inventory_view(df_stage)

    stock_repl_initial  = build_stock_dict(non_bom_repl)
    stock_stage_initial = build_stock_dict(non_bom_stage)

    merged_initial   = copy.deepcopy(stock_repl_initial)
    stage_only_keys  = []

    for desc, qty in stock_stage_initial.items():
        if desc not in merged_initial:
            merged_initial[desc] = qty
            stage_only_keys.append(desc)

    merged_work = copy.deepcopy(merged_initial)

    component_id_map = {}
    for _, r in non_bom_repl.iterrows():
        component_id_map[r['Description']] = r['productId']
    for _, r in non_bom_stage.iterrows():
        component_id_map.setdefault(r['Description'], r['productId'])

    bom_mapping = create_bom_mapping_from_excel(bom_file)

    products = []

    # BOM products
    for _, row in df[df['Is_BOM'] == 1].iterrows():
        key = (row['Description'], row['productId'])
        comps = bom_mapping.get(key, {})
        products.append({
            'Product_ID': row['productId'],
            'Product': row['Description'],
            'Type': 'BOM',
            'Forecast_raw': row['median_forecast'],
            'Target_Forecast': abs(row['median_forecast']),
            'Components': comps,
            'Allocated': 0,
            'Components_Used': {},
            'Extra_Cols': row.to_dict()
        })

    # Non-BOM products
    for _, row in df[df['Is_BOM'] == 0].iterrows():
        products.append({
            'Product_ID': row['productId'],
            'Product': row['Description'],
            'Type': 'Non-BOM',
            'Forecast_raw': row['median_forecast'],
            'Target_Forecast': abs(row['median_forecast']),
            'Components': {row['Description']: 1},
            'Allocated': 0,
            'Components_Used': {},
            'Extra_Cols': row.to_dict()
        })

    # FIRST allocation pass (IMPORTANT)
    for p in products:
        if p['Target_Forecast'] > 0 and p['Allocated'] < p['Target_Forecast'] and \
           has_stock_for_one(p['Components'], merged_work):
            merged_work = consume_one(p['Components'], merged_work)
            for comp, qty in p['Components'].items():
                p['Components_Used'][comp] = p['Components_Used'].get(comp, 0) + qty
            p['Allocated'] += 1

    # WHILE loop allocation
    while True:
        any_alloc = False
        for p in products:
            remaining = p['Target_Forecast'] - p['Allocated']
            if remaining > 0 and has_stock_for_one(p['Components'], merged_work):
                merged_work = consume_one(p['Components'], merged_work)
                for comp, qty in p['Components'].items():
                    p['Components_Used'][comp] = p['Components_Used'].get(comp, 0) + qty
                p['Allocated'] += 1
                any_alloc = True
        if not any_alloc:
            break

    # ================= OUTPUT DATAFRAMES =================

    extra_columns = [
        "Special_Instructions", "BOM_Component", "ASIN", "In-Season_SPINS",
        "Off-Season_SPINS", "currently_in_season", "Supplier",
        "Current_FBA_Pricing_Strategy", "Current_FBM_Pricing_Strategy",
        "Sublocation_summary", "DaysOfSupplyAtAmazon", "Amazon_Alert",
        "InvAge0To90Days", "InvAge91To180Days", "InvAge181To270Days",
        "InvAge271To365Days", "InvAge365PlusDays", "Inbound", "Available",
        "inventory_wh_bp", "YoY_Trend", "total_at_or_for_amazon",
        "Required_Ship_Date", "model_a_oos_suggested_qty",
        "model_b_oos_suggested_qty", "model_c_oos_suggested_qty",
        "model_new_oos_suggested_qty"
    ]

    allocation_df = pd.DataFrame([{
        'Product_ID': p['Product_ID'],
        'Product': p['Product'],
        'Type': p['Type'],
        'Forecast': p['Forecast_raw'],
        'Allocated': p['Allocated'],
        'Components_Used': "; ".join(f"{c}: {q}" for c, q in p['Components_Used'].items()),
        **{col: p['Extra_Cols'].get(col, None) for col in extra_columns}
    } for p in products])

    stock_report_rows = []

    for k in sorted(stock_repl_initial.keys(), key=lambda x: str(x)):
        stock_report_rows.append({
            'Component_ID': component_id_map.get(k),
            'Component': k,
            'Initial_Stock': merged_initial.get(k, 0),
            'Remaining_Stock': merged_work.get(k, 0),
            'Source': 'Replenishment'
        })

    for k in sorted(stage_only_keys, key=lambda x: str(x)):
        stock_report_rows.append({
            'Component_ID': component_id_map.get(k),
            'Component': k,
            'Initial_Stock': merged_initial.get(k, 0),
            'Remaining_Stock': merged_work.get(k, 0),
            'Source': 'StageOnly'
        })

    stock_report_df = pd.DataFrame(stock_report_rows)

    usage_rows = []
    for p in products:
        for comp_desc, used_qty in p['Components_Used'].items():
            usage_rows.append({
                'Component': comp_desc,
                'Component_ID': component_id_map.get(comp_desc),
                'Product_ID': p['Product_ID'],
                'Product': p['Product'],
                'Qty_Used': used_qty,
                'Came_From': 'StageOnly' if comp_desc in stage_only_keys else 'Replenishment'
            })

    component_usage_df = pd.DataFrame(usage_rows)
    if not component_usage_df.empty:
        component_usage_df['Total_Used_Component'] = (
            component_usage_df.groupby(['Component', 'Component_ID'])['Qty_Used'].transform('sum')
        )

    return allocation_df, stock_report_df, component_usage_df
