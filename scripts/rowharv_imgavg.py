import pandas as pd
import os

# ### 2023-24

# # ─── CONFIG ─────────────────────────────────────────────────────────────────────
# HARVEST_FILE = r'F:\Personal\Career\Harvest_ai_5\calibration project\kg je Reihe Saison 2023-24 Mitteltross.xlsx'
# VIDEO_FILE   = r'F:\Personal\Career\Harvest_ai_5\calibration project\fruit_count_avg.csv'
# OUTPUT_FILE  = 'training_dataset_2023-24.csv'

# # ─── STEP 1: READ & CLEAN HARVEST DATA WITH SPLIT HEADERS ───────────────────────
# # Pfad columns are on Excel row 6, Datum on row 7 → pandas header=[5,6]
# df_raw = pd.read_excel(
#     HARVEST_FILE,
#     sheet_name='Sheet',
#     header=[5, 6]
# )

# # Flatten the MultiIndex columns:
# #  - keep top-level names for 'Pfad ...'
# #  - rename the bottom-level 'Datum' to 'harvest_date'
# new_cols = []
# for top, bot in df_raw.columns:
#     if isinstance(top, str) and top.startswith('Pfad'):
#         new_cols.append(top)
#     elif bot == 'Datum':
#         new_cols.append('harvest_date')
#     else:
#         # fallback (shouldn’t be used)
#         new_cols.append(bot or top)
# df_raw.columns = new_cols

# # select only the columns we need
# df_h = df_raw[['harvest_date', 'Pfad 109', 'Pfad 090', 'Pfad 079']].copy()
# df_h = df_h.loc[:, ~df_h.columns.duplicated()]
# df_h['harvest_date'] = pd.to_datetime(df_h['harvest_date'], errors='coerce')
# df_h = df_h.dropna(subset=['harvest_date'])

# # restrict to your season window
# # start_dt = pd.to_datetime('2022-12-01')
# # end_dt   = pd.to_datetime('2023-05-02')
# # df_h = df_h[(df_h['harvest_date'] >= start_dt) & (df_h['harvest_date'] <= end_dt)]

# print("▶️ Harvest file:", HARVEST_FILE)
# print("   harvest_date from", df_h['harvest_date'].min(),
#       "to",             df_h['harvest_date'].max(),
#       "(", len(df_h),   "rows )")


# # ─── STEP 2: READ & CLEAN VIDEO METRICS ─────────────────────────────────────────
# df_v = pd.read_csv(VIDEO_FILE)
# # print(df_v.head())
# df_v = df_v.rename(columns={'Datum': 'date'})
# df_v['date'] = pd.to_datetime(df_v['date'], errors='coerce')
# print(df_v.head())
# df_v = df_v.dropna(subset=['date', 'row_id'])
# print(df_v.head())
# df_v['row_id'] = df_v['row_id'].astype(int)

# print("▶️ Video file:  ", VIDEO_FILE)
# print("   video date from", df_v['date'].min(),
#       "to",               df_v['date'].max(),
#       "(", len(df_v),     "rows )")


# # ─── STEP 3: MATCH EACH HARVEST DATE × PFAD TO THE CORRECT VIDEO ROW ───────────
# records = []
# for _, hrow in df_h.iterrows():
#     hd = hrow['harvest_date']
#     for col in ['Pfad 109', 'Pfad 090', 'Pfad 079']:
#         pid = int(col.split()[1])
#         # skip if no harvest for that Pfad
#         if pd.isna(hrow[col]):
#             continue

#         # same-day and all previous video rows for this pid
#         same = df_v[(df_v['date'] == hd) & (df_v['row_id'] == pid)]
#         prev = df_v[(df_v['date'] <  hd) & (df_v['row_id'] == pid)]

#         if not same.empty:
#             # pick the same-day row
#             sel = same.iloc[0]
#             if not prev.empty:
#                 # find the very last previous row
#                 prev_date = prev['date'].max()
#                 prev_row  = prev[prev['date'] == prev_date].iloc[0]
#                 # if red_fruit_count dropped, assume harvest happened first → use previous
#                 if sel['red_fruit_count'] < prev_row['red_fruit_count']:
#                     sel = prev_row
#         else:
#             # no same-day → fall back to the latest before harvest
#             cand = df_v[(df_v['date'] <= hd) & (df_v['row_id'] == pid)]
#             if cand.empty:
#                 # no video at all for this pid → skip
#                 continue
#             pick_date = cand['date'].max()
#             sel = cand[cand['date'] == pick_date].iloc[0]

#         rec = sel.to_dict()
#         rec['row_harvest'] = hrow[col]
#         rec['harvest_date'] = hd
#         records.append(rec)

# df_new = pd.DataFrame.from_records(records) if records else pd.DataFrame(columns=list(df_v.columns) + ['harvest_date', 'row_harvest'])


# # ─── STEP 4: APPEND TO EXISTING OUTPUT + DEDUPE ────────────────────────────────
# if os.path.exists(OUTPUT_FILE):
#     df_old = pd.read_csv(OUTPUT_FILE)
#     df_all = pd.concat([df_old, df_new], ignore_index=True)
# else:
#     df_all = df_new

# df_all = df_all.drop_duplicates(subset=['harvest_date', 'row_id'])
# print(df_all.head())


# # ─── STEP 5: WRITE OUT THE FINAL CSV ──────────────────────────────────────────
# df_all.to_csv(OUTPUT_FILE, index=False)
# print(f"✅ Pipeline complete. {len(df_all)} total rows written to '{OUTPUT_FILE}'.")

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 2024-25
# ─── CONFIG ─────────────────────────────────────────────────────────────────────
HARVEST_FILE = r'KW49_kg je Reihe.xlsx'
VIDEO_FILE   = r'fruit_count_avg_2023_24.csv'
OUTPUT_FILE  = 'inference.csv'

# ─── STEP 1: READ & CLEAN HARVEST DATA WITH SPLIT HEADERS ───────────────────────
# Pfad columns are on Excel row 6, Datum on row 7 → pandas header=[5,6]
df_raw = pd.read_excel(
    HARVEST_FILE,
    sheet_name='Sheet',
    header=[5, 6]
)

# Flatten the MultiIndex columns:
#  - keep top-level names for 'Pfad ...'
#  - rename the bottom-level 'Datum' to 'harvest_date'
new_cols = []
for top, bot in df_raw.columns:
    if isinstance(top, str) and top.startswith('Pfad'):
        new_cols.append(top)
    elif bot == 'Datum':
        new_cols.append('harvest_date')
    else:
        # fallback (shouldn’t be used)
        new_cols.append(bot or top)
df_raw.columns = new_cols

# select only the columns we need
df_h = df_raw[['harvest_date', 'Pfad 109', 'Pfad 079', 'Pfad 090']].copy()
df_h = df_h.loc[:, ~df_h.columns.duplicated()]
df_h['harvest_date'] = pd.to_datetime(df_h['harvest_date'], errors='coerce', dayfirst=True)
df_h = df_h.dropna(subset=['harvest_date'])

print(df_h.head())

# # restrict to your season window
# start_dt = pd.to_datetime('2025-02-28')
# end_dt   = pd.to_datetime('2025-05-27')
# df_h = df_h[(df_h['harvest_date'] >= start_dt) & (df_h['harvest_date'] <= end_dt)]

print("Harvest file:", HARVEST_FILE)
print("harvest_date from", df_h['harvest_date'].min(),
      "to",             df_h['harvest_date'].max(),
      "(", len(df_h),   "rows )")
print(df_h.head())


# ─── STEP 2: READ & CLEAN VIDEO METRICS ─────────────────────────────────────────
df_v = pd.read_csv(VIDEO_FILE)
# print(df_v.head())
df_v = df_v.rename(columns={'Datum': 'date'})
df_v['date'] = pd.to_datetime(df_v['date'], errors='coerce', format='%Y/%m/%d')
# print(df_v.head())
df_v = df_v.dropna(subset=['date', 'row_id'])
# print(df_v.head())
df_v['row_id'] = df_v['row_id'].astype(int)

print("Video file:  ", VIDEO_FILE)
print("   video date from", df_v['date'].min(),
      "to",               df_v['date'].max(),
      "(", len(df_v),     "rows )")
print(df_v.head())


# ─── STEP 3: MATCH EACH HARVEST DATE × PFAD TO THE CORRECT VIDEO ROW ───────────
records = []
for _, hrow in df_h.iterrows():
    hd = hrow['harvest_date']
    for col in ['Pfad 109', 'Pfad 079', 'Pfad 090']:
        pid = int(col.split()[1])
        # skip if no harvest for that Pfad
        if pd.isna(hrow[col]):
            continue

        # same-day and all previous video rows for this pid
        same = df_v[(df_v['date'] == hd) & (df_v['row_id'] == pid)]
        prev = df_v[(df_v['date'] <  hd) & (df_v['row_id'] == pid)]

        if not same.empty:
            # pick the same-day row
            sel = same.iloc[0]
            if not prev.empty:
                # find the very last previous row
                prev_date = prev['date'].max()
                prev_row  = prev[prev['date'] == prev_date].iloc[0]
                # if red_fruit_count dropped, assume harvest happened first → use previous
                if sel['red_fruit_count'] < prev_row['red_fruit_count']:
                    sel = prev_row
        else:
            # no same-day → fall back to the latest before harvest
            cand = df_v[(df_v['date'] <= hd) & (df_v['row_id'] == pid)]
            if cand.empty:
                # no video at all for this pid → skip
                continue
            pick_date = cand['date'].max()
            sel = cand[cand['date'] == pick_date].iloc[0]

        rec = sel.to_dict()
        rec['row_harvest'] = hrow[col]
        rec['harvest_date'] = hd
        records.append(rec)

df_new = pd.DataFrame.from_records(records) if records else pd.DataFrame(columns=list(df_v.columns) + ['harvest_date', 'row_harvest'])
# print(df_new.head())


# # ─── STEP 4: APPEND TO EXISTING OUTPUT + DEDUPE ────────────────────────────────
# if os.path.exists(OUTPUT_FILE):
#     df_old = pd.read_csv(OUTPUT_FILE)
#     df_all = pd.concat([df_old, df_new], ignore_index=True)
# else:
#     df_all = df_new

df_all = df_new

df_all = df_all.drop_duplicates(subset=['harvest_date', 'row_id'])
# print(df_all.head())


# ─── STEP 5: WRITE OUT THE FINAL CSV ──────────────────────────────────────────
df_all.to_csv(OUTPUT_FILE, index=False, date_format='%d-%m-%Y')
print(f"Pipeline complete. {len(df_all)} total rows written to '{OUTPUT_FILE}'.")
