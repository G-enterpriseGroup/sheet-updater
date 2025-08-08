import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
import yfinance as yf
import pandas as pd

# OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authenticate
creds = Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=SCOPES
)
gc = gspread.authorize(creds)

# Open spreadsheet
SHEET_URL = (
    "https://docs.google.com/spreadsheets/"
    "d/1uXUn3Tl9Kd3K3gRQuiJhuaVVz5dssGmqaAcIbYD5Zrw/edit"
)
ss = gc.open_by_url(SHEET_URL)

# 1️⃣ Stamp timestamp in Tickers!I1
tickers_ws = ss.worksheet("Tickers")
eastern = ZoneInfo("America/New_York")
now = datetime.now(eastern)
timestamp = now.strftime("%m.%d.%y %H:%M")
tickers_ws.update(range_name="I1", values=[[timestamp]])

# 2️⃣ Read tickers, skip header if any
all_vals = tickers_ws.col_values(1)
tickers = [t.strip() for t in all_vals[1:] if t.strip()]  # drop row-1 header
existing = {ws.title for ws in ss.worksheets()}
new_tickers = [t for t in tickers if t not in existing]

if not new_tickers:
    print("✅ No new tickers to process.")
    exit()

# Container for summary rows
summary_rows = []

def calculate_max_loss(price, df, exp_date):
    num = 100
    days = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.today()).days
    df = df.copy()
    df["Expiration Date"]      = exp_date
    df["Days Until Expiration"]= days
    df["Cost of Put (Ask)"]    = df["ask"] * num
    df["Max Loss (Ask)"]       = (df["strike"]*num) - (price*num + df["Cost of Put (Ask)"])
    df["Cost of Put (Last)"]   = df["lastPrice"] * num
    df["Max Loss (Last)"]      = (df["strike"]*num) - (price*num + df["Cost of Put (Last)"])
    return df

for ticker in new_tickers:
    tk = yf.Ticker(ticker)
    hist = tk.history(period="1d")["Close"]
    if hist.empty:
        print(f"⚠️  No price data for {ticker}, skipping.")
        continue
    price = hist.iloc[-1]

    # Pull option chains
    parts = []
    for exp in tk.options:
        puts = tk.option_chain(exp).puts[[
            "contractSymbol","strike","lastPrice","bid","ask",
            "volume","openInterest","impliedVolatility"
        ]]
        parts.append(calculate_max_loss(price, puts, exp))

    if not parts:
        print(f"⚠️  {ticker} has no listed options, skipping.")
        continue

    df = pd.concat(parts, ignore_index=True)
    df["Expiration Date"] = pd.to_datetime(df["Expiration Date"])
    df = df.sort_values(["Expiration Date","Max Loss (Ask)"]).reset_index(drop=True)
    # Round all numeric columns
    num_cols = df.select_dtypes(include="number").columns
    df[num_cols] = df[num_cols].round(2)

    # Remove old sheet & add new one
    try:
        old = ss.worksheet(ticker)
        ss.del_worksheet(old)
    except gspread.exceptions.WorksheetNotFound:
        pass

    ws = ss.add_worksheet(
        title=ticker,
        rows=str(len(df)+5),
        cols=str(len(df.columns))
    )
    set_with_dataframe(ws, df)

    # Re-fetch metadata for this new sheet
    meta = ss.fetch_sheet_metadata()["sheets"]
    sid = next(
        s["properties"]["sheetId"]
        for s in meta if s["properties"]["title"] == ticker
    )

    hdr = df.columns.tolist()
    requests = []

    # Hide unwanted columns (D, F, G, H, K, M → indices 3,5,6,7,10,12)
    for idx in (3,5,6,7,10,12):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sid,
                    "dimension": "COLUMNS",
                    "startIndex": idx,
                    "endIndex": idx+1
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser"
            }
        })

    # Highlight both Max Loss columns yellow
    for col in ("Max Loss (Ask)","Max Loss (Last)"):
        cidx = hdr.index(col)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sid,
                    "startRowIndex": 1,
                    "endRowIndex": len(df)+1,
                    "startColumnIndex": cidx,
                    "endColumnIndex": cidx+1
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red":1,"green":1,"blue":0.6}
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    # Blue-fill the best (max) row per expiration
    for exp, sub in df.groupby("Expiration Date"):
        best_r = int(sub["Max Loss (Last)"].idxmax())
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sid,
                    "startRowIndex": best_r+1,
                    "endRowIndex": best_r+2,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(hdr)
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor":{"red":0.7,"green":0.9,"blue":1}
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
        row = df.loc[best_r]
        summary_rows.append({
            "Ticker": ticker,
            "contractSymbol": row["contractSymbol"],
            "strike": row["strike"],
            "Expiration Date": row["Expiration Date"].date(),
            "Days Until Expiration": int(row["Days Until Expiration"]),
            "Max Loss (Ask)": float(row["Max Loss (Ask)"]),
            "Max Loss (Last)": float(row["Max Loss (Last)"])
        })

    # Bold header row
    requests.append({
        "repeatCell": {
            "range":{
                "sheetId": sid,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": len(hdr)
            },
            "cell":{"userEnteredFormat":{
                "backgroundColor":{"red":0.95,"green":0.95,"blue":0.95},
                "textFormat":{"bold":True}
            }},
            "fields":"userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })

    # Push all formatting in one batch
    ss.batch_update({"requests": requests})

# ─── Summary Sheet ─────────────────────────────────────────────────────────────
if summary_rows:
    sum_df = pd.DataFrame(summary_rows)
    # Replace old Summary sheet
    try:
        old = ss.worksheet("Summary")
        ss.del_worksheet(old)
    except gspread.exceptions.WorksheetNotFound:
        pass

    ws2 = ss.add_worksheet(
        title="Summary",
        rows=str(len(sum_df)+5),
        cols=str(len(sum_df.columns))
    )
    set_with_dataframe(ws2, sum_df)
    ws2.freeze(rows=1)

    # Re-fetch metadata for Summary
    meta2 = ss.fetch_sheet_metadata()["sheets"]
    sid2 = next(
        s["properties"]["sheetId"]
        for s in meta2 if s["properties"]["title"] == "Summary"
    )

    # Header bold
    req2 = [{
        "repeatCell": {
            "range":{
                "sheetId":sid2,
                "startRowIndex":0,
                "endRowIndex":1,
                "startColumnIndex":0,
                "endColumnIndex":len(sum_df.columns)
            },
            "cell":{"userEnteredFormat":{
                "backgroundColor":{"red":0.95,"green":0.95,"blue":0.95},
                "textFormat":{"bold":True}
            }},
            "fields":"userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    }]

    # Alternating row colors by ticker
    palette = [
        {"red":0.9,"green":0.9,"blue":0.7},
        {"red":0.9,"green":0.7,"blue":0.9},
        {"red":0.7,"green":0.9,"blue":0.9},
        {"red":0.9,"green":0.8,"blue":0.7},
        {"red":0.8,"green":0.9,"blue":0.7},
        {"red":0.7,"green":0.8,"blue":0.9}
    ]
    hdr2 = sum_df.columns.tolist()
    groups = sum_df.groupby("Ticker").groups
    for i, (_, idxs) in enumerate(groups.items()):
        color = palette[i % len(palette)]
        for ridx in idxs:
            req2.append({
                "repeatCell":{
                    "range":{
                        "sheetId":sid2,
                        "startRowIndex":ridx+1,
                        "endRowIndex":ridx+2,
                        "startColumnIndex":0,
                        "endColumnIndex":len(hdr2)
                    },
                    "cell":{"userEnteredFormat":{"backgroundColor":color}},
                    "fields":"userEnteredFormat.backgroundColor"
                }
            })

    ss.batch_update({"requests": req2})

print("✅ All new ticker sheets and Summary updated successfully.")
