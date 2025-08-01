# Authenticate
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
import yfinance as yf
import pandas as pd

# Define required OAuth scopes for Sheets and Drive access
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load service account credentials with defined scopes
creds = Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=SCOPES
)
# Authorize gspread client
gc = gspread.authorize(creds)

# Open spreadsheet
sheet_url = (
    "https://docs.google.com/spreadsheets/"
    "d/1uXUn3Tl9Kd3K3gRQuiJhuaVVz5dssGmqaAcIbYD5Zrw/edit"
)
spreadsheet = gc.open_by_url(sheet_url)

# Stamp update time in cell D1 of the Tickers sheet (MM.DD.YY HH:MM Eastern)
tickers_ws = spreadsheet.worksheet("Tickers")
eastern = ZoneInfo("America/New_York")
now = datetime.now(eastern)
timestamp = now.strftime("%m.%d.%y %H:%M")
tickers_ws.update("D1", [[timestamp]])

# Read tickers and identify new ones
tickers = [t.strip() for t in tickers_ws.col_values(1) if t.strip()]
existing_titles = [ws.title for ws in spreadsheet.worksheets()]
new_tickers = [t for t in tickers if t not in existing_titles]

if not new_tickers:
    print("✅ No new tickers to process.")
    exit()

# Prepare summary container
summary_rows = []

# Helper function for calculating losses
def calculate_max_loss(price, df, exp_date):
    num = 100
    days = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.today()).days
    df = df.copy()
    df["Expiration Date"] = exp_date
    df["Days Until Expiration"] = days
    df["Cost of Put (Ask)"] = df["ask"] * num
    df["Max Loss (Ask)"] = (df["strike"] * num) - (price * num + df["Cost of Put (Ask)"])
    df["Cost of Put (Last)"] = df["lastPrice"] * num
    df["Max Loss (Last)"] = (df["strike"] * num) - (price * num + df["Cost of Put (Last)"])
    return df

# Process each new ticker
for ticker in new_tickers:
    # Fetch data
    tk = yf.Ticker(ticker)
    price = tk.history(period="1d")["Close"].iloc[-1]
    parts = []
    for exp in tk.options:
        puts = tk.option_chain(exp).puts[[
            "contractSymbol", "strike", "lastPrice", "bid", "ask",
            "volume", "openInterest", "impliedVolatility"
        ]]
        parts.append(calculate_max_loss(price, puts, exp))
    df = pd.concat(parts, ignore_index=True)
    df["Expiration Date"] = pd.to_datetime(df["Expiration Date"])
    df = df.sort_values(["Expiration Date", "Max Loss (Ask)"]).reset_index(drop=True)
    df[df.select_dtypes(include="number").columns] = df.select_dtypes(include="number").round(2)

    # Delete old sheet if exists, then add new one
    try:
        ws_old = spreadsheet.worksheet(ticker)
        spreadsheet.del_worksheet(ws_old)
    except Exception:
        pass
    ws = spreadsheet.add_worksheet(
        title=ticker,
        rows=str(len(df) + 5),
        cols=str(len(df.columns))
    )
    set_with_dataframe(ws, df)

    # Prepare formatting requests
    meta = spreadsheet.fetch_sheet_metadata()["sheets"]
    sid = next(
        s["properties"]["sheetId"]
        for s in meta if s["properties"]["title"] == ticker
    )
    requests = []
    hdr = df.columns.tolist()

    # Hide columns D, F, G, H, K, M
    for i in [3, 5, 6, 7, 10, 12]:
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser"
            }
        })

    # Highlight Max Loss columns
    for col_name in ("Max Loss (Ask)", "Max Loss (Last)"):
        idx = hdr.index(col_name)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sid,
                    "startRowIndex": 1,
                    "endRowIndex": len(df) + 1,
                    "startColumnIndex": idx,
                    "endColumnIndex": idx + 1
                },
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 0.6}}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    # Blue-fill best rows and collect summary
    rows_by_exp = {
        exp: int(sub["Max Loss (Last)"].idxmax())
        for exp, sub in df.groupby("Expiration Date")
    }
    for ridx in rows_by_exp.values():
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sid, "startRowIndex": ridx + 1, "endRowIndex": ridx + 2, "startColumnIndex": 0, "endColumnIndex": len(hdr)},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.7, "green": 0.9, "blue": 1}}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
        row = df.loc[ridx]
        summary_rows.append({
            "Ticker": ticker,
            "contractSymbol": row["contractSymbol"],
            "strike": row["strike"],
            "Expiration Date": row["Expiration Date"].date(),
            "Days Until Expiration": int(row["Days Until Expiration"]),
            "Max Loss (Ask)": float(row["Max Loss (Ask)"]),
            "Max Loss (Last)": float(row["Max Loss (Last)"])
        })

    # Header row formatting
    requests.append({
        "repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": len(hdr)},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })

    spreadsheet.batch_update({"requests": requests})

# Create or update Summary sheet
if summary_rows:
    sum_df = pd.DataFrame(summary_rows)
    try:
        summary_ws = spreadsheet.worksheet("Summary")
        spreadsheet.del_worksheet(summary_ws)
    except Exception:
        pass
    ws2 = spreadsheet.add_worksheet(
        title="Summary",
        rows=str(len(sum_df) + 5),
        cols=str(len(sum_df.columns))
    )
    set_with_dataframe(ws2, sum_df)
    ws2.freeze(rows=1)

    # Summary formatting
    meta2 = spreadsheet.fetch_sheet_metadata()["sheets"]
    sid2 = next(
        s["properties"]["sheetId"] for s in meta2 if s["properties"]["title"] == "Summary"
    )
    palette = [
        {"red": 0.9, "green": 0.9, "blue": 0.7},
        {"red": 0.9, "green": 0.7, "blue": 0.9},
        {"red": 0.7, "green": 0.9, "blue": 0.9},
        {"red": 0.9, "green": 0.8, "blue": 0.7},
        {"red": 0.8, "green": 0.9, "blue": 0.7},
        {"red": 0.7, "green": 0.8, "blue": 0.9}
    ]
    req2 = []
    # Header formatting for Summary
    req2.append({
        "repeatCell": {
            "range": {"sheetId": sid2, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": len(sum_df.columns)},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })
    hdr2 = sum_df.columns.tolist()
    ticker_groups = sum_df.groupby("Ticker").groups
    for i, (ticker, indices) in enumerate(ticker_groups.items()):
        for ridx in indices:
            req2.append({
                "repeatCell": {
                    "range": {"sheetId": sid2, "startRowIndex": ridx + 1, "endRowIndex": ridx + 2, "startColumnIndex": 0, "endColumnIndex": len(hdr2)},
                    "cell": {"userEnteredFormat": {"backgroundColor": palette[i % len(palette)]}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
    spreadsheet.batch_update({"requests": req2})

print("✅ All new ticker sheets and Summary updated with dynamic colors")
