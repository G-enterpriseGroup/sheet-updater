import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
import yfinance as yf
import pandas as pd

# ── AUTH ────────────────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=SCOPES
)
gc = gspread.authorize(creds)

# ── OPEN SHEET ─────────────────────────────────────────────────────────────────
SHEET_URL = "https://docs.google.com/spreadsheets/d/1uXUn3Tl9Kd3K3gRQuiJhuaVVz5dssGmqaAcIbYD5Zrw/edit"
ss = gc.open_by_url(SHEET_URL)

# ── STAMP TIMESTAMP ─────────────────────────────────────────────────────────────
tickers_ws = ss.worksheet("Tickers")
now = datetime.now(ZoneInfo("America/New_York"))
tickers_ws.update(range_name="I1", values=[[now.strftime("%m.%d.%y %H:%M")]])

# ── LOAD TICKERS ─────────────────────────────────────────────────────────────────
all_vals     = tickers_ws.col_values(1)
tickers      = [t.strip() for t in all_vals[1:] if t.strip()]  # drop header row
existing     = {ws.title for ws in ss.worksheets()}
new_tickers  = [t for t in tickers if t not in existing]
if not new_tickers:
    print("✅ No new tickers to process."); exit()

# ── HELPERS ─────────────────────────────────────────────────────────────────────
def calculate_max_loss(price, df, exp):
    num = 100
    days = (datetime.strptime(exp, "%Y-%m-%d") - datetime.today()).days
    df = df.copy()
    df["Expiration Date"]       = exp
    df["Days Until Expiration"] = days
    df["Cost of Put (Ask)"]     = df["ask"]   * num
    df["Max Loss (Ask)"]        = df["strike"]*num - (price*num + df["Cost of Put (Ask)"])
    df["Cost of Put (Last)"]    = df["lastPrice"] * num
    df["Max Loss (Last)"]       = df["strike"]*num - (price*num + df["Cost of Put (Last)"])
    return df

summary_rows = []

# ── PROCESS EACH NEW TICKER ────────────────────────────────────────────────────
for tkr in new_tickers:
    tk = yf.Ticker(tkr)
    hist = tk.history(period="1d")["Close"]
    if hist.empty:
        print(f"⚠️ No price for {tkr}, skipping."); continue
    price = hist.iloc[-1]

    parts = []
    for exp in tk.options:
        puts = tk.option_chain(exp).puts[[
            "contractSymbol","strike","lastPrice","bid","ask",
            "volume","openInterest","impliedVolatility"
        ]]
        parts.append(calculate_max_loss(price, puts, exp))
    if not parts:
        print(f"⚠️ {tkr} has no options, skipping."); continue

    df = pd.concat(parts, ignore_index=True)
    df["Expiration Date"] = pd.to_datetime(df["Expiration Date"])
    df = df.sort_values(["Expiration Date","Max Loss (Ask)"]).reset_index(drop=True)
    num_cols = df.select_dtypes("number").columns
    df[num_cols] = df[num_cols].round(2)

    # remove old sheet
    try: ss.del_worksheet(ss.worksheet(tkr))
    except: pass

    # write new sheet
    ws = ss.add_worksheet(title=tkr, rows=str(len(df)+5), cols=str(len(df.columns)))
    set_with_dataframe(ws, df)

    # re-fetch sheetId
    sid = next(s["properties"]["sheetId"]
               for s in ss.fetch_sheet_metadata()["sheets"]
               if s["properties"]["title"] == tkr)

    hdr = df.columns.tolist()
    reqs = []

    # hide D,F,G,H,K,M → idx 3,5,6,7,10,12
    for i in (3,5,6,7,10,12):
        reqs.append({
            "updateDimensionProperties": {
                "range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":i,"endIndex":i+1},
                "properties":{"hiddenByUser":True},
                "fields":"hiddenByUser"
            }
        })

    # highlight Max Loss columns yellow
    for col in ("Max Loss (Ask)","Max Loss (Last)"):
        c = hdr.index(col)
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":sid,"startRowIndex":1,"endRowIndex":len(df)+1,
                         "startColumnIndex":c,"endColumnIndex":c+1},
                "cell":{"userEnteredFormat":{"backgroundColor":{"red":1,"green":1,"blue":0.6}}},
                "fields":"userEnteredFormat.backgroundColor"
            }
        })

    # blue-fill best (max Last) row per expiration & collect summary row
    for _, sub in df.groupby("Expiration Date"):
        best = int(sub["Max Loss (Last)"].idxmax())
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":sid,"startRowIndex":best+1,"endRowIndex":best+2,
                         "startColumnIndex":0,"endColumnIndex":len(hdr)},
                "cell":{"userEnteredFormat":{"backgroundColor":{"red":0.7,"green":0.9,"blue":1}}},
                "fields":"userEnteredFormat.backgroundColor"
            }
        })
        r = df.loc[best]
        summary_rows.append({
            "Ticker":               tkr,
            "contractSymbol":       r["contractSymbol"],
            "strike":               r["strike"],
            "Expiration Date":      r["Expiration Date"].date(),
            "Days Until Expiration":int(r["Days Until Expiration"]),
            "Max Loss (Ask)":       float(r["Max Loss (Ask)"]),
            "Max Loss (Last)":      float(r["Max Loss (Last)"])
        })

    # bold header row
    reqs.append({
        "repeatCell":{
            "range":{"sheetId":sid,"startRowIndex":0,"endRowIndex":1,
                     "startColumnIndex":0,"endColumnIndex":len(hdr)},
            "cell":{"userEnteredFormat":{
                "backgroundColor":{"red":0.95,"green":0.95,"blue":0.95},
                "textFormat":{"bold":True}
            }},
            "fields":"userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })

    ss.batch_update({"requests": reqs})

# ── BUILD SUMMARY SHEET ─────────────────────────────────────────────────────────
if summary_rows:
    sum_df = pd.DataFrame(summary_rows)[[
        "Ticker","contractSymbol","strike",
        "Expiration Date","Days Until Expiration",
        "Max Loss (Ask)","Max Loss (Last)"
    ]]

    # remove old Summary
    try: ss.del_worksheet(ss.worksheet("Summary"))
    except: pass

    ws2 = ss.add_worksheet(
        title="Summary",
        rows=str(len(sum_df)+5),
        cols=str(len(sum_df.columns))
    )
    set_with_dataframe(ws2, sum_df)
    ws2.freeze(rows=1)

    sid2 = next(s["properties"]["sheetId"]
                for s in ss.fetch_sheet_metadata()["sheets"]
                if s["properties"]["title"] == "Summary")

    # header bold
    req2 = [{
        "repeatCell":{
            "range":{"sheetId":sid2,"startRowIndex":0,"endRowIndex":1,
                     "startColumnIndex":0,"endColumnIndex":len(sum_df.columns)},
            "cell":{"userEnteredFormat":{
                "backgroundColor":{"red":0.95,"green":0.95,"blue":0.95},
                "textFormat":{"bold":True}
            }},
            "fields":"userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    }]

    # alternate colors by Ticker
    palette = [
        {"red":0.9,"green":0.9,"blue":0.7},
        {"red":0.9,"green":0.7,"blue":0.9},
        {"red":0.7,"green":0.9,"blue":0.9},
        {"red":0.9,"green":0.8,"blue":0.7},
        {"red":0.8,"green":0.9,"blue":0.7},
        {"red":0.7,"green":0.8,"blue":0.9}
    ]
    groups = sum_df.groupby("Ticker").groups
    for i, (_, idxs) in enumerate(groups.items()):
        color = palette[i % len(palette)]
        for ridx in idxs:
            req2.append({
                "repeatCell":{
                    "range":{"sheetId":sid2,"startRowIndex":ridx+1,"endRowIndex":ridx+2,
                             "startColumnIndex":0,"endColumnIndex":len(sum_df.columns)},
                    "cell":{"userEnteredFormat":{"backgroundColor":color}},
                    "fields":"userEnteredFormat.backgroundColor"
                }
            })

    ss.batch_update({"requests": req2})

print("✅ All sheets—including Summary—updated.")
