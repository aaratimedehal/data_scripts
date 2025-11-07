

### now just filling in the latest month, better for flexibility

import snowflake.connector
import pandas as pd
import numpy as np
import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os
from cryptography.hazmat.primitives import serialization

# ---- Load environment variables ----
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SF_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", os.path.join(os.path.dirname(__file__), "rsa_key.p8"))
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")

def load_private_key(path):
    """Load private key for Snowflake authentication"""
    with open(path, "rb") as key_file:
        return serialization.load_pem_private_key(key_file.read(), password=None)

# ---- SQL Query ----
SQL_QUERY = """
    SELECT KPI_NAME, KPI_VALUE, MONTH
    FROM PRODUCTION.ANALYST.TRIAL_AUTOMATION_SHEET
"""

# ---- Connect to Snowflake ----
private_key = load_private_key(SF_PRIVATE_KEY_PATH)
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    account=SNOWFLAKE_ACCOUNT,
    private_key=private_key,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()
cursor.execute(SQL_QUERY)
columns = [col[0] for col in cursor.description]
data = cursor.fetchall()
df = pd.DataFrame(data, columns=columns)
cursor.close()
conn.close()

# ---- Add total_customers as sum of prod + cons ----
totals = (
    df[df["KPI_NAME"].isin(["total_consumption_customers", "total_production_customers"])]
    .groupby("MONTH")["KPI_VALUE"].sum()
    .reset_index()
)
totals["KPI_NAME"] = "total_customers"
df = pd.concat([df, totals], ignore_index=True)

# ---- KPI Mapping ----
KPI_MAPPING = {
    #customers
    "total_consumption_customers": "Customers in Delivery (SE Consumption)",
    "total_production_customers": "Customers in Delivery (SE Production)",
    "total_customers": "Customers in Delivery (SE Total)",
    "new_signed_post_conversion": "New Signed (Post Conversion Loss)",
    "new_signed_post_conversion_se_consumption": "Signed Customers (SE Consumption)",
    "new_signed_post_conversion_se_production": "Signed Customers (SE Production)",
    "new_signed_post_conversion_fi_consumption": "Signed Customers (FI Consumption)",
    "new_signed_post_conversion_fi_production": "Signed Customers (FI Production)",
    "hansen_total_customers": "Customers in Delivery (FI Total)",
    "hansen_consumption_customers": "Customers in Delivery (FI Consumption)",
    "hansen_production_customers": "Customers in Delivery (FI Production)",
    #"winback": "Winback",
    "churned": "Churned Customers",
    #mimer
    "fcrd_up_d1": "FCR-D UP D1",
    "fcrd_up_d2": "FCR-D UP D2",
    "fcrd_up_total": "FCR-D UP TOTALT",
    "fcrd_down_d1": "FCR-D DOWN D1",
    "fcrd_down_d2": "FCR-D DOWN D2",
    "fcrd_down_total": "FCR-D DOWN TOTALT",
    #devices
    "energy_production_number": "Solar",
    "battery_number": "BESS",
    "electric_vehicle_number": "EV",
    "heating_system_number": "HVAC",
    "charging_station_number": "Chargebox",
    "weather_station_number": "Weather Station",
    "realtime_monitor_number": "SaveEye",
    "bess_sold": "BESS Sold Cumulative",
    "bess_installed": "BESS Installed Cumulative (Customers)",
    "vpp_chargebox_added": "VPP Chargebox Added",
    "vpp_chargebox_added_customer": "VPP Chargebox Added (Customers)",
    "total_vpp_chargebox_customer": "Total VPP Chargebox (Customers)",
    "energy_production_cumulative_net_customer": "Energy Production",
    "battery_cumulative_net_customer": "Battery",
    "electric_vehicle_cumulative_net_customer": "Electric Vehicle",
    "heating_system_cumulative_net_customer": "Heating System",
    "charging_station_cumulative_net_customer": "Charging Station",
    "weather_station_cumulative_net_customer": "Home Devices",
    "realtime_monitor_cumulative_net_customer": "SaveEye Devices",

    #vpp_revenue
    "bess_gross_revenue_sek": "BESS Gross Revenue (SEK)",
    "bess_gross_revenue_eur": "BESS Gross Revenue (EUR)",
    "bess_net_revenue_sek": "BESS Net Revenue (SEK)",
    "bess_net_revenue_eur": "BESS Net Revenue (EUR)",
    "total_revenue_sek": "Chargebox Gross Revenue (SEK)",
    "total_revenue_eur": "Chargebox Gross Revenue (EUR)",
    #cb
    "total_bidded_size_mw": "Total Bid Size MWh",
    "total_bidded_se4": "Total Bid Size (SE3)",
    "total_bidded_se3": "Total Bid Size (SE4)",
    "total_bid_potential": "Total Bid Number Potential",
    "total_bids_placed": "Bids Placed",
    "total_bids_won": "Bids Won",
    "total_bids_lost": "Bids Lost",
    "max_bid_size_se3": "Maximum Bid Size (SE3)",
    "max_bid_size_se4": "Maximum Bid Size (SE4)",
    "total_min_available_power": "Total Biddable Size (MWh)",
    #bess
    "bess_total_bids": "Total Bids Placed",
    "bess_no_participation_bids": "Bids - No Participation",
    "bess_bids_won": "Bids - Won",
    "bess_bids_lost": "Bids - Lost",
    #"bess_bids_pending": "Bids - Pending",
    "bess_max_bid_size_SE2": "BESS Maximum Bid Size (SE2)",
    "bess_max_bid_size_SE3": "BESS Maximum Bid Size (SE3)",
    "bess_max_bid_size_SE4": "BESS Maximum Bid Size (SE4)",
    "bess_won_capacity_mwh": "Won Capacity MWh",
    "bess_bidded_capacity_mwh": "Bid Capacity MWh",
    #costs
    "xledger_costs_5911": "Digital marketing - Performance (5911)",
    "xledger_costs_5909": "Digital marketing - Affiliate (5909)",
    "xledger_costs_5908": "Digital marketing - Price comparision websites (5908)",
    "xledger_costs_5921": "Branding and PR (5921)",
    "xledger_costs_6090": "Other marketing - Partner (6090)",
    "xledger_costs_6551": "Sales & Marketing Consulting (6551)",
    "xledger_cost_discounts": "Discounts",
    #app opens
    "app_opens_per_customer": "Customers without a device (Avg.)",
    "app_opens_per_customer_with_device": "Customer with a device (Avg.)",
    #intercom
    "customer_satisfaction_total_score": "Customer satisfaction - Total score",
    "customer_satisfaction_ai_teammate": "Customer satisfaction - AI teammate",
    "customer_satisfaction_physical_teammate": "Customer satisfaction - Physical teammate"

}

# Map KPI_NAME to display name
df["KPI_DISPLAY"] = df["KPI_NAME"].map(KPI_MAPPING)

# Convert MONTH to datetime for proper comparison
df["MONTH"] = pd.to_datetime(df["MONTH"])

# Get the latest month (true max, not alphabetical)
latest_month = df["MONTH"].max()

# Filter to latest month only
df = df[df["MONTH"] == latest_month]

# Convert MONTH back to string for matching sheet headers
df["MONTH"] = df["MONTH"].dt.strftime("%B %Y")
latest_month_str = df["MONTH"].iloc[0]

# ---- Connect to Google Sheets ----
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
if not GOOGLE_CREDENTIALS_PATH:
    raise ValueError("GOOGLE_CREDENTIALS_PATH env var is required for Google Sheets access")
creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=SCOPES)
client = gspread.authorize(creds)

spreadsheet = client.open("KPI NEW AUTOMATED")   #update
sheet = spreadsheet.worksheet("KPI")    #update

# ---- Map KPI -> value for this latest month ----
latest_map = dict(zip(df["KPI_DISPLAY"], df["KPI_VALUE"]))

# Get sheet headers (row 1, months)
headers = sheet.row_values(1)

# Ensure latest month string is present
if latest_month_str not in headers:
    raise ValueError(f"Latest month {latest_month_str} not found in sheet headers")

# Column index for latest month
month_col = headers.index(latest_month_str) + 1  # gspread is 1-based

# Get all KPI names (col A) and Types (col B)
sheet_kpis = sheet.col_values(1)
sheet_types = sheet.col_values(2)

# Only update the specific cells for rows marked Automated.
# Do not write blanks anywhere to avoid wiping formulas/values in Manual/Fixed/Calculated rows.
updated_count = 0
skipped_count = 0

from gspread.exceptions import APIError
import time

BATCH_SIZE = 200  # safe chunk size to avoid 429
pending_updates = []  # list of {"range": A1, "values": [[value]]}

for row_index, (kpi, kpi_type) in enumerate(zip(sheet_kpis[1:], sheet_types[1:]), start=2):  # rows start at 2
    if kpi_type == "Automated" and kpi in latest_map:
        value = latest_map[kpi]
        # Skip NaN/Inf/None to avoid Google Sheets JSON errors
        if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
            skipped_count += 1
            print(f"⏭️  Skipped row {row_index} → {kpi} (NaN/Inf)")
            continue
        a1 = rowcol_to_a1(row_index, month_col)
        pending_updates.append({"range": a1, "values": [[value]]})
        updated_count += 1
        print(f"✅ Queued row {row_index} → {kpi}: {value}")
    else:
        # Preserve any non-Automated types (e.g., Calculated, Manual, Fixed)
        skipped_count += 1
        print(f"⏭️  Skipped row {row_index} → {kpi} (Type: {kpi_type})")

# Flush in batches
for i in range(0, len(pending_updates), BATCH_SIZE):
    chunk = pending_updates[i:i + BATCH_SIZE]
    # batch_update accepts a list of request bodies; each with single-cell range and values
    try:
        sheet.batch_update(chunk, value_input_option="RAW")
    except APIError as e:
        if "429" in str(e):
            time.sleep(2)
            sheet.batch_update(chunk, value_input_option="RAW")
        else:
            raise

print(f"\n Summary:")
print(f"   • Updated: {updated_count} Automated fields")
print(f"   • Preserved (untouched): {skipped_count} non-Automated fields")

print(f"\n Latest month ({latest_month_str}) values written only for Automated rows in column {latest_month_str}.")
print(f" Calculated/Manual/Fixed fields preserved - no cells overwritten.")

