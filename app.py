import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import gspread
# from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import streamlit as st
import json

def get_fundamentals(ticker):
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}  # Avoid blocking by Finviz
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        st.warning(f"Failed to fetch data for {ticker}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find("table", class_="snapshot-table2")
    if not table:
        st.warning(f"Fundamentals table not found for {ticker}")
        return None
    
    data = {"Ticker": ticker}  # Ensure ticker is the first column
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        for i in range(0, len(cols), 2):
            key = cols[i].text.strip()
            value = cols[i+1].text.strip()
            data[key] = value
    
    return data

def scrape_finviz_fundamentals(tickers):
    results = []
    for ticker in tickers:
        st.write(f"Scraping {ticker}...")
        fundamentals = get_fundamentals(ticker)
        if fundamentals:
            results.append(fundamentals)
        time.sleep(2)  # Delay to prevent getting blocked
    
    return pd.DataFrame(results)

def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # service_account_info = json.loads(st.secrets["SERVICE_ACCOUNT_KEY"])
    # creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
    
    creds = service_account.Credentials.from_service_account_info(st.secrets["GCP_SERVICE_ACCOUNT"])
    return gspread.authorize(creds)

def get_tickers_from_google_sheet():
    client = get_google_client()
    sheet = client.open_by_url(st.secrets["GOOGLE_SHEET_URL"]).worksheet("Ticker list")
    tickers = sheet.col_values(1)[1:]  # Assuming the first row is a header
    return tickers

def write_to_google_sheet(dataframe):
    client = get_google_client()
    sheet = client.open_by_url(st.secrets["GOOGLE_SHEET_URL"]).worksheet("Fundamentals")
    sheet.clear()
    sheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

def main():
    st.title("Finviz Fundamentals Scraper")
    
    if st.button("Run Scraper"):
        st.write("Fetching tickers from Google Sheet...")
        tickers = get_tickers_from_google_sheet()
        
        if not tickers:
            st.error("No tickers found in Google Sheet.")
            return
        
        df = scrape_finviz_fundamentals(tickers)
        
        if not df.empty:
            st.write("Writing data to Google Sheet...")
            write_to_google_sheet(df)
            st.success("Fundamentals successfully updated!")
            st.dataframe(df)
        else:
            st.error("No data scraped.")

if __name__ == "__main__":
    main()
