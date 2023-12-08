# app.py

from flask import Flask, render_template
import pandas as pd
import sqlite3
import requests 
from bs4 import BeautifulSoup
import requests 

app = Flask(__name__, template_folder='templates')


def scrape_website(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': 'individualResults'})
        headers = [header.text.strip() for header in table.find_all('th')]
        data = []
        for row in table.find_all('tr')[1:]:
            row_data = [cell.text.strip() for cell in row.find_all('td')]
            data.append(row_data)
        df = pd.DataFrame(data, columns=headers)
        df.columns = df.columns.str.lower()
        df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
        return df
    else:
        print(f"Failed to retrieve the page. Status Code: {response.status_code}")
        return None


def process_data(df):
    df['place'] = pd.to_numeric(df['place'], errors='coerce').astype('Int64')
    df['bib'] = pd.to_numeric(df['bib'], errors='coerce').astype('Int64')
    # df['time_seconds'] = pd.to_datetime(df['time'], format='%M:%S', errors='coerce').dt.minute * 60 + pd.to_datetime(df['time'], format='%M:%S', errors='coerce').dt.second
    # df['gun_time_seconds'] = pd.to_datetime(df['gun time'], format='%M:%S', errors='coerce').dt.minute * 60 + pd.to_datetime(df['gun time'], format='%M:%S', errors='coerce').dt.second
    if all(df.isnull().sum() / df.shape[0] < 5):
        df_cleaned = df.dropna()
        print("Rows with missing values dropped.")
    else:
        df_cleaned = df.fillna(df.mean())
        print("Missing values filled with mean.")
    return df_cleaned

def create_database(df, database_name='race_results_database.db', table_name='race_results_table'):
    conn = sqlite3.connect(database_name)
    df.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.commit()
    conn.close()
    print("Data successfully stored in SQLite database.")


def query_database(query):
    conn = sqlite3.connect('race_results_database.db')
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result


@app.route('/data')
def data():
    print("h")
    # Define the URL to scrape
    url_to_scrape = "https://www.hubertiming.com/results/2017GPTR10K"

    # Web scraping
    df_raw = scrape_website(url_to_scrape)

    # Data processing
    df_processed = process_data(df_raw)

    # Database creation
    create_database(df_processed)

    # Query database for display
    query = "SELECT * FROM race_results_table"
    df_display = query_database(query)

    return render_template('data.html', data=df_display)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

if __name__ == '__main__':
    app.run(debug=True)
