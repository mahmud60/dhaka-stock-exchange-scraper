# dags/dse_utils.py

import requests
from bs4 import BeautifulSoup
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import logging

URL = 'https://www.dse.com.bd/latest_share_price_scroll_l.php'
BASE_URL = "https://www.dse.com.bd/displayCompany.php?name="


def fetch_webpage():
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

def extract_share_data():
    soup = fetch_webpage()
    table = soup.find('table', {'class': 'table table-bordered background-white shares-table fixedHeader'})
    rows = []
    for tr in table.find_all('tr'):
        cols = tr.find_all('td')
        if cols:
            row_data = [td.text.strip() for td in cols]
            rows.append(row_data)
    return rows

def company_list():
    hook = PostgresHook(postgres_conn_id='dse_connection')
    sql = "SELECT * FROM company_list"
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    companies = cursor.fetchall()
    cursor.close()
    conn.close()
    return companies

def create_company_table():
    hook = PostgresHook(postgres_conn_id='dse_connection')
    sql = """
        CREATE TABLE IF NOT EXISTS company_list (
            company_id SERIAL PRIMARY KEY,
            company_trade_name VARCHAR(255) NOT NULL,
            company_url VARCHAR(255) NOT NULL,
            UNIQUE(company_trade_name)
        );
    """
    hook.run(sql)

def insert_company_data():
    rows = extract_share_data()
    hook = PostgresHook(postgres_conn_id='dse_connection')
    sql = """
        INSERT INTO company_list (company_trade_name, company_url) 
        VALUES (%s, %s)
        ON CONFLICT (company_trade_name) DO NOTHING;
    """
    for data in rows:
        try:
            hook.run(sql, parameters=(data[1], BASE_URL + data[1]))
        except Exception as e:
            logging.error(f"Insert error: {e}")


def create_share_prices_table():
    """Create the company_share_prices table if it doesn't exist."""
    hook = PostgresHook(postgres_conn_id='dse_connection')
    create_table_query = """
        CREATE TABLE IF NOT EXISTS company_share_prices (
            id SERIAL,
            company_id INTEGER NOT NULL,
            date DATE,
            latest_trading_price VARCHAR(50),
            low_price VARCHAR(50),
            high_price VARCHAR(50),
            closing_price VARCHAR(50),
            ycp VARCHAR(50),
            change VARCHAR(10),
            trade VARCHAR(50),
            value VARCHAR(50),
            volume VARCHAR(50),
            PRIMARY KEY(company_id, id),
            FOREIGN KEY(company_id)
                REFERENCES company_list(company_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
    """
    hook.run(create_table_query)


def save_share_prices_data():
    """Save extracted share prices data to the PostgreSQL table."""
    hook = PostgresHook(postgres_conn_id='dse_connection')
    extracted_rows = extract_share_data()
    
    insert_query = """
        INSERT INTO company_share_prices (
            company_id, date, latest_trading_price, high_price, low_price, closing_price, ycp, change, trade, value, volume
        ) VALUES (
            (Select company_id FROM company_list WHERE company_trade_name LIKE %s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
    
    for row in extracted_rows:
        try:
            hook.run(insert_query, parameters=(
                row[1],datetime.now(), row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]
            ))
        except Exception as error:
            logging.error(f"Error inserting share prices data: {error}")

def create_company_basic_table():
    hook = PostgresHook(postgres_conn_id='dse_connection')
    create_table_query = """
        CREATE TABLE IF NOT EXISTS company_basic_info (
            id SERIAL,
            company_id INTEGER NOT NULL,
            date DATE,
            authorized_capital VARCHAR(50),
            paid_up_capital VARCHAR(50),
            type_of_instrument VARCHAR(50),
            face_per_value VARCHAR(10),
            market_lot VARCHAR(50),
            total_no_outstanding_securities VARCHAR(50),
            sector VARCHAR(100),
            PRIMARY KEY (company_id,id),
            UNIQUE(company_id),
            FOREIGN KEY (company_id)
                REFERENCES company_list (company_id)
                ON UPDATE CASCADE ON DELETE CASCADE
            )
    """
    hook.run(create_table_query)

def extract_and_save_company_basic_data():
    companies = company_list()  # Use the company_list function to get the list of companies
    for company in companies:
            response = requests.get(company[2])
            soup = BeautifulSoup(response.content, 'html.parser')
            tables = soup.find_all('table', {'class': 'table table-bordered background-white'})
            if not tables:
                print("No data tables found on the page.")
                exit()
            # Extract basic info (shortened for brevity)
            basic_info_data = []
            for tr in tables[1].find_all('tr'):  # Skip the header row
                if len(basic_info_data) < 9:
                    cols = tr.find_all('td')
                    row_data = [td.text.strip() for td in cols]
                    if row_data:
                        for row in row_data:
                            if row == '-':
                                row = 0
                            basic_info_data.append(row)
            hook = PostgresHook(postgres_conn_id='dse_connection')
            insert_basic_info_query = """
                INSERT INTO company_basic_info (date, authorized_capital, paid_up_capital, type_of_instrument, face_per_value, market_lot,
                                                total_no_outstanding_securities, sector, company_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            hook.run(insert_basic_info_query, parameters=(datetime.now(), basic_info_data[0], basic_info_data[2], basic_info_data[3], basic_info_data[4],
                            basic_info_data[5], basic_info_data[6], basic_info_data[7], company[0]))  # Add basic info parameters

def create_company_market_data_table():
    hook = PostgresHook(postgres_conn_id='dse_connection')
    create_table_query = """
        CREATE TABLE IF NOT EXISTS company_market_info (
            id SERIAL,
            company_id INTEGER NOT NULL,
            date DATE,
            last_trading_price VARCHAR(50),
            closing_price VARCHAR(50),
            last_update VARCHAR(20),
            day_range VARCHAR(20),
            change VARCHAR(20),
            change_percentage VARCHAR(20),
            day_value VARCHAR(20),
            weeks_moving_range VARCHAR(20),
            opening_price VARCHAR(20),
            day_volume VARCHAR(50),
            adjusted_opening_price VARCHAR(50),
            day_trade VARCHAR(20),
            yesterday_closing_price VARCHAR(20),
            market_capitalization VARCHAR(50),
            PRIMARY KEY(company_id, id),
            FOREIGN KEY (company_id)
                REFERENCES company_list(company_id)
                ON UPDATE CASCADE ON DELETE CASCADE
            )
        """
    hook.run(create_table_query)

def extract_and_save_company_market_info():
    companies = company_list()  # Use the company_list function to get the list of companies
    for company in companies:
        response = requests.get(company[2])
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table', {'class': 'table table-bordered background-white'})
        if not tables:
            print("No data tables found on the page.")
            exit()
        # Extract market info (shortened for brevity)
        market_data = [...]  # Parsed data from webpage
        for tr in tables[0].find_all('tr'):  # Skip the header row
            cols = tr.find_all('td')
            row_data = [td.text.strip() for td in cols]
            if row_data:
                for row in row_data:
                    market_data.append(row)
            print(market_data)
        hook = PostgresHook(postgres_conn_id='dse_connection')
        insert_market_info_query = """
            INSERT INTO company_market_info (date, last_trading_price, closing_price, last_update, day_range, change,
                                             change_percentage, day_value, weeks_moving_range, opening_price, day_volume,
                                             adjusted_opening_price, day_trade, yesterday_closing_price, market_capitalization, company_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        hook.run(insert_market_info_query, parameters=(datetime.now(), market_data[1], market_data[2], market_data[3], market_data[4],market_data[5], market_data[6],
                            market_data[7], market_data[8], market_data[9], market_data[10],market_data[11], market_data[12], market_data[13], market_data[14], company[0]))  # Add market info parameters