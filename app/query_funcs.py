#!/usr/bin/env python3

import mysql.connector
import mariadb
import getpass
import pandas as pd


def create_connection():
    """
    Create a connection to Bioed-new using local ssh tunnel, should work for testing on our own computers but will need to be updated for use on the actual server.
    """
    pswd = getpass.getpass("Enter BU username: ")

    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='cnowack',
            password= pswd,
            database='cnowack',
            port=3307,
            use_pure=True
        )
        print("Success! You are connected.")

        cursor = connection.cursor()

    except Exception as e:
        print(f"Connection failed: {e}")

    return connection, cursor



def command(c):
    cursor.execute(c)

    # Check if the command actually returns rows (like SELECT or SHOW)
    if cursor.with_rows:
        result = cursor.fetchall()
        for row in result:
            print(row)
    else:
        # For commands like INSERT, UPDATE, or USE
        print(f"Affected rows: {cursor.rowcount}")
            
    cursor.fetchall()



def query(q):
    df = pd.read_sql_query(q, connection)
    return df

