#!/usr/bin/env python3

import query_funcs
from flask import Flask, request, render_template

app = Flask(__name__)

# Currently assuming the query will be available on the homepage
@app.route("/home", methods=["GET"])

def query_response():

    # set blank response until data is sent to app
    response = ""

    # get submitted data from request
    if request.args:
        # this will likely be a query
        # get fields and syntex from various query values (v1, v2, etc.)
        query_v1 = request.args.get("query_v1")
        query_v2 = request.args.get("query_v2")
        query_v3 = request.args.get("query_v3")

        # construct sql query
        sql = f"""
            select {query_v1}
            from {query_v2}
            where {query_v3}
            """
        
        # request connection
        connection, cursor = create_connection()

        # Set db
        command("USE My_IMDB_Movies")
        command("select database();")
        current_db = cursor.fetchone()
        print(f"Now working in: {current_db[0]}")

        # some function that runs query on db, returns df
        response_df = query(sql)

        # Some function to extract the relevant info from response_df
        c_type, microbes = placeholder_func(response_df)

        # create a text response
        response = f"""
            <div style = "border: 4px solid black;
            padding: 6px;">
            <h3>Query Results:</h3>
            <p><b>Cancer Type:</b> {c_type}</p>
            <p><b>Associated Microbes:</b> {microbes}</p>
            </div>
        """
    return render_template("results.html", results_html=response)
