#!/usr/bin/env python3
"""
Cancer Microbiome Database (CMD) — Flask application
BF768 Spring 2026 | Cam Nowack, Jack Sherry, Simon Lu
"""

from flask import Flask, request, render_template, jsonify, abort
import mariadb
import os
from dotenv import load_dotenv

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/analysis/shannon", methods=["GET"])
def shannon():
    return render_template("shannon.html")

@app.route("/analysis/sankey", methods=["GET"])
def sankey():
    return render_template("sankey.html")

@app.route("/analysis/metagenomic", methods=["GET"])
def metagenomic():
    return render_template("metagenomic.html")

@app.route("/analysis/pca", methods=["GET"])
def pca():
    return render_template("pca.html")

@app.route("/help/usage", methods=["GET"])
def usage_guide():
    return render_template("usage_guide.html")

@app.route("/help/examples", methods=["GET"])
def example_queries():
    return render_template("example_queries.html")

@app.route("/help/license", methods=["GET"])
def license_page():
    return render_template("license.html")


#===============================================================#
#       Cam - Sankey Plots                                      #
#===============================================================#

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
SQL_DIR  = os.path.join(APP_ROOT, "..", "db")

def load_sql(filename):
    """
    Loads sql file from SQL_DIR
    """
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r") as f:
        return f.read()

@app.route("/api/sankey_data", methods=["GET"])
def api_sankey_data():
    load_dotenv()
    db_usr = os.environ.get("DB_USR")
    db_pw  = os.environ.get("DB_PW")

    # Map filter_type -> (sql filename, param value)
    filter_type = request.args.get("filter_type")
    filter_value = request.args.get("filter_value")

    sql_file_map = {
        "sid":         "sankey_sid.sql",
        "cancer_type": "sankey_cancer.sql",
        "treatment":   "sankey_treatment.sql",
        "sample_type": "sankey_sample.sql"
    }

    if filter_type not in sql_file_map:
        return jsonify({"error": "Invalid filter_type"}), 400
    if not filter_value:
        return jsonify({"error": "filter_value is required"}), 400

    query = load_sql(sql_file_map[filter_type])
    # 4 taxonomic levels -> repeat param 4 times
    stage_count_map = {
    "sid":         4,   # phylum -> class -> order -> family -> genus
    "cancer_type": 3,   # phylum -> class -> order -> family
    "treatment":   3,
    "sample_type": 3
    }
    params = (filter_value,) * stage_count_map[filter_type]

    connection, cursor = None, None
    import mysql.connector
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user=db_usr,
            password=db_pw,
            database="Team13",
            port=3307,
            use_pure=True,
        )
        cursor = connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = [[src, tgt, int(val)] for src, tgt, val in rows]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:     cursor.close()
        if connection: connection.close()

@app.route("/api/filter_options", methods=["GET"])
def api_filter_options():
    load_dotenv()
    db_usr = os.environ.get("DB_USR")
    db_pw  = os.environ.get("DB_PW")

    query = load_sql("sankey_dropdown_values.sql")

    connection, cursor = None, None
    import mysql.connector
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1", 
            user=db_usr, 
            password=db_pw,
            database="Team13", 
            port=3307, 
            use_pure=True,
        )
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        options = {}
        for filter_name, value in rows:
            options.setdefault(filter_name, []).append(value)
        return jsonify(options)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:     cursor.close()
        if connection: connection.close()


#===============================================================#
#       Jack - Shannon Diveristy plots                          #
#===============================================================#

@app.route("/api/cancer_diversity_search", methods=["GET"])
def api_cancer_diversity_search():
    """Returns per-patient diversity scores filtered by cancer type and/or sample type.
    Params:
        cancer_type  -- partial match against Patient.cancer_type (empty = all)
        sample_type  -- exact match against Sample.sample_type: 'buccal','stool','nasal' (empty = all)
        index_type   -- 'shannon' (default) or 'simpson'
    """
    cancer_type = request.args.get("cancer_type", "").strip()
    sample_type = request.args.get("sample_type", "").strip()

    shannon_expr = "ROUND(-SUM(o.relative_abundance * LOG(o.relative_abundance)), 4)"
    simpson_expr = "ROUND(1 - SUM(o.relative_abundance * o.relative_abundance), 4)"

    # Build query dynamically based on which filters are active
    query = (
        f"SELECT p.patient_id, p.cancer_type, p.cancer_category, p.response_status, "
        f"{shannon_expr} AS shannon_score, "
        f"{simpson_expr} AS simpson_score, "
        "s.sample_type "
        "FROM Patient p "
        "JOIN Sample s ON p.patient_id = s.patient_id "
        "JOIN Observation o ON s.sid = o.sid "
        "WHERE o.relative_abundance > 0 "
    )
    params = []

    if cancer_type:
        query += "AND p.cancer_type LIKE ? "
        params.append(f"%{cancer_type}%")

    if sample_type:
        query += "AND s.sample_type = ? "
        params.append(sample_type)

    query += "GROUP BY p.patient_id, p.cancer_type, p.cancer_category, p.response_status, s.sample_type"

    conn, cursor = get_db_connection()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([[row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in rows])


def get_db_connection():
    """Returns a (connection, cursor) pair. Caller is responsible for closing both."""
    connection = mariadb.connect(
        host="bioed-new.bu.edu",
        user="jgsherry",
        password="jgsherry",
        db="Team13",
        port=4253,
    )
    connection.autocommit = False
    cursor = connection.cursor()
    return connection, cursor


if __name__ == "__main__":
    app.run(debug=True, port=8073)