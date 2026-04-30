#!/usr/bin/env python3
"""
Cancer Microbiome Database (CMD) — Flask application
BF768 Spring 2026 | Cam Nowack, Jack Sherry, Simon Lu
"""

from flask import Flask, request, render_template, jsonify
import mariadb

app = Flask(__name__)


# ===========================================================================
# SECTION 1: PAGE ROUTES
# ===========================================================================

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


# ===========================================================================
# SECTION 2: AJAX / JSON ENDPOINTS
# ===========================================================================

@app.route("/api/sankey_data", methods=["GET"])
def api_sankey_data():
    """Returns Sankey-format data: [[source, target, value], ...]."""
    # TODO: query DB, format for Google Charts Sankey
    return jsonify([])


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


# ===========================================================================
# SECTION 3: DATABASE HELPER
# ===========================================================================

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


# ===========================================================================
# SECTION 4: APP STARTUP
# ===========================================================================

if __name__ == "__main__":
    app.run(debug=True, port=8073)