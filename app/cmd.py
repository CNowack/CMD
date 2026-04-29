#!/usr/bin/env python3
"""
Cancer Microbiome Database (CMD) — Flask application
BF768 Spring 2026 | Cam Nowack, Jack Sherry, Simon Lu

This file is the main entry point for the web application. It defines all
URL routes ("endpoints") and tells Flask which HTML template to render for
each one. Database queries and chart-generating logic will be added later
in separate functions or modules — keep this file focused on *routing*.
"""

from flask import Flask, request, render_template, jsonify, abort
import mariadb
import os
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------------
# `Flask(__name__)` creates the application object. The __name__ argument
# tells Flask where to look for templates (the `templates/` folder) and
# static assets like CSS/JS (the `static/` folder), relative to this file.
app = Flask(__name__)


# ===========================================================================
# SECTION 1: PAGE ROUTES (HTML pages the user navigates to)
# ===========================================================================
# Each function below is a "view function" — Flask calls it when a user's
# browser requests the matching URL. The @app.route(...) line above each
# function is a "decorator" that registers the URL path.
#
# The function NAME (e.g. `home`) is what `url_for('home')` in the Jinja
# templates resolves to. If you rename a function, update base.html too.

@app.route("/", methods=["GET"])
def home():
    """
    Landing page. Satisfies Benson evalue #2: project name, student
    developers, faculty advisor, BU/BF768 attribution, and an abstract.
    Most of that lives in home.html and the shared footer in base.html.
    """
    return render_template("home.html")


# --- Analysis pages --------------------------------------------------------

@app.route("/analysis/shannon", methods=["GET"])
def shannon():
    """Shannon diversity index visualization page."""
    return render_template("shannon.html")


@app.route("/analysis/sankey", methods=["GET"])
def sankey():
    """Sankey diagram (Cancer Type -> Treatment -> Response)."""
    return render_template("sankey.html")


@app.route("/analysis/metagenomic", methods=["GET"])
def metagenomic():
    """Metagenomic / taxonomic abundance analysis page."""
    return render_template("metagenomic.html")


@app.route("/analysis/pca", methods=["GET"])
def pca():
    """Principal Component Analysis page."""
    return render_template("pca.html")


# --- Help pages ------------------------------------------------------------

@app.route("/help/usage", methods=["GET"])
def usage_guide():
    """Usage guide — explains each form, input formats, output formats."""
    return render_template("usage_guide.html")


@app.route("/help/examples", methods=["GET"])
def example_queries():
    """Pre-filled example queries the user can click to try the database."""
    return render_template("example_queries.html")


@app.route("/help/license", methods=["GET"])
def license_page():
    # NOTE: function is named `license_page`, not `license`, because
    # `license` is a built-in Python name and shadowing it causes confusion.
    """License / data-use page."""
    return render_template("license.html")


# ===========================================================================
# SECTION 2: AJAX / JSON ENDPOINTS (data the JavaScript fetches)
# ===========================================================================
# These don't render HTML — they return JSON. The frontend JavaScript
# (jQuery $.get in our case) calls them in the background to load chart
# data WITHOUT a full page reload. This is the AJAX requirement (evalue #10).
#
# We'll fill these in as each analysis page is built. Stubs for now so the
# routes exist and don't 404.

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
    """
    Returns Sankey data: list of [source, target, valueue] triples.
    Aggregates ASV counts across taxonomic ranks: Phylum -> Class -> Order -> Family.
    """

    load_dotenv()
    # set username and password
    db_usr = os.environ.get("DB_USR")
    db_pw = os.environ.get("DB_PW")
    if request.args:
        # Get the sid from request.args
        sid = request.args.get('sid')
        if sid != "":
            # The query is identical to db/sankey_query.sql. Kept inline for now;
            # could be moved to a .sql file and read in if it grows.
            query = load_sql("sankey.sql")
            params = (sid, sid, sid)

            connection, cursor = None, None
            try:
                connection = mariadb.connect(
                    host = 'bioed-new.bu.edu',
                    user = db_usr,
                    password = db_pw,
                    db = 'Team13',
                    port = 4253)
                
                cursor = connection.cursor()
                cursor.execute(query, params)
                rows = cursor.fetchall()
                # convert tuples to list
                results = [[source, target, int(value)] for source, target, value in rows]
                return jsonify(results)
            except mariadb.Error as e:
                return jsonify({"error": str(e)}), 500
            finally:
                if cursor:   cursor.close()
                if connection: connection.close()
        return jsonify("")
    return jsonify("")


@app.route("/api/cancer_diversity_search", methods=["GET"])
def api_cancer_diversity_search():
    """Returns per-patient diversity scores for a given cancer type (shannon.html).
    Params:
        cancer_type  -- partial match against Patient.cancer_type
        index_type   -- 'shannon' (default) or 'simpson'
    """
    cancer_type = request.args.get("cancer_type", "").strip()
    index_type  = request.args.get("index_type", "shannon").strip()
    if not cancer_type:
        return jsonify([])

    if index_type == "simpson":
        score_expr = "ROUND(1 - SUM(o.relative_abundance * o.relative_abundance), 4)"
    else:
        score_expr = "ROUND(-SUM(o.relative_abundance * LOG(o.relative_abundance)), 4)"

    conn, cursor = get_db_connection()
    cursor.execute(
        f"SELECT p.bbid, p.cancer_type, p.cancer_category, p.response_status, "
        f"{score_expr} AS diversity_score "
        "FROM Patient p "
        "JOIN Sample s ON p.bbid = s.bbid "
        "JOIN Observation o ON s.sid = o.sid "
        "WHERE p.cancer_type LIKE ? AND o.relative_abundance > 0 "
        "GROUP BY p.bbid, p.cancer_type, p.cancer_category, p.response_status",
        (f"%{cancer_type}%",)
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([[row[0], row[1], row[2], row[3], row[4]] for row in rows])


# ===========================================================================
# SECTION 3: DATABASE HELPER (centralize the connection logic)
# ===========================================================================
# Pulled from HW3/HW5 pattern. Once we deploy to bioed-new, host/port/user
# will change — having one function means we update in one place.

def get_db_connection():
    """
    Returns a (connection, cursor) pair. Caller is responsible for closing
    both.
    """
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
# `if __name__ == '__main__'` means: only run the dev server when this file
# is executed directly (`python cmd.py`). When deployed on bioed-new under
# Apache/WSGI, that server imports this file but does NOT run this block.
#
# debug=True gives auto-reload on file save and a browser-based debugger
# when something crashes. Turn it OFF in production — it's a security hole.

if __name__ == "__main__":
    app.run(debug=True, port=8073)