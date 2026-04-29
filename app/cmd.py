#!/usr/bin/env python3
"""
Cancer Microbiome Database (CMD) — Flask application
BF768 Spring 2026 | Cam Nowack, Jack Sherry, Simon Lu

This file is the main entry point for the web application. It defines all
URL routes ("endpoints") and tells Flask which HTML template to render for
each one. Database queries and chart-generating logic will be added later
in separate functions or modules — keep this file focused on *routing*.
"""

from flask import Flask, request, render_template, jsonify
import mariadb

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
    Landing page. Satisfies Benson eval #2: project name, student
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
# data WITHOUT a full page reload. This is the AJAX requirement (eval #10).
#
# We'll fill these in as each analysis page is built. Stubs for now so the
# routes exist and don't 404.

@app.route("/api/sankey_data", methods=["GET"])
def api_sankey_data():
    """Returns Sankey-format data: [[source, target, value], ...]."""
    # TODO: query DB, format for Google Charts Sankey
    return jsonify([])


@app.route("/api/shannon_data", methods=["GET"])
def api_shannon_data():
    """Returns Shannon diversity values per sample/group."""
    # TODO
    return jsonify([])


# ===========================================================================
# SECTION 3: DATABASE HELPER (centralize the connection logic)
# ===========================================================================
# Pulled from HW3/HW5 pattern. Once we deploy to bioed-new, host/port/user
# will change — having one function means we update in one place.

def get_db_connection():
    """
    Returns a (connection, cursor) pair. Caller is responsible for closing
    both. We disable autocommit so we can wrap multi-step writes in a
    transaction later if needed.
    """
    connection = mariadb.connect(
        host="bioed-new.bu.edu",
        user="cnowack",          # TODO: replace with shared team account
        password="REPLACE_ME",   # TODO: load from env var, never commit
        db="cmd",                # TODO: confirm final DB name with team
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
    app.run(debug=True, port=5007)