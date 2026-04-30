#!/usr/bin/env python3
"""
Cancer Microbiome Database (CMD) — Flask application
BF768 Spring 2026 | Cam Nowack, Jack Sherry, Simon Lu

This file is the main entry point for the web application. It defines all
URL routes ("endpoints") and tells Flask which HTML template to render for
each one. Database queries and chart-generating logic will be added later
in separate functions or modules — keep this file focused on *routing*.
"""

from flask import Flask, render_template, jsonify, request
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


@app.route("/analysis/metaanalysis", methods=["GET"])
def metaanalysis():
    """Meta-analysis"""
    return render_template("metaanalysis.html")


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


@app.route("/api/meta1_filters", methods=["GET"])
def api_meta1_filters():
    """
    Lightweight metadata for Meta1 filter controls.
    Returns SIG genera by group and BMC cancer categories that have
    baseline stool samples.
    """
    conn, cursor = get_db_connection()
    try:
        cursor.execute("""
            SELECT DISTINCT sig_group, genus
            FROM   Taxonomy
            WHERE  sig_group IN ('SIG1', 'SIG2') AND genus IS NOT NULL
            ORDER  BY sig_group, genus
        """)
        genera_rows = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT p.cancer_category
            FROM   Patient p
            JOIN   Sample  s ON p.patient_id = s.patient_id
            WHERE  p.data_source      = 'BMC'
              AND  s.sample_type      = 'stool'
              AND  p.cancer_category IS NOT NULL
            ORDER  BY p.cancer_category
        """)
        cat_rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    return jsonify({
        "sig1_genera":           [g for sg, g in genera_rows if sg == 'SIG1'],
        "sig2_genera":           [g for sg, g in genera_rows if sg == 'SIG2'],
        "bmc_cancer_categories": [r[0] for r in cat_rows],
    })


@app.route("/api/meta1_data", methods=["GET"])
def api_meta1_data():
    """
    Meta1 — SIG genus prevalence.

    Query params:
      view        : 'overall' (default) | 'os12'
      timepoint   : 'Baseline' (default) | 'Follow Up' | 'Progression' | 'All'
      sample_type : 'stool' (default) | 'buccal' | 'nasal' | 'all'

    BMC genus record  : { genus, groups: [{label, n_detected, n_total, prevalence}] }
    Derosa record (overall): { genus, n_detected, n_total, prevalence }
    Derosa record (os12):    { genus, groups: [{label, n_detected, n_total, prevalence}] }
    """
    view        = request.args.get('view',        'overall')
    timepoint   = request.args.get('timepoint',   'Baseline')
    sample_type = request.args.get('sample_type', 'stool')

    if view        not in ('overall', 'os12'):
        view = 'overall'
    if timepoint   not in ('Baseline', 'Follow Up', 'Progression', 'All'):
        timepoint = 'Baseline'
    if sample_type not in ('stool', 'buccal', 'nasal', 'all'):
        sample_type = 'stool'

    # Timepoint derived-column expression (BMC only; Derosa is always baseline)
    TP_CASE = """
        CASE
            WHEN s.timepoint REGEXP '(?i)baseline'
                THEN 'Baseline'
            WHEN s.timepoint REGEXP '(?i)progress|off.treatment|post.treatment|post.progression'
                THEN 'Progression'
            WHEN s.timepoint REGEXP '(?i)month|week|follow|treatment|post'
                THEN 'Follow Up'
            ELSE 'Other'
        END
    """

    # WHERE fragments for BMC queries (values are whitelist-validated above)
    tp_where = f"AND ({TP_CASE}) = '{timepoint}'" if timepoint != 'All' else ""
    st_where = f"AND s.sample_type = '{sample_type}'" if sample_type != 'all' else ""

    # GROUP BY expression and NULL guard for BMC
    if view == 'os12':
        bmc_grp  = "CASE WHEN p.os_months >= 12 THEN 'OS≥12' WHEN p.os_months < 12 THEN 'OS<12' END"
        bmc_null = "AND p.os_months IS NOT NULL"
    else:
        bmc_grp  = "p.cancer_category"
        bmc_null = "AND p.cancer_category IS NOT NULL"

    conn, cursor = get_db_connection()
    try:
        # ── BMC denominator ───────────────────────────────────────────
        cursor.execute(f"""
            SELECT ({bmc_grp}) AS grp_label, COUNT(DISTINCT p.patient_id) AS n_total
            FROM   Patient p
            JOIN   Sample  s ON p.patient_id = s.patient_id
            WHERE  p.data_source = 'BMC'
              {bmc_null}
              {tp_where}
              {st_where}
            GROUP  BY grp_label
        """)
        bmc_totals = {r[0]: r[1] for r in cursor.fetchall() if r[0] is not None}

        # ── BMC numerator ─────────────────────────────────────────────
        cursor.execute(f"""
            SELECT t.sig_group, ga.genus, ({bmc_grp}) AS grp_label,
                   COUNT(DISTINCT ga.patient_id) AS n_detected
            FROM   GenusAbundance ga
            JOIN   Sample   s ON ga.sid        = s.sid
            JOIN   Patient  p ON ga.patient_id = p.patient_id
            JOIN   Taxonomy t ON ga.genus      = t.genus
            WHERE  p.data_source      = 'BMC'
              {bmc_null}
              AND  t.sig_group       IN ('SIG1', 'SIG2')
              AND  ga.relative_abundance > 0
              {tp_where}
              {st_where}
            GROUP  BY t.sig_group, ga.genus, grp_label
        """)
        bmc_det = {(r[0], r[1], r[2]): r[3] for r in cursor.fetchall()}

        # ── Derosa denominator (always stool; baseline-only cohort) ───
        if view == 'os12':
            cursor.execute("""
                SELECT CASE WHEN p.os_months >= 12 THEN 'OS≥12'
                            WHEN p.os_months  < 12 THEN 'OS<12' END AS grp_label,
                       COUNT(DISTINCT p.patient_id) AS n_total
                FROM   Patient p
                JOIN   Sample  s ON p.patient_id = s.patient_id
                WHERE  p.data_source = 'Derosa_NSCLC'
                  AND  s.sample_type = 'stool'
                  AND  p.os_months  IS NOT NULL
                GROUP  BY grp_label
            """)
            derosa_totals = {r[0]: r[1] for r in cursor.fetchall()}
            derosa_total  = 0
        else:
            cursor.execute("""
                SELECT COUNT(DISTINCT p.patient_id)
                FROM   Patient p
                JOIN   Sample  s ON p.patient_id = s.patient_id
                WHERE  p.data_source = 'Derosa_NSCLC'
                  AND  s.sample_type = 'stool'
            """)
            derosa_total  = cursor.fetchone()[0] or 0
            derosa_totals = {}

        # ── Derosa numerator ──────────────────────────────────────────
        if view == 'os12':
            cursor.execute("""
                SELECT t.sig_group, ga.genus,
                       CASE WHEN p.os_months >= 12 THEN 'OS≥12'
                            WHEN p.os_months  < 12 THEN 'OS<12' END AS grp_label,
                       COUNT(DISTINCT ga.patient_id) AS n_detected
                FROM   GenusAbundance ga
                JOIN   Sample   s ON ga.sid        = s.sid
                JOIN   Patient  p ON ga.patient_id = p.patient_id
                JOIN   Taxonomy t ON ga.genus      = t.genus
                WHERE  p.data_source = 'Derosa_NSCLC'
                  AND  s.sample_type = 'stool'
                  AND  t.sig_group  IN ('SIG1', 'SIG2')
                  AND  ga.relative_abundance > 0
                  AND  p.os_months  IS NOT NULL
                GROUP  BY t.sig_group, ga.genus, grp_label
            """)
            derosa_det_os12 = {(r[0], r[1], r[2]): r[3] for r in cursor.fetchall()}
            derosa_det      = {}
        else:
            cursor.execute("""
                SELECT t.sig_group, ga.genus,
                       COUNT(DISTINCT ga.patient_id) AS n_detected
                FROM   GenusAbundance ga
                JOIN   Sample   s ON ga.sid        = s.sid
                JOIN   Patient  p ON ga.patient_id = p.patient_id
                JOIN   Taxonomy t ON ga.genus      = t.genus
                WHERE  p.data_source = 'Derosa_NSCLC'
                  AND  s.sample_type = 'stool'
                  AND  t.sig_group  IN ('SIG1', 'SIG2')
                  AND  ga.relative_abundance > 0
                GROUP  BY t.sig_group, ga.genus
            """)
            derosa_det      = {(r[0], r[1]): r[2] for r in cursor.fetchall()}
            derosa_det_os12 = {}

        # ── all SIG genera ────────────────────────────────────────────
        cursor.execute("""
            SELECT DISTINCT sig_group, genus FROM Taxonomy
            WHERE  sig_group IN ('SIG1', 'SIG2') AND genus IS NOT NULL
            ORDER  BY sig_group, genus
        """)
        all_genera = cursor.fetchall()

    finally:
        cursor.close()
        conn.close()

    cats = ['OS<12', 'OS≥12'] if view == 'os12' else sorted(bmc_totals.keys())

    bmc_sig1, bmc_sig2     = [], []
    derosa_sig1, derosa_sig2 = [], []

    for sg, genus in all_genera:
        # BMC record
        groups = []
        for cat in cats:
            n_tot = bmc_totals.get(cat, 0)
            if n_tot == 0:
                continue
            n_det = bmc_det.get((sg, genus, cat), 0)
            groups.append({
                "label":      cat,
                "n_detected": n_det,
                "n_total":    n_tot,
                "prevalence": round(n_det / n_tot * 100.0, 1),
            })
        (bmc_sig1 if sg == 'SIG1' else bmc_sig2).append({"genus": genus, "groups": groups})

        # Derosa record
        if view == 'os12':
            groups = []
            for grp in ['OS<12', 'OS≥12']:
                n_tot = derosa_totals.get(grp, 0)
                if n_tot == 0:
                    continue
                n_det = derosa_det_os12.get((sg, genus, grp), 0)
                groups.append({
                    "label":      grp,
                    "n_detected": n_det,
                    "n_total":    n_tot,
                    "prevalence": round(n_det / n_tot * 100.0, 1),
                })
            derosa_rec = {"genus": genus, "groups": groups}
        else:
            n_det = derosa_det.get((sg, genus), 0)
            derosa_rec = {
                "genus":      genus,
                "n_detected": n_det,
                "n_total":    derosa_total,
                "prevalence": round(n_det / derosa_total * 100.0, 1) if derosa_total > 0 else 0.0,
            }
        (derosa_sig1 if sg == 'SIG1' else derosa_sig2).append(derosa_rec)

    return jsonify({
        "view":           view,
        "timepoint":      timepoint,
        "sample_type":    sample_type,
        "bmc_categories": cats,
        "bmc":    {"sig1": bmc_sig1,    "sig2": bmc_sig2},
        "derosa": {"sig1": derosa_sig1, "sig2": derosa_sig2},
    })


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
        user="slianglu",          # TODO: replace with shared team account
        password="slianglu",   # TODO: load from env var, never commit
        db="Team13",                # TODO: confirm final DB name with team
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
    app.run(debug=True, port=5005)