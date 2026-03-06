1. The .gitignore File (Critical for Sequencing Data)

2. Documentation (docs/)

Keep your draft and final proposals here. As you approach the March 31 deadline, you will also store your Entity-Relationship (ER) diagram here.

3. Database Assets (db/)

Isolate your SQL logic from your web application logic.

schema.sql: This file will contain your CREATE TABLE statements, explicitly defining your primary keys, foreign keys mapping sample sites to patients, and the indexes mentioned in requirement 11.

sample_queries.sql: Dedicate a file to the three sample queries required for step 12.

Data Loader: A Python script to read the outputs from the ampliseq Nextflow pipeline (ASV tables, taxonomy assignments) and insert them into your MariaDB database.

4. The Web Application (app/)

This will house the user interface for your interactive web application. If you are using Flask, app.py will route user requests (like exploring specific clinical characteristics) to the database, retrieve the longitudinal data, and pass it to the HTML files in templates/ for rendering.

5. External Processing (scripts/)

Requirement 13 asks for a description of data processing external to the database. If you are performing statistical analysis or generating the phylogenetic trees dynamically before sending them to the web app, those scripts belong here.

6. Environment Management (requirements.txt)

To ensure you, Jack, and Simon are using the exact same libraries (e.g., specific versions of Flask, Pandas, or database connectors), you should use a virtual environment.

Create the environment on bioed-new: python3 -m venv venv

Activate it: source venv/bin/activate

Install your packages: pip install flask mariadb pandas

Freeze the dependencies to a file: pip freeze > requirements.txt

Any team member who clones the repo can then run pip install -r requirements.txt to replicate the environment perfectly.