# 1. Create schema
mariadb -h bioed-new.bu.edu -P 4253 -u slianglu -p slianglu < scripts/schema.sql

# 2. Load BMC data + SIG annotation
python3 scripts/load_bmc_data.py

# 3. Process Derosa raw files → processed CSVs
python3 scripts/process_derosa.py

# 4. Load processed Derosa CSVs into database
python3 scripts/load_derosa_data.py