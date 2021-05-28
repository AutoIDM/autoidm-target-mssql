#/bin/bash 
cat codata_sens.txt | ./target-mssql.sh --config=target_mssql/.secrets/config.json
