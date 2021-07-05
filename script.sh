podman run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=SAsa12345678!' -e 'MSSQL_AGENT_ENABLED=true' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-CU9-ubuntu-16.04
