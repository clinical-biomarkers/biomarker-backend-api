# Config File Definitions 

```json
    {
        "project": "project name",
        "api_port": {
            "prd": "production server api port",
            "beta": "beta server api port",
            "tst": "test server api port",
            "dev": "development server api port"
        },
        "data_path": "prefix filepath for the bind-mounted directory",
        "dbinfo": {
            "dbname": "database name",
            "port": { 
                "prd": "production server database port",
                "beta": "beta server database port",
                "tst": "test server database port",
                "dev": "development server database port"
            },
            "bridge_network": "docker bridge network name",
            "admin": {
                "db": "admin database name (admin)",
                "user": "admin username",
                "password": "admin password"
            },
            "biomarkerdb_api": {
                "db": "database name",
                "collection": "data collection",
                "id_collection": "ID map",
                "user": "biomarker database username",
                "password": "biomarker database password"
            }
        }
    }
```
