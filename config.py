HOST = "localhost"
PORT = 9200
MAPPING = {
    "mappings": {
        "iis_logs": {
            "properties": {
                "c-ip": {"type": "ip"},
                "cs-bytes": {"type": "double"},
                "cs-uri-stem": {"type": "text"},
                "sc-bytes": {"type": "double"},
                "time": {
                    "type": "date",
                    "format": "hour_minute_second"
                },
                "time-taken": {"type": "integer"}
            }
        }
    }
}