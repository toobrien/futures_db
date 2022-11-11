echo "SELECT * FROM ohlc WHERE contract_id = \"$1\" ORDER BY date DESC LIMIT 1;" | sqlite3 futures.db
