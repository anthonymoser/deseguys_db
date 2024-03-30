.PHONY : all

all : combined.zip

combined.zip : duck.duckdb duck.duckdb.wal entities.csv 
	zip $@ $^

combined.db : build_db.py csv_source.zip
	unzip csv_source.zip
	python build_db.py

csv_source.zip :
	@curl --remote-name https://storage.googleapis.com/pdt_central/deseguys/csv_source.zip