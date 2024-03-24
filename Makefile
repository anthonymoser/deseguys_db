.PHONY : all

all : combined.zip

combined.zip : combined.db
	zip $@ $^

combined.db : build_db.py data.zip
	unzip data.zip
	python build_db.py

data.zip :
	@curl -o data.zip --remote-name https://storage.googleapis.com/pdt_central/deseguys/data.zip
