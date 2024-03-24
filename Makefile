.PHONY : all

all : combined.zip

combined.zip : combined.db
	zip $@ $^

combined.db : build_db.py test_data.zip
	unzip test_data.zip
	python build_db.py

# %.csv : test_data.zip
	# unzip test_data.zip

test_data.zip :
	@curl --remote-name https://storage.googleapis.com/pdt_central/deseguys/test_data.zip