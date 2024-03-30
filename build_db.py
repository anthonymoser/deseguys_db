from parse_csv import parse
from csv_schemas import schemas
import glob
import multiprocessing
# from google.cloud import storage
# import google.auth
import zipfile
import duckdb as d
from prep_csvs import file_prep
# storage_client = storage.Client()

# key = 'app engine service acct linear-enigma-307504-40cd0b800f9b.json'
# credentials, project = google.auth.load_credentials_from_file(
#     filename=key,
#     scopes=[
#         "https://www.googleapis.com/auth/drive",
#         "https://www.googleapis.com/auth/cloud-platform",
#     ]
# )
# storage_client = storage.Client(credentials=credentials, project=project)

# def upload_blob(bucket_name, source_file_name, destination_blob_name):
#     """Uploads a file to the bucket."""
        
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
#     blob.upload_from_filename(source_file_name)

#     print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# def download_blob(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a blob from the bucket."""

#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)

#     print(
#         "Downloaded storage object {} from bucket {} to local file {}.".format(
#             source_blob_name, bucket_name, destination_file_name
#         )
#     )

def get_file_list(filepath:str) ->list:
    return sorted(glob.glob(filepath))

def process_csvs():
    exports = get_file_list('csv_export/*.csv')
    p = multiprocessing.Pool()
    for s in schemas:
        filename = s['filename']
        if s.get('extract_field', None) is not None:
            export_path = f"{filename[:-4]}_{s['link_type']}.csv"
        else:
            export_path = f"{filename[:-4]}_{s['entity_type']}.csv"
        
        if export_path not in exports:
            print("processing ", filename)
            p.apply_async(parse, [s]) 

    p.close()
    p.join() # Wait for all child processes to close.

def create_table():
    table_sql = """ 
    CREATE TABLE IF NOT EXISTS entities(
        entity_type VARCHAR,
        link_type VARCHAR,
        name VARCHAR,
        street VARCHAR,
        city VARCHAR,
        state VARCHAR,
        postal_code VARCHAR,
        details VARCHAR,
        index_type VARCHAR,
        index_value VARCHAR, 
        is_primary TINYINT
        )
    """
    duck.sql(table_sql)

def load_exports(exports:list):
    for e in exports:
        insert_sql = f"""
            INSERT INTO entities
            SELECT 
                entity_type, 
                link_type, 
                name, 
                street, 
                city, 
                state, 
                postal_code, 
                details, 
                index_type, 
                index_value, 
                is_primary
            FROM 
                '{e}'
            """
        duck.sql(insert_sql)

def export_csv(filename:str):
    export_sql = f"COPY (SELECT * FROM entities) TO '{filename}' (HEADER, DELIMITER ',');"
    duck.sql(export_sql)
    
def main():   
    global duck  
    # print("Downloading source data")
    # download_blob('pdt_central', 'deseguys/csv_source.zip', 'csv_source.zip')
    # with zipfile.ZipFile("csv_source.zip", mode="r") as archive:
        # archive.extractall(".")
    # print("data extracted")
    
    prepped = [f.split('/').pop() for f in get_file_list('csv_prep/*.csv')]
    for f in file_prep:
        if f not in prepped:
            print(f"prepping {f}")
            file_prep[f](f)
    print("files prepped")
      
    process_csvs()
    exports = get_file_list('csv_export/*.csv')
    
    print("initializing db")
    duck = d.connect('duck.duckdb')    
    create_table()
    load_exports(exports)
    export_csv('entities.csv')

    # with zipfile.ZipFile("combined.zip", mode="w") as archive:
    #     archive.write("duck.duckdb")
    #     archive.write("duck.duckdb.wal")
    #     archive.write("entities.csv")
    # print("db zipped")    

    # upload_blob('pdt_central', 'combined.zip', 'deseguys/combined.zip')

if __name__ == '__main__':
    main()