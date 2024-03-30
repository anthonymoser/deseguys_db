import csv
import json
import shutil
import duckdb as d
from messy_data import clean_name, clean_street, extract_name_and_address


def get_data(filename:str)->list:

    data = []
    with open(filename) as csvfile:
        readCSV = csv.DictReader(csvfile)
        for row in readCSV:
            row = {k: row[k] for k in row.keys()}
            
            data.append(row)

    return data


def export_entity_csv(filename:str, data:list, schema:dict, start_row:int = 0):
    
    try:
        with open(filename, 'a+', newline='') as csvfile:
            print(f"exporting {filename}")
            del(schema['filename'])
            fieldnames = [k for k in schema.keys() if k != 'extract_field']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if start_row == -1:
                
                writer.writeheader()
            for row in data:
                row_id = int(row[schema['row_id']])
                if row_id > start_row:
                    if schema.get('extract_field', None) is not None:
                        raw = row[schema['extract_field']]
                        if raw != "":
                            record = extract_name_and_address(raw)
                            if record is not None:
                                values = {
                                    'row_id':   row_id,
                                    # 'id':       str(uuid4()),
                                    'link_type': schema['link_type'],
                                    'entity_type': schema['entity_type'],
                                    'name':     record.get('name', None), 
                                    'street':   record.get('street', None), 
                                    'city':     record.get('city', None),
                                    'state':    record.get('state', None),
                                    'postal_code': record.get('postal_code', None),
                                    'details':  {},
                                    'index_type': schema['index_type'], 
                                    'index_value': row.get(schema['index_value'], None), 
                                    'is_primary': schema['is_primary']
                                }
                                writer.writerow(values)   
                        
                    else:    
                        values = {
                            'row_id':   row_id,
                            # 'id':       str(uuid4()),
                            'link_type': schema['link_type'],
                            'entity_type': schema['entity_type'],
                            'name':     clean_name(row.get(schema['name'], None)), 
                            'street':   clean_street(row.get(schema['street'], None)), 
                            'city':     row.get(schema['city'], None),
                            'state':    row.get(schema['state'], None),
                            'postal_code': row.get(schema['postal_code'], None),
                            'details':  json.dumps({key: row.get(key, None) for key in schema['details']}),
                            'index_type': schema['index_type'], 
                            'index_value': row.get(schema['index_value'], None), 
                            'is_primary': schema['is_primary']
                        }
                        if values['name'] != "" or values['street'] != "":
                            writer.writerow(values)
                        
                    

        print("Exported file " + filename)
         
    except Exception as e:
        print(row)
        print("Error exporting file: ", e)
        

def get_row_id(filename:str) -> int:
    try:
        result = d.sql(f"SELECT max(row_id) FROM '{filename}'")
        row_id = result.fetchone()        
        row_id = row_id[0] if row_id[0] is not None else -1
        return row_id
    except Exception as e:
        print(e)
        return -1
    
            
def parse(schema: dict):
    filename = schema['filename']
    print(filename)
    data = get_data(f"csv_prep/{filename}")
    if schema.get('extract_field', None) is not None:
        export_path = f"{filename[:-4]}_{schema['link_type']}.csv"
    else:
        export_path = f"{filename[:-4]}_{schema['entity_type']}.csv"
        
    row_id = get_row_id(export_path)
    print("starting with row ", row_id)
        
    export_entity_csv(export_path, data, schema, row_id)
    shutil.move(export_path, f"csv_export/{export_path}")
    print(f"{filename} parsed") 
    
