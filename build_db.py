from deseguys import * 
from campaigns import data_sources as campaign_ds
from businesses import data_sources as business_ds 
from contracts import data_sources as contracts_ds 
import sqlite3
import json 
import pandas as pd 

conn = sqlite3.connect('combined.db')
cur = conn.cursor() 


def insert_records(): 
    
    global addresses, entities, names, links 
    address_sql = "INSERT INTO addresses(id, street, city, state, postal_code) values (:id, :street, :city, :state, :postal_code);"
    cur.executemany(address_sql, [ a.to_dict() for a in addresses ])
    
    name_sql = "INSERT INTO names(id, name) VALUES (:id, :name)"
    cur.executemany(name_sql, [ n.to_dict() for n in names ])

    inserts = [e.to_dict() for e in entities]
    for e in inserts: 
        e['details'] = json.dumps(e['details'])
    
    entity_sql = "INSERT INTO entities(id, type, name_id, address_id, index_type, index_id, details, is_primary) VALUES (:id, :type, :name_id, :address_id, :index_type, :index_id, :details, :is_primary)"
    cur.executemany(entity_sql, inserts)

    inserts = [ l.to_dict() for l in links ]
    for link in inserts:
        link['details'] = json.dumps(link['details'])
    
    link_sql = "INSERT INTO links(id, source_entity_id, type, target_entity_id, details) VALUES (:id, :source_entity_id, :type, :target_entity_id, :details)"
    cur.executemany(link_sql, inserts)
    conn.commit()
    


sql = {
    "addresses" : """ 
        CREATE TABLE addresses(
            id VARCHAR PRIMARY KEY,
            street VARCHAR, 
            city VARCHAR, 
            state VARCHAR,
            postal_code VARCHAR
        ); """, 
    
    "names": """
        CREATE TABLE names(
            id VARCHAR PRIMARY KEY,
            name VARCHAR
        ); """, 
        
    "entities": """
        CREATE TABLE entities(
            id VARCHAR PRIMARY KEY,
            type VARCHAR,
            name_id VARCHAR,
            address_id VARCHAR,
            index_type VARCHAR,
            index_id VARCHAR,
			details VARCHAR,
            is_primary int,
            FOREIGN KEY(name_id) REFERENCES names(id),
            FOREIGN KEY(address_id) REFERENCES addresses(id)
        );
        """,
        
    "links": """
        CREATE TABLE links(
            id VARCHAR PRIMARY KEY,
            source_entity_id VARCHAR, 
            type VARCHAR, 
            target_entity_id VARCHAR, 
            details VARCHAR
        )
    """ 
}

for table in sql: 
    cur.execute(sql[table])
    conn.commit()
print("db initialized")

for ds in business_ds: 
    print(ds)
    business_ds[ds].load_df()
    business_ds[ds].parse_record_maps()
    
for ds in campaign_ds:
    print(ds) 
    campaign_ds[ds].load_df()
    campaign_ds[ds].parse_record_maps()

for ds in contracts_ds:
    print(ds)
    contracts_ds[ds].load_df()
    contracts_ds[ds].parse_record_maps()

insert_records()