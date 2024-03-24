import msgspec
import pandas as pd 
import requests 
import glob 
from uuid import uuid4
from typing import Callable
from tqdm import tqdm 
from messy_data import *


addresses = []
names = []
entities = []
links = []
errors = []

address_ids = {}
name_ids = {}
entity_ids = {}

primary_entities = {}
parsed = []

class Entity(msgspec.Struct, kw_only=True):
    id : str|None = None
    type : str 
    name_id : str|None = None 
    address_id : str|None = None
    index_type : str|None = None 
    index_id : str|None = None 
    is_primary: int = 0
    details : dict = {}
    
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid4())
    
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Address(msgspec.Struct, kw_only=True):
    id : str|None = None
    street : str 
    city : str 
    state : str 
    postal_code : str 
    
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid4())
        # self.postal_code = self.postal_code[:5]
    
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}
    
class Name(msgspec.Struct, kw_only=True):  
    id : str|None = None
    name : str = 'name'
    
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid4())
            
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Link(msgspec.Struct, kw_only=True):
    id : str|None = None 
    source_entity_id : str 
    type : str 
    target_entity_id : str 
    details : dict 
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid4())
            
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}
            
class RecordMap(msgspec.Struct, kw_only=True):
    address: Address | None = None
    name : Name | None = None
    entity : Entity | None = None
    primary_entity: bool = False
    clean_data :  bool = True 
    link: str|None = None 
    link_details: dict = {}
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}


class DataSource(msgspec.Struct):
    filename: str  
    cleaning : Callable 
    df : pd.DataFrame | None = None
    record_maps : list[RecordMap] = []
        
    def load_df(self): 
        self.df = pd.read_csv(self.filename).pipe(self.cleaning)
        print(f"loaded {self.filename}")
    
    def parse_record_maps(self): 
        for rm in self.record_maps:
            parse_records(self.df.to_dict('records'), rm)    
    
    

# Utility functions

def download_file(url, filename):
    r = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(r.content)

        
def clean_columns(df:pd.DataFrame)->pd.DataFrame:
    lowercase = { 
        c: c.lower().strip().replace(' ', '_') 
        for c in df.columns }
    df = df.rename(columns=lowercase)
    return df    


def load_files():
    llc_files = glob.glob(f"./llc/*.csv")
    corp_files = glob.glob(f"./corporate/*.csv")

    llc = {}
    corp = {}

    file_map = {
        "llcallmst.csv": "master",
        "llcallnam.csv": "company_name",
        "llcallagt.csv": "agent", 
        "llcallarp.csv": "annual_reports",
        "llcallase.csv": "assumed_name",
        "llcallold.csv": "old_names",
        "llcallmgr.csv": "managers",
        "llcallser.csv": "series", 
        
        "cdxallmst.csv": "master", 
        "cdxallnam.csv": "company_name",
        "cdxallagt.csv": "agent", 
        "cdxallarp.csv": "annual_reports",
        "cdxallaon.csv": "assumed_old_names",
        "cdxallstk.csv": "stock",
        "cdxalloth.csv": "other"
        
    }

    for f in llc_files:
        filename = f.split('/').pop()
        name = file_map.get(filename)
        if name in ['master', 'company_name', 'agent', 'assumed_name', 'old_names', 'managers']:
            print(filename, name)
            llc[name] = clean_columns(pd.read_csv(f, low_memory=False)).assign(file_number = lambda df: df['file_number'].apply(lambda x: f'{x:08}'))
        
    for f in corp_files:
        filename = f.split('/').pop()
        name = file_map.get(filename)
        if name in ['master', 'company_name', 'agent', 'assumed_old_names']:
            print(name, file_map.get(name))
            corp[name] = clean_columns(pd.read_csv(f, low_memory=False)).assign(file_number = lambda df: df['file_number'].apply(lambda x: f'{x:08}'))    
    
    return llc, corp


def parse_records(data:list, schema:RecordMap):
    
    for row in tqdm(data):
        try:
            address_id = None 
            name_id = None 

            row_name = Name( name = clean_name(row[schema.name.name]) if schema.clean_data is True else row[schema.name.name])
            name_id = get_name_id(row_name)
            if name_id == row_name.id:
                names.append(row_name)

            if schema.address is not None:
                try:
                    postal_code = row[schema.address.postal_code][:5]
                except Exception as e: 
                    # print(row)
                    postal_code = None 
                    
                row_address = Address(
                    street = clean_street(row[schema.address.street]) if schema.clean_data is True else row[schema.address.street], 
                    city = row[schema.address.city], 
                    state = row.get(schema.address.state, schema.address.state), 
                    postal_code = postal_code)
                
                if row_address.street != "":
                    address_id = get_address_id(row_address)
                    if address_id == row_address.id:
                        addresses.append(row_address)
            

            row_entity = Entity(
                type = schema.entity.type, 
                name_id = name_id, 
                address_id= address_id, 
                index_type = schema.entity.index_type,
                index_id = row[schema.entity.index_id],
                is_primary = schema.entity.is_primary,
                details = { d: row[schema.entity.details[d]] for d in schema.entity.details }
            )
            
            entity_id = get_entity_id(row_entity)
            if entity_id == row_entity.id:
                entities.append(row_entity)
            
            if schema.primary_entity is True:       
                if row_entity.index_type not in primary_entities: 
                    primary_entities[row_entity.index_type] = {row_entity.index_id: entity_id}
                else:
                    primary_entities[row_entity.index_type][row_entity.index_id] = entity_id 
            
            elif schema.primary_entity is False:
                
                links.append(Link(
                    source_entity_id = entity_id, 
                    type = row.get(schema.link, schema.link),
                    target_entity_id = primary_entities[row_entity.index_type][row_entity.index_id], 
                    details = {}
                ))
        except Exception as e:
            errors.append({"row": row, "error": str(e)})
            continue 
    
    
def combine_fields(df:pd.DataFrame, combined_name:str, fields:list[str]):
    ef = df.copy()
    ef[combined_name] = ""
    for f in fields: 
        ef[combined_name] = ef[combined_name] + " " + ef[f].fillna('')
    ef[combined_name] = ef[combined_name].str.strip()
    return ef.drop(columns=fields)
                

def get_address_id(row_address:Address):
    try:
        address_id = address_ids[row_address.postal_code][row_address.street]
    except KeyError:
        if row_address.postal_code not in address_ids:
            address_ids[row_address.postal_code] = {row_address.street: row_address.id}
        else: 
            address_ids[row_address.postal_code][row_address.street] = row_address.id 
        address_id = row_address.id
    return address_id 


def get_name_id(row_name:Name):
    try:
        name_id = name_ids[row_name.name]
    except KeyError:
        name_ids[row_name.name]=row_name.id
        name_id = row_name.id
    return name_id 


def get_entity_id(row_entity): 
    try:
        entity_id = entity_ids[row_entity.name_id][row_entity.address_id]
    except KeyError:
        if row_entity.name_id not in entity_ids:
            entity_ids[row_entity.name_id] = {row_entity.address_id: row_entity.id}
        else: 
            entity_ids[row_entity.name_id][row_entity.address_id] = row_entity.id 
        entity_id = row_entity.id
    return entity_id 