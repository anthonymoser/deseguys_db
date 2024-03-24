from deseguys import * 
from messy_data import clean_name, clean_street 

data_sources = {
    
    "candidates": DataSource(
                    filename = "data/candidates.csv", 
                    cleaning = lambda df: ( 
                                df
                                    .rename(columns={"id": f"candidate_id"})
                                    .assign(type = 'candidate')
                                    .assign(name = lambda df: df['first_name'] + ' ' + df['last_name'])
                                    .assign(street = lambda df: df['address_1'].fillna('') + ' ' + df['address_2'].fillna(''))
                                    .assign(street = lambda df: df['street'].str.strip())
                                    .drop(columns = ['offices_id', 'residence_county', 'party', 'redaction_requested', 'last_name', 'first_name'])),
                    record_maps = [
                        RecordMap(
                            name = Name(),
                            address = Address(street = 'street', city='city', state = 'state', postal_code='zipcode'),
                            entity = Entity(type='candidate', index_type='candidate_id', index_id='candidate_id', is_primary=1), 
                            primary_entity = True)
                    ]), 
    
    
    "committees": DataSource(
                    filename = "data/committees.csv", 
                    cleaning = lambda df: (
                                df[['id', 'type', 'name', 'address1', 'address2', 'address3', 'city', 'state', 'zipcode', 'party', 'purpose']]
                                .pipe(combine_fields, combined_name="street", fields=['address1', 'address2', 'address3'])
                                .rename(columns = {"id": "committee_id"})
                            ), 
                    record_maps=[ 
                        RecordMap(
                            address = Address(street = 'street', city='city', state='state', postal_code='zipcode'),
                            name = Name(), 
                            entity = Entity(type='committee', index_type = 'committee_id', index_id = 'committee_id', details = {"purpose": "purpose"}, is_primary=1), 
                            primary_entity = True
                        )
                    ]), 
    
    
    "receipts": DataSource( 
                        filename = 'data/receipts.csv', 
                        cleaning = lambda df: (
                            df
                                .pipe(combine_fields, combined_name = "name",           fields = ['first_name', 'last_name'])
                                .pipe(combine_fields, combined_name = 'street',         fields = ['address1', 'address2'])
                                .pipe(combine_fields, combined_name = 'vendor_name',    fields = ['vendor_first_name', 'vendor_last_name'])
                                .pipe(combine_fields, combined_name = 'vendor_street',  fields = ['vendor_address1', 'vendor_address2'])
                                .drop(columns = ['filed_doc_id', 'etrans_id', 'redaction_requested', 'description', 'd2_part', 'archived', 'country', 'id'])
                        ),
                        record_maps = [
                            
                            RecordMap(
                                name = Name(), 
                                address = Address(street='street', city='city', state='state', postal_code='zipcode'),
                                entity = Entity(type='donor', index_type = 'committee_id', index_id = 'committee_id'),
                                primary_entity=False, 
                                link='donation', 
                                link_details={"amount": "amount"}
                            ), 
                            
                            RecordMap(
                                name = Name(),
                                address = Address(street='street', city='city', state='state', postal_code='zipcode'),  
                                entity = Entity(type='donor', index_type = 'donation_id', index_id = 'donation_id', is_primary=1), 
                                primary_entity=True, 
                            ), 
                            
                            RecordMap(
                                name = Name(name="vendor_name"), 
                                address = Address(street='vendor_street', city='vendor_city', state='vendor_state', postal_code='vendor_zipcode'), 
                                entity = Entity(type='vendor', index_type = 'donation_id', index_id = 'donation_id'), 
                                primary_entity=False, 
                                link = 'employs'
                            ), 
                            
                        ])
    
}