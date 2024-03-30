import pandas as pd
from deseguys import clean_columns, combine_fields

file_prep = {
    "llc_company.csv": lambda x: (
        pd.read_csv("csv_source/llcallmst.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))[
            ["file_number", "records_off_street", "records_off_city", "records_off_zip"]
        ]
        .assign(state="IL")
        .assign(type="company")
        .merge(
            (
                pd.read_csv("csv_source/llcallnam.csv")
                .pipe(clean_columns)
                .assign(
                    file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
                )
            ),
            how="left",
            on="file_number",
        )
        .reset_index()
        .to_csv("csv_prep/llc_company.csv", index=False)
    ),
    "llc_dba.csv": lambda x: (
        pd.read_csv("csv_source/llcallase.csv",  low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index()
        .to_csv("csv_prep/llc_dba.csv", index=False)
    ),
    "llc_old.csv": lambda x: (
        pd.read_csv("csv_source/llcallold.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index()
        .to_csv("csv_prep/llc_old.csv", index=False)
    ), 
    "llc_managers.csv": lambda x: (
        pd.read_csv("csv_source/llcallmgr.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index() 
        .to_csv("csv_prep/llc_managers.csv", index=False)
    ),
    "llc_agents.csv": lambda x: (
        pd.read_csv("csv_source/llcallagt.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .assign(state = "IL")
        .reset_index() 
        .to_csv('csv_prep/llc_agents.csv', index=False)
    ), 
    "corp_officers.csv": lambda x: (
        pd.read_csv("csv_source/cdxallmst.csv")
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index() 
        .to_csv("csv_prep/corp_officers.csv", index=False)
    ), 
    "corp_company.csv": lambda x: (
        pd.read_csv("csv_source/cdxallnam.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index() 
        .to_csv("csv_prep/corp_company.csv", index=False)
    ),
    "corp_agent.csv": lambda x: (
        pd.read_csv("csv_source/cdxallagt.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .assign(state = "IL")
        .reset_index() 
        .to_csv("csv_prep/corp_agent.csv", index=False)
    ), 
    "corp_old_names.csv": lambda x: (
        pd.read_csv("csv_source/cdxallaon.csv", low_memory=False)
        .pipe(clean_columns)
        .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        .reset_index() 
        .to_csv("csv_prep/corp_old_names.csv", index=False)
    ), 
    "candidates.csv": lambda x: (
        pd.read_csv("csv_source/candidates.csv", low_memory=False)
        .rename(columns={"id": f"candidate_id"})
        .assign(name = lambda df: df['first_name'] + ' ' + df['last_name'])
        .assign(street = lambda df: df['address_1'].fillna('') + ' ' + df['address_2'].fillna(''))
        .assign(street = lambda df: df['street'].str.strip())
        .drop(columns = ['offices_id', 'residence_county', 'party', 'redaction_requested', 'last_name', 'first_name'])
        .reset_index()
        .to_csv("csv_prep/candidates.csv", index=False)
    ), 
    "committees.csv": lambda x: (
        pd.read_csv('csv_source/committees.csv', low_memory=False)
        [['id', 'type', 'name', 'address1', 'address2', 'address3', 'city', 'state', 'zipcode', 'party', 'purpose']]
        .pipe(combine_fields, combined_name="street", fields=['address1', 'address2', 'address3'])
        .rename(columns = {"id": "committee_id"})
        .reset_index()
        .to_csv('csv_prep/committees.csv', index=False)
    ), 
    "receipts.csv": lambda x: (
        pd.read_csv("csv_source/receipts.csv", low_memory=False)
        .pipe(combine_fields, combined_name = "name",           fields = ['first_name', 'last_name'])
        .pipe(combine_fields, combined_name = 'street',         fields = ['address1', 'address2'])
        .pipe(combine_fields, combined_name = 'vendor_name',    fields = ['vendor_first_name', 'vendor_last_name'])
        .pipe(combine_fields, combined_name = 'vendor_street',  fields = ['vendor_address1', 'vendor_address2'])
        .rename(columns = {"id": "donation_id"})
        .drop(columns = ['filed_doc_id', 'etrans_id', 'redaction_requested', 'description', 'd2_part', 'archived', 'country'])
        .reset_index()
        .to_csv('csv_prep/receipts.csv', index=False)
    ), 
    "contracts.csv": lambda x: (
        pd.read_csv('csv_source/contracts.csv', low_memory=False)
        .pipe(clean_columns)
        .astype({"revision_number": "str"})
        .assign(
            contract_revision=lambda df: df["purchase_order_(contract)_number"]
            + " rev "
            + df["revision_number"]
        )
        .pipe(
            combine_fields,
            combined_name="vendor_street",
            fields=["address_1", "address_2"],
        )
        .reset_index()
        .to_csv('csv_prep/contracts.csv', index=False)    
    )
}

def prep_files():
    for f in file_prep:
        print(f"prepping {f}")
        file_prep[f](f)
