from deseguys import *
from messy_data import *


data_sources = {
    
    "llc_company": DataSource(
        filename="data/llcallmst.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
            .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
            [["file_number","records_off_street","records_off_city","records_off_zip"]]
            # .assign(postal_code=lambda df: df["postal_code"].str[:5])
            .assign(state="IL")
            .assign(type="company")
            .merge(
                (
                    pd.read_csv("data/llcallnam.csv")
                    .pipe(clean_columns)
                    .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
                ),
                how="left",
                on="file_number",
            )
        ),
        record_maps=[
            RecordMap(
                address=Address(
                    street="records_off_street",
                    city="records_off_city",
                    state="state",
                    postal_code="records_off_zip",
                ),
                name=Name(),
                entity=Entity(
                    type="company", 
                    index_type="LLC file number", 
                    index_id="file_number", 
                    is_primary=1
                ),
                primary_entity=True,
            ),
        ],
    ),
    
    "llc_dba": DataSource(
        filename="data/llcallase.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns).assign(
                file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
            )
        ),
        record_maps=[
            RecordMap(
                address=None,
                name=Name(),
                entity=Entity(
                    type="company", index_type="LLC file number", index_id="file_number"
                ),
                primary_entity=False,
                link="dba",
            )
        ],
    ),
    
    "llc_old": DataSource(
        filename="data/llcallold.csv",
        cleaning=lambda df: df.pipe(clean_columns).assign(
            file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
        ),
        record_maps=[
            RecordMap(
                address=None,
                name=Name(),
                entity=Entity(
                    type="company",
                    index_type="LLC file number",
                    index_id="file_number",
                ),
                primary_entity=False,
                link="old_name",
            )
        ],
    ),
    
    "llc_managers": DataSource(
        filename="data/llcallmgr.csv",
        cleaning=lambda df: df.pipe(clean_columns).assign(
            file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
        ),
        record_maps=[
            RecordMap(
                address=Address(
                    street="mm_street",
                    city="mm_city",
                    state="mm_juris",
                    postal_code="mm_zip",
                ),
                name=Name(name="mm_name"),
                entity=Entity(
                    type="manager", index_type="LLC file number", index_id="file_number"
                ),
                primary_entity=False,
                link="manager",
            )
        ],
    ),
    
    "llc_agents": DataSource(
        filename="data/llcallagt.csv",
        cleaning=lambda df: df.pipe(clean_columns).assign(
            file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
        ),
        record_maps=[
            RecordMap(
                address=Address(
                    street="agent_street",
                    city="agent_city",
                    state="IL",
                    postal_code="postal_code",
                ),
                name=Name(name="agent_name"),
                entity=Entity(
                    type="agent", index_type="LLC file number", index_id="file_number"
                ),
                primary_entity=False,
                link="agent",
            ),
        ],
    ),
    
    "corp_company": DataSource(
        filename="data/cdxallnam.csv",
        cleaning=lambda df: df.pipe(clean_columns).assign(
            file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}")
        ),
        record_maps=[
            RecordMap(
                address=None,
                name=Name(),
                entity=Entity(
                    type="company",
                    index_type="CORP file number",
                    index_id="file_number",
                    is_primary=1
                ),
                primary_entity=True,
            )
        ],
    ),
    
    "corp_president": DataSource(
        filename="data/cdxallmst.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
                .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
                [["file_number", "pres_name_addr"]]
                .pipe(
                    extract_names_and_addresses,
                    index_field="file_number",
                    extract_field="pres_name_addr",
                    type="president",
                )),
        record_maps=[
            RecordMap(
                address=Address(
                    street="street",
                    city="city",
                    state="state",
                    postal_code="postal_code",
                ),
                name=Name(),
                entity=Entity(
                    type="president", index_type="CORP file number", index_id="file_number"
                ),
                primary_entity=False,
                clean_data = False,
                link="president",
            )
        ],
    ),
    
    "corp_secretary": DataSource(
        filename = "data/cdxallmst.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
            .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
            [["file_number", "sec_name_addr"]]
            .pipe(
                extract_names_and_addresses,
                index_field="file_number",
                extract_field="sec_name_addr",
                type="secretary",
             )
        ),
        record_maps=[
            RecordMap(
                address=Address(
                    street="street",
                    city="city",
                    state="state",
                    postal_code="postal_code",
                ),
                name=Name(),
                entity=Entity(
                    type="secretary", index_type="CORP file number", index_id="file_number"
                ),
                primary_entity=False,
                clean_data = False,
                link="secretary",
            )
        ],
    ), 
    
    "corp_agent": DataSource(
        filename = "data/cdxallagt.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
            .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
            .assign(state = "IL")
        ),
        record_maps=[
            RecordMap(
                address=Address(
                    street="agent_street",
                    city="agent_city",
                    state="state",
                    postal_code="agent_zip",
                ),
                name=Name(),
                entity=Entity(
                    type="agent", index_type="CORP file number", index_id="file_number"
                ),
                primary_entity=False,
                link="agent",
            )
        ],
    ), 
    
    "corp_old_names": DataSource(
        filename = "data/cdxallaon.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
            .assign(file_number=lambda df: df["file_number"].apply(lambda x: f"{x:08}"))
        ),
        record_maps=[
            RecordMap(
                address=None, 
                name=Name(name="assumed_old_name"),
                entity=Entity(
                    type="company", index_type="CORP file number", index_id="file_number"
                ),
                primary_entity=False,
                link="old_name",
            )
        ],
    ), 
    
}
