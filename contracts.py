from deseguys import *
from messy_data import *


data_sources = {
    "contracts": DataSource(
        filename="data/contracts.csv",
        cleaning=lambda df: (
            df.pipe(clean_columns)
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
        ),
        record_maps=[
            RecordMap(
                address=None,
                name=Name(name="contract_revision"),
                entity=Entity(
                    type="contract",
                    index_type="contract number",
                    index_id="contract_revision",
                    details={
                        "description": "purchase_order_description", 
                        "amount": "award_amount", 
                        "start_date": "start_date", 
                        "end_date": "end_date", 
                        "approval_date": "approval_date"
                    },
                    is_primary=1,
                ),
                primary_entity=True,
            ),
            RecordMap(
                name=Name(name="vendor_name"),
                address=Address(
                    street="vendor_street",
                    city="city",
                    state="state",
                    postal_code="zip",
                ),
                entity=Entity(
                    type="vendor",
                    index_type="contract number",
                    index_id="contract_revision",
                    is_primary=0,
                ),
                primary_entity=False,
                link="vendor",
                link_details={"amount": "award_amount"}
            ),
            RecordMap(
                name=Name(name='department'), 
                address = None, 
                entity = Entity(
                    type = "department", 
                    index_type = "contract number", 
                    index_id = "contract_revision", 
                    is_primary=0
                ),
                primary_entity=False, 
                link = "contract", 
                link_details={"amount": "award_amount"}
            )
        ],
    )
}
