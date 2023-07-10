from datetime import timedelta
import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource, RequestSource
from feast.types import Float32, Int64, String

from sdk.python.feast import on_demand_feature_view
from sdk.python.feast.types import Float64

# Note that file source paths are not validated, so there doesn't actually need to be any data
# at the paths for these file sources. Since these paths are effectively fake, this example
# feature repo should not be used for historical retrieval.


customer_profile_source = FileSource(
    name="customer_profile_source",
    path="data/customer_profiles.parquet",
    timestamp_field="event_timestamp",
)


customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_keys=["customer_id"],
)


customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_orders_day", dtype=Float32),
        Field(name="name", dtype=String),
        Field(name="age", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_profile_source,
    tags={},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="customer_inp",
    schema=[
        Field(name="customer_inp_1", dtype=Float32),
        Field(name="customer_inp_2", dtype=Int64),
    ],
)

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[customer_profile, input_request],
    schema=[
        Field(name="cus_specific_avg_orders_day", dtype=Float32),
        Field(name="cus_specific_name", dtype=String),
        Field(name="cus_specific_age", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    entities = [customer],
    feature_view_name = "transformed_customer_rating_fv",
    push_source_name = "transformed_customer_rating_ps",
    batch_source = customer_profile_source
)

def transformed_customer_rating(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["cus_specific_avg_orders_day"] = inputs["avg_orders_day"] + inputs["customer_inp_1"]
    df["cus_specific_name"] = inputs["name"]
    df["cus_specific_age"] = inputs["age"] + inputs["customer_inp_2"]
    df["customer_id"] = inputs["customer_id"] 
    return df


all_customers_feature_service = FeatureService(
    name="customers_service",
    features=[customer_profile],
    tags={"release": "production"},
)
