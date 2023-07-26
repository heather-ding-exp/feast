from datetime import timedelta
import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource, RequestSource
from feast.types import Float32, Int64, String

from feast import OnDemandFeatureView
from feast.types import Float64
from feast.value_type import ValueType
from feast.on_demand_feature_view import on_demand_feature_view

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
    value_type=ValueType.STRING,
)


customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_orders_day", dtype=Float32),
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
    ],
)

@on_demand_feature_view(
    sources=[customer_profile, input_request],
    schema=[
        Field(name="cus_specific_avg_orders_day", dtype=Float64),
        Field(name="cus_specific_age", dtype=Int64),
    ],
)

def transformed_customer_rating(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["cus_specific_avg_orders_day"] = inputs["avg_orders_day"] + inputs["customer_inp_1"]
    df["cus_specific_age"] = inputs["age"] + 1
    return df

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features


all_customers_feature_service = FeatureService(
    name="customers_service",
    features=[customer_profile],
    tags={"release": "production"},
)
