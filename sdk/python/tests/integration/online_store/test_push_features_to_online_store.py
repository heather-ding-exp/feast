
import datetime

import pandas as pd
import pytest
from feast.data_source import RequestSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.types import Float32, Int32, Int64

from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import location


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_push_features_and_read(environment, universal_data_sources):
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    location_fv = feature_views.pushed_locations
    store.apply([location(), location_fv])

    data = {
        "location_id": [1],
        "temperature": [4],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_ingest = pd.DataFrame(data)

    store.push("location_stats_push_source", df_ingest)

    online_resp = store.get_online_features(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 1}],
    )
    online_resp_dict = online_resp.to_dict()
    assert online_resp_dict["location_id"] == [1]
    assert online_resp_dict["temperature"] == [4]


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_get_ondemandfeatures_and_push(environment, universal_data_sources):
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    location_fv = feature_views.pushed_locations
    store.apply([location(), location_fv])
    
    # Define an odfv
    def udf1(features_df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["augmented_temperature"] = features_df["temperature"] + 2
        return df

    on_demand_location_feature_view = OnDemandFeatureView(
        name="location_stats_odfv",
        sources=[location_fv],
        schema=[
            Field(name="location_id", dtype=Int32),
            Field(name="augmented_temperature", dtype=Int64),
        ],
        udf=udf1,
        udf_string="udf1 source code",
        persist=True, 
        entities = [location()],
        feature_view_name="on_demand_feature_view_1_fv",
        push_source_name="on_demand_feature_view_1_ps",
        batch_source = location_fv.batch_source,
    )
    store.apply([on_demand_location_feature_view])

    # Push some initial data to the online store
    data = {
        "location_id": [1],
        "temperature": [4],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_ingest = pd.DataFrame(data)
    store.push("location_stats_push_source", df_ingest)

    # Get on-demand features
    result = store.get_online_features(
            features=[
                "location_stats_odfv:location_id",
                "location_stats_odfv:temperature",
            ],
            entity_rows=[
                {"location_id": 1},
            ],
            full_feature_names=False,
        ).to_dict()
    
    assert result["location_id"] == [1]
    assert result["temperature"] == [6]

    # Get and update on-demand features
    result = store.get_online_features_and_update(
            features=[
                "location_stats_odfv:location_id",
                "location_stats_odfv:temperature",
            ],
            entity_rows=[
                {"location_id": 1},
            ],
        ).to_dict()
    assert result["location_id"] == [1]
    assert result["temperature"] == [6]

    # Get persisted on-demand features
    result = store.get_online_features(
            features=[
                "on_demand_feature_view_1_fv:location_id",
                "on_demand_feature_view_1_fv:temperature",
            ],
            entity_rows=[
                {"location_id": 1},
            ],
            full_feature_names=False,
        ).to_dict()
    
    assert result["location_id"] == [1]
    assert result["temperature"] == [6]

