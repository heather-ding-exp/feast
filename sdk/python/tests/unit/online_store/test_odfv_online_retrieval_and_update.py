import os
import pdb
import time
from datetime import datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feast import FeatureStore, RepoConfig
from feast.errors import FeatureViewNotFoundException
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig
from tests.utils.cli_repo_creator import CliRunner, get_example_repo

#@pytest.mark.my_marker
def test_online() -> None:
    """
    Test reading an ODFV from the online store and updating it in local mode.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_persisted_ODFV.py"), "file"
    ) as store:
        
        # Write some data to two tables
        customer_profile_fv = store.get_feature_view(name="customer_profile")
        transformed_customer_rating_fv = store.get_feature_view(name = "transformed_customer_rating_fv")

        provider = store._get_provider()

        customer_key = EntityKeyProto(
            join_keys=["customer_id"], entity_values=[ValueProto(string_val="5")]
        )
        provider.online_write_batch(
            config=store.config,
            table=customer_profile_fv,
            data=[
                (
                    customer_key,
                    {
                        "avg_orders_day": ValueProto(float_val=1.0),
                        "name": ValueProto(string_val="John"),
                        "age": ValueProto(int64_val=3),
                    },
                    datetime.utcnow(),
                    datetime.utcnow(),
                )
            ],
            progress=None,
        )
        provider.online_write_batch(
            config=store.config,
            table=transformed_customer_rating_fv,
            data=[
                (
                    customer_key,
                    {
                        "cus_specific_avg_orders_day": ValueProto(float_val=0.0),
                        "cus_specific_name": ValueProto(string_val="John"),
                        "cus_specific_age": ValueProto(int64_val=0),
                    },
                    datetime.utcnow(),
                    datetime.utcnow(),
                )
            ],
            progress=None,
        )

        # Retrieve two features, 
        result = store.get_online_features(
            features=[
                "customer_profile:avg_orders_day",
                "customer_profile:age",
            ],
            entity_rows=[
                {"customer_id": "5"},
            ],
            full_feature_names=False,
        ).to_dict()
        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [1.0]
        assert result["cus_specific_age"] == [3]

        # Retrieve two on-demand features 
        result = store.get_online_features(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
            full_feature_names=False,
        ).to_dict()
        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]

        # Retrieve and update two on-demand features 
        result = store.get_online_features_and_update(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
            full_feature_names=False,
        ).to_dict()
        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]

        # Wait a bit for update to reflect in online store
        time.sleep(5)

        # Retrieve two recently updated online features 
        result = store.get_online_features(
            features=[
                "transformed_customer_rating_fv:cus_specific_avg_orders_day",
                "transformed_customer_rating_fv:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5"},
            ],
            full_feature_names=False,
        ).to_dict()
        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]




        # # Create new FeatureStore object with fast cache invalidation
        # cache_ttl = 1
        # fs_fast_ttl = FeatureStore(
        #     config=RepoConfig(
        #         registry=RegistryConfig(
        #             path=store.config.registry.path, cache_ttl_seconds=cache_ttl
        #         ),
        #         online_store=store.config.online_store,
        #         project=store.project,
        #         provider=store.config.provider,
        #         entity_key_serialization_version=2,
        #     )
        # )

        # # Should download the registry and cache it permanently (or until manually refreshed)
        # result = fs_fast_ttl.get_online_features(
        #     features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #     ],
        #     entity_rows=[
        #         {"customer_id": "5"},
        #     ],
        #     full_feature_names=False,
        # ).to_dict()

        # # Rename the registry.db so that it cant be used for refreshes
        # os.rename(store.config.registry.path, store.config.registry.path + "_fake")

        # # Wait for registry to expire
        # time.sleep(cache_ttl)

        # # Will try to reload registry because it has expired (it will fail because we deleted the actual registry file)
        # with pytest.raises(FileNotFoundError):
        #     fs_fast_ttl.get_online_features(
        #         features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #         ],
        #         entity_rows=[
        #             {"customer_id": "5"},
        #         ],
        #         full_feature_names=False,
        #     ).to_dict()

        # # Restore registry.db so that we can see if it actually reloads registry
        # os.rename(store.config.registry.path + "_fake", store.config.registry.path)

        # # Test if registry is actually reloaded and whether results return
        # result = fs_fast_ttl.get_online_features(
        #     features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #     ],
        #     entity_rows=[
        #         {"customer_id": "5"},
        #     ],
        #     full_feature_names=False,
        # ).to_dict()
    
        # # Create a registry with infinite cache (for users that want to manually refresh the registry)
        # fs_infinite_ttl = FeatureStore(
        #     config=RepoConfig(
        #         registry=RegistryConfig(
        #             path=store.config.registry.path, cache_ttl_seconds=0
        #         ),
        #         online_store=store.config.online_store,
        #         project=store.project,
        #         provider=store.config.provider,
        #         entity_key_serialization_version=2,
        #     )
        # )

        # # Should return results (and fill the registry cache)
        # result = fs_infinite_ttl.get_online_features(
        #     features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #     ],
        #     entity_rows=[
        #         {"customer_id": "5"},
        #     ],
        #     full_feature_names=False,
        # ).to_dict()

        # # Wait a bit so that an arbitrary TTL would take effect
        # time.sleep(2)

        # # Rename the registry.db so that it cant be used for refreshes
        # os.rename(store.config.registry.path, store.config.registry.path + "_fake")

        # # TTL is infinite so this method should use registry cache
        # result = fs_infinite_ttl.get_online_features(
        #     features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #     ],
        #     entity_rows=[
        #         {"customer_id": "5"},
        #     ],
        #     full_feature_names=False,
        # ).to_dict()

        # # Force registry reload (should fail because file is missing)
        # with pytest.raises(FileNotFoundError):
        #     fs_infinite_ttl.refresh_registry()

        # # Restore registry.db so that teardown works
        # os.rename(store.config.registry.path + "_fake", store.config.registry.path)
