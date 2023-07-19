import multiprocessing
import os
import pdb
import time
from datetime import datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from multiprocess import Queue, Process

from feast import FeatureStore, RepoConfig
from feast.errors import FeatureViewNotFoundException
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig
from tests.utils.cli_repo_creator import CliRunner, get_example_repo

def test_odfv() -> None:
    """
    Test reading a basic no persistence ODFV from the online store 
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_ODFV.py"), "file"
    ) as store:
        # Write some data to two tables
        customer_profile_fv = store.get_feature_view(name="customer_profile")

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
        assert "avg_orders_day" in result
        assert "age" in result
        assert result["customer_id"] == ["5"]
        assert result["avg_orders_day"] == [1.0]
        assert result["age"] == [3]

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

def test_online_retrieval_and_update() -> None:
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
        assert "avg_orders_day" in result
        assert "age" in result
        assert result["customer_id"] == ["5"]
        assert result["avg_orders_day"] == [1.0]
        assert result["age"] == [3]

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
        result = store.get_online_features_and_update_online_store(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
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

def test_online_retrieval_and_update_plus() -> None:
    """
    Test reading an ODFV from the online store and updating it in local mode.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_persisted_ODFV.py"), "file"
    ) as store:
        
        input_queue = Queue()
        process = Process(target=store.receive_input_and_run, args=(input_queue,))
        process.start()
        
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

        print("Retrieving two online features")
        start_time = time.time()
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
        end_time = time.time() 
        print("Elapsed time:", end_time - start_time, "seconds")
        assert "avg_orders_day" in result
        assert "age" in result
        assert result["customer_id"] == ["5"]
        assert result["avg_orders_day"] == [1.0]
        assert result["age"] == [3]

        print("Retrieving two on-demand features")
        start_time = time.time()
        result = store.get_online_features(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
        ).to_dict()
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")
        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]

        print("Retrieving and updating two on-demand features")
        start_time = time.time()
        result = store.get_online_features(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
        )
        copy = result
        copy = copy.to_df(include_event_timestamps=False)
        result = result.to_dict()

        input_queue.put(
            [copy,
            [
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            [
                {"customer_id": "5", "customer_inp_1": 1.0},
            ]])
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")

        # Wait a bit for update to reflect in online store
        time.sleep(5)

        assert "cus_specific_avg_orders_day" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]


        print("Retrieve and update only one on-demand persisted feature and one on-demand non-persisted feature")
        start_time = time.time()
        result = store.get_online_features(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating_no_persistence:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
        )
        copy = result
        copy = copy.to_df(include_event_timestamps=False)
        result = result.to_dict()

        input_queue.put(
            [copy,
            [
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating_no_persistence:cus_specific_age",
            ],
            [
                {"customer_id": "5", "customer_inp_1": 1.0},
            ]])
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")
        assert "cus_specific_avg_orders_day" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]

        # # Wait a bit for update to reflect in online store
        # time.sleep(5)

        # print("Retrieve and attempt to update two regular features")
        # start_time = time.time()
        # result = store.get_online_features_and_update_online_store(
        #     features=[
        #         "customer_profile:avg_orders_day",
        #         "customer_profile:age",
        #     ],
        #     entity_rows=[
        #         {"customer_id": "5"},
        #     ],
        #     full_feature_names=False,
        # ).to_dict()
        # end_time = time.time()
        # print("Elapsed time:", end_time - start_time, "seconds")

        print("Retrieve two historical on-demand features")
        start_time = time.time()
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
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")

        assert "cus_specific_avg_orders_day" in result
        assert "cus_specific_age" in result
        assert result["customer_id"] == ["5"]
        assert result["cus_specific_avg_orders_day"] == [2.0]
        assert result["cus_specific_age"] == [4]

        input_queue.put('exit')
        process.join()

