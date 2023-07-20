import time
import warnings

from datetime import datetime
from pandas.testing import assert_frame_equal

from multiprocess import Queue, Process

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

from tests.utils.cli_repo_creator import CliRunner, get_example_repo

def run_demo() -> None:
    """
    Demo that exhibits full capabilities of get_online_features_and_update_online_store()!!!
    """
    warnings.filterwarnings("ignore")
    
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_persisted_ODFV.py"), "file"
    ) as store:
        
        input_queue = Queue()
        process = Process(target=store.receive_update_on_demand_feature_view_reqs_and_run, args=(input_queue,))
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

        print("\n--- Retrieving two online features ---")
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
        for key, value in sorted(result.items()):
            print(key, " : ", value)

        print("\n--- Retrieving two on-demand features ---")
        start_time = time.time()
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
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")
        for key, value in sorted(result.items()):
            print(key, " : ", value)

        print("\n--- Retrieving and updating two on-demand features ---")
        start_time = time.time()
        result = store.get_online_features_and_update_online_store(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ], 
            queue=input_queue
        ).to_dict()
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")
        for key, value in sorted(result.items()):
            print(key, " : ", value)

        # Wait a bit for update to reflect in online store
        time.sleep(5)

        print("\n--- Retrieve and update only one on-demand persisted feature and one on-demand non-persisted feature ---")
        start_time = time.time()
        result = store.get_online_features_and_update_online_store(
            features=[
                "transformed_customer_rating:cus_specific_avg_orders_day",
                "transformed_customer_rating_no_persistence:cus_specific_age",
            ],
            entity_rows=[
                {"customer_id": "5", "customer_inp_1": 1.0},
            ],
            queue=input_queue
        ).to_dict()
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")
        for key, value in sorted(result.items()):
            print(key, " : ", value)

        # Wait a bit for update to reflect in online store
        time.sleep(5)

        print("\n--- Retrieve and attempt to update two regular features ---")
        start_time = time.time()
        result = store.get_online_features_and_update_online_store(
            features=[
                "customer_profile:avg_orders_day",
                "customer_profile:age",
            ],
            entity_rows=[
                {"customer_id": "5"},
            ],
            full_feature_names=False,
            queue=input_queue
        ).to_dict()
        end_time = time.time()
        print("Elapsed time:", end_time - start_time, "seconds")

        print("\n--- Retrieve two historical on-demand features ---")
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
        for key, value in sorted(result.items()):
            print(key, " : ", value)

        input_queue.put('exit')
        process.join()

if __name__ == "__main__":
    run_demo()