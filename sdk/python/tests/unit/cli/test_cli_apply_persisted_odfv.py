from datetime import timedelta
import tempfile
from pathlib import Path
from textwrap import dedent

import pandas as pd
from feast.entity import Entity
from feast.field import Field
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.value_type import ValueType
from feast.data_source import RequestSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.types import Float32, Float64, Int64, String
from feast.feature_store import FeatureStore

from tests.utils.cli_repo_creator import CliRunner, get_example_repo


def test_apply_persisted_odfv() -> None:
    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:
        runner = CliRunner()
        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)
        data_path = Path(data_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                    f"""
            project: foo
            registry: {data_path / "registry.db"}
            provider: local
            online_store:
                path: {data_path / "online_store.db"}
            """
                )
            )
        store = FeatureStore(repo_path=repo_path, fs_yaml_file=repo_config)

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
        def transformed_customer_rating_udf(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["cus_specific_avg_orders_day"] = inputs["avg_orders_day"] + inputs["customer_inp_1"]
            df["cus_specific_age"] = inputs["age"] + 1
            #df["customer_id"] = inputs["customer_id"] 
            return df
        # Define an on demand feature view which can generate new features based on
        # existing feature views and RequestSource features
        transformed_customer_rating = OnDemandFeatureView(
            name = "transformed_customer_rating",
            sources=[customer_profile, input_request],
            schema=[
                Field(name="cus_specific_avg_orders_day", dtype=Float64),
                Field(name="cus_specific_age", dtype=Int64),
                Field(name="customer_id", dtype=String),
            ],
            udf=transformed_customer_rating_udf,
            udf_string="transformed customer rating",
            persist=True, 
            entities = [customer],
            feature_view_name = "transformed_customer_rating_fv",
            push_source_name = "transformed_customer_rating_ps",
            batch_source = customer_profile_source
        )
        store.apply([customer_profile_source, customer, customer_profile, input_request, transformed_customer_rating])
        assert "transformed_customer_rating_fv" and "customer_profile" in [fv.name for fv in store.list_feature_views()]
        assert "transformed_customer_rating_ps" in [ps.name for ps in store.list_data_sources()]
        assert "transformed_customer_rating" in [odfv.name for odfv in store.list_on_demand_feature_views()]

def test_cli_apply_persisted_odfv() -> None:
    """
    Tests that applying a persisted on demand feature view applies corresponding feature view, push source, and batch source properly. 
    """
    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:
        runner = CliRunner()
        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)
        data_path = Path(data_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: {data_path / "registry.db"}
        provider: local
        online_store:
            path: {data_path / "online_store.db"}
        """
            )
        )

        # Import feature view from an existing file so it exists in two files.
        repo_example = repo_path / "example.py"
        repo_example.write_text(
            get_example_repo("example_feature_repo_persisted_ODFV.py")
        )

        rc, output = runner.run_with_output(["apply"], cwd=repo_path)

        print(rc, output)
        #assert rc == 0
        store = FeatureStore(repo_path=repo_path, fs_yaml_file=repo_config)
        assert "transformed_customer_rating_fv" and "customer_profile" in [fv.name for fv in store.list_feature_views()]
        assert "transformed_customer_rating_ps" in [ps.name for ps in store.list_data_sources()]
        
