import tempfile
import sys

from sdk.python.tests.unit.cli.cli_repo_creator import CliRunner, get_example_repo

sys.path.append('../../..')

from pathlib import Path
from textwrap import dedent


def run_simple_apply_test(example_repo_file_name: str, expected_error: bytes = None):
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

        repo_example = repo_path / "example.py"
        repo_example.write_text(get_example_repo(example_repo_file_name))
        rc, output = runner.run_with_output(["apply"], cwd=repo_path)

        assert rc != 0 and expected_error in output


run_simple_apply_test("example_feature_repo_persisted_ODFV.py")