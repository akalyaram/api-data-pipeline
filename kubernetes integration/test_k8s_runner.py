import json
import sys
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine

import k8s_runner


class KubernetesRunnerTests(unittest.TestCase):
    def test_secret_parses_string_port_and_omits_password(self):
        secret = {
            "host": "database.example.test",
            "port": "5432",
            "username": "fdic_user",
            "password": "must-not-be-used",
            "dbname": "postgres",
        }
        boto3 = Mock()
        boto3.client.return_value.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }

        with patch.dict(sys.modules, {"boto3": boto3}):
            result = k8s_runner._read_database_secret("secret-arn", "us-east-2")

        self.assertEqual(result, {
            "host": "database.example.test",
            "port": 5432,
            "username": "fdic_user",
            "dbname": "postgres",
        })
        self.assertNotIn("password", result)

    def test_secret_rejects_non_integral_port(self):
        secret = {
            "host": "database.example.test",
            "port": 5432.5,
            "username": "fdic_user",
            "dbname": "postgres",
        }
        boto3 = Mock()
        boto3.client.return_value.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }

        with patch.dict(sys.modules, {"boto3": boto3}):
            with self.assertRaisesRegex(ValueError, "port must be an integer"):
                k8s_runner._read_database_secret("secret-arn", "us-east-2")

    def test_engine_generates_iam_token_on_connect(self):
        secret = {
            "host": "database.example.test",
            "port": 5432,
            "username": "fdic_user",
            "dbname": "postgres",
        }
        secrets_client = Mock()
        secrets_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }
        rds_client = Mock()
        rds_client.generate_db_auth_token.return_value = "temporary-token"
        boto3 = Mock()
        boto3.client.side_effect = [secrets_client, rds_client]
        engine = create_engine("sqlite:///:memory:")
        callbacks = []
        original_listen = k8s_runner.event.listen

        def capture_listener(target, name, callback, **kwargs):
            callbacks.append(callback)
            original_listen(target, name, callback, **kwargs)

        try:
            with (
                patch.dict(sys.modules, {"boto3": boto3}),
                patch.object(k8s_runner, "create_engine", return_value=engine) as create,
                patch.object(k8s_runner.event, "listen", side_effect=capture_listener),
            ):
                returned_engine = k8s_runner.create_rds_engine("secret-arn", "us-east-2")

            url = create.call_args.args[0]
            self.assertIs(returned_engine, engine)
            self.assertEqual(url.drivername, "postgresql+psycopg2")
            self.assertIsNone(url.password)
            self.assertEqual(url.database, "postgres")

            connection_parameters = {}
            callbacks[0](None, None, [], connection_parameters)
            self.assertEqual(connection_parameters["password"], "temporary-token")
            rds_client.generate_db_auth_token.assert_called_once_with(
                DBHostname="database.example.test",
                Port=5432,
                DBUsername="fdic_user",
            )
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
