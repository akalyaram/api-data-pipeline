# FDIC pipeline for AWS Lambda and RDS PostgreSQL

This folder is a separate Lambda-ready copy of the local pipeline. It connects
to PostgreSQL on Amazon RDS using RDS IAM database authentication. Secrets
Manager stores connection metadata only; the database password is never stored
or fetched. It does not use the SQLite database from the project root.

## Secret and Lambda configuration

Set the Lambda environment variables:

- `DB_SECRET_ARN`: ARN of a Secrets Manager secret whose `SecretString` is JSON
  with `host`, `port`, `username`, and `dbname` fields. Do not include a
  password. `port` defaults to `5432`.
- `AWS_REGION`: region containing the secret and database (Lambda normally
  provides this variable).
- Optional: `FDIC_API_URL`, `FDIC_PAGE_SIZE` (default `1000`), and
  `FDIC_OVERLAP_DAYS` (default `7`).

The Lambda execution role needs `secretsmanager:GetSecretValue` on that secret
and `rds-db:connect` for the database user. For Aurora PostgreSQL, the
`rds-db:connect` resource ARN format is:

```text
arn:aws:rds-db:REGION:ACCOUNT_ID:dbuser:DB_CLUSTER_RESOURCE_ID/DB_USER
```

For an RDS PostgreSQL instance, use its DB resource ID instead of the cluster
resource ID. These are the resource IDs (such as `cluster-...` or `db-...`),
not the endpoint hostnames. Attach the policy to the Lambda execution role.
Enable IAM database authentication for the RDS database/cluster.

Example Lambda execution-role policy for an Aurora cluster in `us-east-2`
(replace the account, cluster resource ID, and database username):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": "arn:aws:rds-db:us-east-2:123456789012:dbuser:cluster-ABCDEFGHIJKL01234/fdic_lambda_user"
    }
  ]
}
```

Create a PostgreSQL login for the same username used in the secret and grant
it the IAM role and required database access. For example, connect as a
database administrator and run, adjusting the username/database/schema to
match your configuration:

```sql
CREATE USER fdic_lambda_user;
GRANT rds_iam TO fdic_lambda_user;
GRANT CONNECT, CREATE ON DATABASE postgres TO fdic_lambda_user;
GRANT USAGE, CREATE ON SCHEMA public TO fdic_lambda_user;
```

If the user should not own/create tables, provision the tables and grant
appropriate `SELECT`, `INSERT`, and `UPDATE` permissions instead.

Configure the Lambda in the RDS VPC, with subnets and security groups that can
reach the database on port 5432. The RDS security group must allow inbound
PostgreSQL traffic from the Lambda security group. The connection enforces TLS
with `sslmode=require`.

The connection metadata is retrieved from Secrets Manager when the Lambda
execution environment first needs a database connection. SQLAlchemy generates
a new IAM authentication token whenever it opens a physical database
connection; it does not keep an expired token in the connection URL. Recycled
connections also receive a fresh token. The engine is reused by later
invocations in that environment.

## Deploy

Use a supported Lambda Python runtime (for example Python 3.12) and set the
handler to `handler.lambda_handler`. Build the package for the Lambda Linux
runtime, not Windows, so the PostgreSQL driver is compatible. For example,
from PowerShell in this folder with Docker Desktop running:

```powershell
docker run --rm --entrypoint /bin/sh -v "${PWD}:/var/task" public.ecr.aws/lambda/python:3.12 -c "python -m pip install -r /var/task/requirements.txt -t /var/task/package"
Copy-Item handler.py,fdic_pipeline.py -Destination package
Compress-Archive -Path package\* -DestinationPath fdic-lambda.zip -Force
```

Upload `fdic-lambda.zip` as the function code. The archive contents must place `handler.py`, `fdic_pipeline.py`, boto3,
SQLAlchemy, requests, and psycopg2 at its root. The function must have outbound
HTTPS access to the FDIC API as well as RDS network access. Use a NAT gateway
or another suitable egress path if the function is in private subnets without
internet access.

## Invoke

The event selects the load mode:

```json
{"mode": "historical"}
```

```json
{"mode": "incremental"}
```

If `mode` is omitted, the function defaults to `incremental`. Historical mode
loads all currently published institutions, not prior versions of their
records. Incremental mode filters by the greatest stored `DATEUPDT` minus the
configured overlap, then upserts by FDIC record `ID`. Neither mode deletes
records that disappear from the feed. An empty database's first incremental
run fetches the full dataset.

Institution writes are sent to PostgreSQL in batches of 500 to avoid a
round-trip for every bank. Batch progress is written to the Lambda log stream
as each page is fetched and each batch is executed. CloudWatch Logs also mark
schema initialization, fetch completion, upsert start, transaction commit, and
run completion or failure phase. Batch messages say "pending transaction
commit"; records are still committed together once all batches execute.
The handler explicitly enables INFO-level logging so the progress messages
appear with the Lambda runtime's default logging configuration. Monitor the
function under **Monitor → View CloudWatch logs** to see the current phase and
record counts.

The SQLAlchemy models create tables automatically. `schema.sql` contains the
equivalent PostgreSQL DDL for manual provisioning. The full FDIC source record
is retained in `fdic_institutions.source_record`; run status and failures are
recorded in `pipeline_runs`.

## Local tests

Install `requirements.txt` into a Python environment, then run:

```powershell
python -m unittest -v
```

The tests use in-memory SQLite and mocked AWS/API calls; they do not require an
AWS account or a live RDS database. Actual PostgreSQL connectivity and IAM
policy/resource configuration must be verified in the deployed VPC
environment.
