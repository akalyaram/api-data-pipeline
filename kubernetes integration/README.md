# FDIC pipeline on Amazon EKS

This is a standalone Kubernetes deployment copy. It does not modify or depend
on the `lambda integration` folder. It runs the FDIC institutions pipeline as
a Kubernetes Job and schedules incremental runs with a CronJob. RDS PostgreSQL
authentication uses short-lived IAM database auth tokens; the Secrets Manager
secret stores connection metadata only, not a database password.

## Prerequisites

- An EKS cluster with `kubectl` configured to use it.
- A PostgreSQL-compatible RDS/Aurora database with IAM database authentication
  enabled.
- A Secrets Manager secret whose JSON `SecretString` has `host`, `port`,
  `username`, and `dbname` fields. Do not include a password. The port can be
  a JSON number or string.
- An EKS IAM role configured for this Kubernetes service account using IRSA,
  or an EKS Pod Identity association.
- Network connectivity from the scheduled pods to RDS on TCP 5432, Secrets
  Manager, and the public FDIC API over HTTPS. Private subnets require
  appropriate VPC endpoints and/or outbound internet access such as NAT.
- The PostgreSQL user named in the secret must be configured for IAM auth
  (grant `rds_iam`) and have the needed schema/table permissions. The initial
  run creates its tables/indexes unless they have already been provisioned.

## IAM role permissions

The IAM role associated with the Kubernetes service account needs
`secretsmanager:GetSecretValue` on the connection metadata secret and
`rds-db:connect` on the exact database user resource. For Aurora PostgreSQL:

```text
arn:aws:rds-db:us-east-2:ACCOUNT_ID:dbuser:DB_CLUSTER_RESOURCE_ID/DB_USERNAME
```

Use the cluster resource ID, not the database endpoint. For an RDS instance,
use its DB resource ID instead. The EKS cluster's OIDC provider must be
associated with IAM when using IRSA.

The sample `service-account.yaml` uses an IRSA role annotation. Replace
`ACCOUNT_ID` and the role name, then apply it:

```powershell
kubectl apply -f .\service-account.yaml
```

With EKS Pod Identity instead, create the Pod Identity association for
namespace `default`, service account `fdic-pipeline`, and the IAM role. In that
case remove the IRSA annotation from the service account manifest. Do not
configure both methods for the same service account.

## Build and push the image (no local Docker required)

1. In ECR, create a private repository named `fdic-pipeline` in `us-east-2`.
2. Zip the build inputs (so `Dockerfile` and `buildspec.yml` are at the ZIP
   root) and upload the source ZIP to an S3 bucket in the same region. For
   example, from PowerShell in this folder:

   ```powershell
   Compress-Archive -Path .\Dockerfile, .\buildspec.yml, .\fdic_pipeline.py, .\k8s_runner.py, .\requirements.txt -DestinationPath ..\fdic-k8s-source.zip -Force
   ```

   This source ZIP is for CodeBuild, not for Lambda/Kubernetes deployment. Do
   not include existing image build output or dependency folders.
3. In CodeBuild, create a project with:
   - Source: **Amazon S3**, selecting the uploaded source ZIP.
   - Environment image: **Managed image**, Linux, standard image, current
     supported version.
   - Environment type: **Linux**; enable **Privileged** mode so Docker can
     build the container.
   - Buildspec: `buildspec.yml`.
   - A service role with ECR push permissions for the repository. It needs
     `ecr:GetAuthorizationToken` on `*` and `ecr:BatchCheckLayerAvailability`,
     `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`,
     `ecr:PutImage`, and `ecr:BatchGetImage` on the ECR repository.
4. Start the CodeBuild build. It pushes an image tagged with the CodeBuild
   build number and prints `IMAGE_URI=...` in the build log. Copy that full
   image URI. If you change code, upload a new source ZIP and start a new
   build to get a new image tag.

The EKS worker nodes/pods need permission to pull from the private ECR
repository. In the same AWS account, the node IAM role usually needs ECR pull
permissions. Replace `ACCOUNT_ID` and `TAG` in both Kubernetes workload
manifests with the account and tag from the CodeBuild image URI.

## Run a historical load

In `historical-job.yaml`, replace `ACCOUNT_ID`, `TAG`, and `SECRET_ARN`.
The service account must already be associated with its IAM role. Then:

```powershell
kubectl apply -f .\historical-job.yaml
kubectl get jobs,pods
kubectl logs job/fdic-historical -f
```

The Job exits successfully after all records are fetched, batched, and
committed. Inspect logs for page fetch counts, batch write counts, transaction
commit, and completion. On failure, use `kubectl describe job fdic-historical`
and the pod logs. With `backoffLimit: 0`, diagnose and delete/recreate the Job
before retrying:

```powershell
kubectl delete job fdic-historical
```

Historical mode loads all institutions currently published by FDIC; it does
not recreate past versions of the data.

## Schedule incremental loads

After the historical Job succeeds, replace the image and secret placeholders
in `incremental-cronjob.yaml`, then apply it:

```powershell
kubectl apply -f .\incremental-cronjob.yaml
kubectl get cronjobs
```

The example runs daily at 06:00 UTC. `concurrencyPolicy: Forbid` avoids
overlapping scheduled runs. To start a one-off incremental run from the
CronJob:

```powershell
kubectl create job --from=cronjob/fdic-incremental fdic-incremental-manual
kubectl logs job/fdic-incremental-manual -f
```

Incremental mode filters on the maximum stored `DATEUPDT` minus the configured
overlap, then upserts by FDIC record `ID`. Neither mode deletes institutions
that disappear from the feed.

## Settings and database schema

The manifests set `AWS_REGION`, `DB_SECRET_ARN`, `FDIC_PAGE_SIZE`, and
`FDIC_OVERLAP_DAYS`. The runner defaults to incremental mode if `--mode` is
omitted. `LOG_LEVEL` defaults to `INFO`. See `schema.sql` for the equivalent
PostgreSQL DDL; the ORM also creates tables automatically.

The RDS username in the secret must have `rds_iam`. If manually provisioning
the tables instead of granting create rights to the runner, grant the database
user the necessary `SELECT`, `INSERT`, and `UPDATE` privileges on both tables
and the required sequence privileges on `pipeline_runs`.
