# infra — out-of-process nightly rebuild

Implements [WXYC/semantic-index#347](https://github.com/WXYC/semantic-index/issues/347). The nightly graph rebuild no longer runs in the API container (where it OOM-killed under the `--memory 1g` cap, taking the API down — canary #50). Instead it runs as an on-demand **ECS Fargate task in the Backend-Service VPC** with an adequate memory budget, and the rebuilt `wxyc_artist_graph.db` is shipped back to the serving host and atomically swapped in **without an API restart**.

See `plans/si-out-of-process-rebuild/plan.md` (in the `wxyc-workspace` meta-repo) for the full design.

## Pieces

| Where | What |
|---|---|
| `build-job.yaml` | CloudFormation: S3 transfer bucket, ECS cluster, run-to-completion Fargate task def (reuses the `semantic-index` image, command → `scripts/run_build_job.py`), IAM roles, egress SG, log group. |
| `deploy.sh` | `aws cloudformation deploy` wrapper; reads `build-job.conf`. |
| `build-job.conf.example` | Template for the BS-VPC IDs (`build-job.conf` is gitignored). |
| `scripts/run_build_job.py` | Container entrypoint: S3 seed → `nightly_sync` → S3 build. |
| `scripts/validate_graph_db.py` | Pre-swap validation gate (header + artist + enrichment-preservation). |
| `scripts/ec2-build-conductor.sh` | Nightly driver on the EC2 host (snapshot → run-task → validate → swap). |
| `deploy/semantic-index-build.{service,timer}` | systemd units that run the conductor. |

## Account / region

WXYC account **`203767826763`** (`AWS_PROFILE=wxyc-api`), **us-east-1** — the same account/VPC as the BS EC2 host and the `wxyc-db` RDS instance. **Not** the personal `503977661500` account.

## Deploy

```bash
cp infra/build-job.conf.example infra/build-job.conf   # then fill in VPC_ID
AWS_PROFILE=wxyc-api ./infra/deploy.sh
```

`deploy.sh` prints the stack outputs (`ClusterArn`, `TaskDefinitionArn`, `BuildSecurityGroupId`, `BucketName`, `TaskRoleArn`, `TaskExecutionRoleArn`) used by the out-of-band steps below.

## Out-of-band, one-time setup

These touch resources the stack does not own, so they are applied by hand (all `AWS_PROFILE=wxyc-api`).

### 1. RDS security-group inbound rule — the single must-do networking step

Without this the task connects nowhere. Find the RDS SG, then allow 5432 from `BuildSecurityGroupId`:

```bash
RDS_SG=$(aws rds describe-db-instances --db-instance-identifier wxyc-db \
  --query 'DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId' --output text)
BUILD_SG=<BuildSecurityGroupId from stack outputs>
aws ec2 authorize-security-group-ingress --group-id "$RDS_SG" \
  --protocol tcp --port 5432 --source-group "$BUILD_SG"
```

### 2. Database DSN as a SecureString

```bash
aws ssm put-parameter --name /semantic-index/database-url-backend \
  --type SecureString \
  --value "postgresql://<user>:<pass>@wxyc-db.cc7ob5nnabjm.us-east-1.rds.amazonaws.com:5432/<db>"
```

Use the **private** RDS endpoint (the task is in-VPC). This is the value already in `.env.semantic-index` as `DATABASE_URL_BACKEND`.

### 3. EC2 instance-profile additions (for the conductor)

The conductor runs on the EC2 host under the `wxyc-ec2-backend` instance profile. Attach an inline policy granting it the S3 round-trip + `ecs:RunTask`/`DescribeTasks` + scoped `iam:PassRole`. Substitute the stack-output ARNs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::wxyc-semantic-index-build/*" },
    { "Effect": "Allow", "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::wxyc-semantic-index-build" },
    { "Effect": "Allow",
      "Action": ["ecs:RunTask", "ecs:DescribeTasks"],
      "Resource": "*",
      "Condition": { "ArnEquals": { "ecs:cluster": "<ClusterArn>" } } },
    { "Effect": "Allow", "Action": "iam:PassRole",
      "Resource": ["<TaskRoleArn>", "<TaskExecutionRoleArn>"] }
  ]
}
```

## Proving + cutover

1. **Dry run.** Launch the task by hand and watch CloudWatch Logs `/ecs/semantic-index-build` (this is the first time the full `[mem]` profile past `after _load_from_pg` is ever observed):
   ```bash
   aws ecs run-task --cluster semantic-index-build --task-definition semantic-index-build \
     --launch-type FARGATE \
     --network-configuration "awsvpcConfiguration={subnets=[<SUBNET>],securityGroups=[<BUILD_SG>],assignPublicIp=ENABLED}"
   ```
   Download `s3://wxyc-semantic-index-build/build/wxyc_artist_graph.db` and run `validate_graph_db.py` — **do not swap yet**.
2. Install the conductor + timer on EC2 (see `deploy/`), let it run the full round-trip for ≥2 nights; confirm prod mtime advances and enrichment persists.
3. Set `SYNC_ENABLED=false` in `.env.semantic-index` and **recreate** the container (`docker restart` does not re-read env).
4. Over ≥2 further nights confirm mtime keeps advancing and canary #50 stops firing on the 09:00 window.
