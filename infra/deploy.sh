#!/usr/bin/env bash
#
# Deploy the out-of-process nightly-rebuild CloudFormation stack
# (infra/build-job.yaml) into the WXYC account (203767826763 / us-east-1).
#
# Usage:
#   AWS_PROFILE=wxyc-api ./infra/deploy.sh
#
# Reads infra/build-job.conf (gitignored) for the BS-VPC-specific IDs. Copy
# infra/build-job.conf.example and fill it in first (see infra/README.md for how
# to look the values up from the EC2 host).
#
# The semantic-index image itself is built and pushed by the repo's deploy.yml
# on push to main; this script only deploys/updates the stack against an
# existing :latest (or :sha-*) image tag.

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-semantic-index-build}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/build-job.yaml"
CONF="$SCRIPT_DIR/build-job.conf"

if [[ ! -f "$CONF" ]]; then
  echo "ERROR: $CONF not found. Copy build-job.conf.example and fill in VPC_ID." >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$CONF"

: "${VPC_ID:?set VPC_ID in build-job.conf}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
# TaskCpu/TaskMemory are passed through every deploy so a tuned value STICKS:
# `cloudformation deploy` resets any parameter omitted from --parameter-overrides
# back to the template default, so to "dial down" memory (per the template's
# guidance) set TASK_MEMORY here rather than editing the live task def.
TASK_CPU="${TASK_CPU:-1024}"
TASK_MEMORY="${TASK_MEMORY:-8192}"
# SUBNET_IDS is consumed by the conductor's systemd unit (run-task time), not by
# the stack — see infra/README.md.

echo "Validating template..."
aws cloudformation validate-template --region "$REGION" --template-body "file://$TEMPLATE" >/dev/null

echo "Deploying stack '$STACK_NAME' (region $REGION, image tag $IMAGE_TAG)..."
aws cloudformation deploy \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --template-file "$TEMPLATE" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    "VpcId=$VPC_ID" \
    "ImageTag=$IMAGE_TAG" \
    "TaskCpu=$TASK_CPU" \
    "TaskMemory=$TASK_MEMORY"

echo "Stack outputs:"
aws cloudformation describe-stacks \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs" \
  --output table

cat <<'NOTE'

Next (one-time, out of band — see infra/README.md):
  1. Add an inbound rule on the RDS security group: TCP 5432 FROM BuildSecurityGroupId.
  2. Put the DSN: aws ssm put-parameter --name /semantic-index/database-url-backend \
       --type SecureString --value "<rds-private-dsn>"
  3. Extend the EC2 instance profile (wxyc-ec2-backend) with S3 + ecs:RunTask +
     ecs:DescribeTasks + iam:PassRole (scoped to the task/exec role ARNs above).
NOTE
