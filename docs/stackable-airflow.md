# Stackable Airflow production deployment

## Use two images

Do not set the ETL pipeline image as `spec.image.custom` on `AirflowCluster`.
That field selects the Stackable-compatible Airflow product image and therefore
affects Airflow components and KubernetesExecutor task pods.

Use these separate images instead:

| Image | Contents | Used by |
| --- | --- | --- |
| Stackable Airflow image | Airflow and the Kubernetes provider | Webserver, scheduler, DAG processor and ordinary executor tasks |
| `hybrid-pipeline` image | Trino client, PyArrow, delta-rs, SQL and contracts; no Airflow | Only customer, account and transaction KubernetesPodOperator pods |
| `hybrid-validation` image | dbt-duckdb, models and data tests | Model-scoped validation pod after each ETL stage |

This allows a single Airflow installation to run unrelated Python, sensor,
notification and orchestration tasks without loading the ETL dependencies.
Only DAG tasks that explicitly set `image=HYBRID_PIPELINE_IMAGE` use the pipeline
image.

## Build and publish the pipeline image

```shell
docker build -f Dockerfile.pipeline -t registry.example.com/data/hybrid-pipeline:1.0.0 .
docker push registry.example.com/data/hybrid-pipeline:1.0.0
docker build -f Dockerfile.validation -t registry.example.com/data/hybrid-validation:1.0.0 .
docker push registry.example.com/data/hybrid-validation:1.0.0
```

Use immutable version tags or image digests in production. Do not use `latest`.

## Prepare Stackable Airflow

Use either Stackable's Celery executor or Kubernetes executor for normal Airflow
task execution. KubernetesPodOperator creates a second, dedicated pod only for
tasks that use it. With KubernetesExecutor this means a small Airflow executor
pod launches and monitors the ETL pod; enable deferrable execution if supported
by the installed provider when that overhead matters.

Check whether the Stackable Airflow image already contains the Kubernetes
provider:

```shell
kubectl exec -n airflow <scheduler-pod> -- \
  pip show apache-airflow-providers-cncf-kubernetes
```

If it is missing, extend the Stackable Airflow base image with only that provider
and select the resulting Stackable-compatible image through
`spec.image.custom`. Do not install the ETL dependencies into it.

## Deploy the DAG

1. Copy `airflow/examples/hybrid_transactions_kpo.py` into the DAG repository
   mounted or synchronized by Stackable Airflow.
2. Set `HYBRID_PIPELINE_IMAGE`, `HYBRID_VALIDATION_IMAGE` and optionally
   `HYBRID_PIPELINE_NAMESPACE` on the
   Airflow components that parse and execute DAGs.
3. Update the RoleBinding subject in `kubernetes/rbac.yaml` to the ServiceAccount
   used by Stackable Airflow, then apply it.
4. Create `hybrid-pipeline-secrets` from a secret manager or sealed/encrypted
   manifest. The example Secret is a field reference, not a production secret.
5. Give the `hybrid-pipeline` ServiceAccount workload identity for the writable
   S3 prefix where possible.

```shell
kubectl apply -f kubernetes/rbac.yaml
kubectl apply -f path/to/encrypted-hybrid-pipeline-secret.yaml
```

The ETL and validation pods exchange no dataset rows through XCom. Customer and account SCD2 tables
and date-partitioned transactions are committed to Delta on S3, which remains
the durable task boundary.

They exchange only a small JSON control message containing Delta table URI and pre/post-write
versions. A failed validation pod restores the prior version only when the table is still at the
expected post-write version. This optimistic-concurrency check prevents rollback from overwriting
a newer commit. No dataset rows or Arrow batches pass through XCom.

## Unrelated tasks

Unrelated DAGs remain ordinary TaskFlow, Python, sensor or other operator tasks.
They use the configured Stackable Airflow executor image. A DAG can also mix
ordinary tasks and KubernetesPodOperator tasks:

```text
ordinary validation task
        -> hybrid customer pod -> hybrid account pod -> hybrid transaction pod
        -> ordinary notification task
```

Choose KubernetesPodOperator per task based on isolation, dependencies,
resources and credentials—not globally for every task in Airflow.
