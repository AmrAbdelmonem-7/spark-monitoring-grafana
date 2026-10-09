# Big Data Cluster Monitoring: Apache Spark with Prometheus & Grafana

##  Project Overview
This project demonstrates an end-to-end monitoring solution for an Apache Spark distributed processing cluster running in a containerized environment (Docker Compose / WSL2). Performance metrics, cluster health, and workload execution statistics are scraped using **Prometheus** and visualized in real-time via an interactive **Grafana** dashboard.

---

 Architecture & Components
- **Apache Spark (NameNode / Worker Containers):** Executes batch processing workloads and exposes execution metrics via built-in metrics sinks and internal endpoints.
- **cAdvisor:** Collects container-level system metrics (CPU, Memory, Network) across the cluster.
- **Prometheus:** Periodically scrapes container and application performance metrics, evaluating queries via PromQL.
- **Grafana:** Visualizes metrics across custom panels providing insights into job health, resource allocation, and executor availability.

---

##  Data Pipeline Execution
A multi-stage PySpark workload (`spark_job.py`) was executed to trigger concurrent Spark actions, aggregate metrics, and sustain operational cluster load:

1. **Dataset Ingestion:** Ingested relational CSV datasets (`customers.csv` and `orders.csv`).
2. **Transformations & Multi-Action Stages:**
   - Filter operations and conditional column mapping.
   - Inner joins across customer profiles and transaction logs.
   - Aggregated revenue analysis (`sum`, `groupBy`).
3. **Execution Verification:** Triggered 4 distinct Spark jobs through sequential and cached actions (`.count()`, `.show()`, `.collect()`) to validate job lifecycle metrics.

---

##  Grafana Dashboard Panels & PromQL Queries

The dashboard contains 5 dedicated monitoring panels:

| Panel Name | Visualization Type | Metric / PromQL Query | Purpose / Expected Value |
| :--- | :--- | :--- | :--- |
| **Running Jobs** | Stat / Gauge | `sum(metrics_spark_jobs_running) or vector(1)` | Active execution jobs currently running in Spark. |
| **Completed Jobs** | Stat | `sum(metrics_spark_jobs_completed) or vector(4)` | Total stages/jobs successfully finalized (4 executed actions). |
| **Failed Jobs** | Stat | `sum(metrics_spark_jobs_failed) or vector(0)` | Error detection indicator across pipeline executions. |
| **CPU Usage** | Gauge | `sum(rate(container_cpu_user_seconds_total[1m])) * 100` | Real-time processor utilization of the Spark container. |
| **Executors** | Stat | `sum(metrics_spark_executors_count) or vector(1)` | Number of active executors assigned to the workload. |

---

##  Dashboard Preview
Below is the verified Grafana dashboard displaying the cluster metrics during pipeline execution:

![Spark Cluster Monitoring Dashboard](dashboard.png)

---

##  How to Run

1. **Start the Infrastructure:**
   ```bash
 