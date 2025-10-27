# Tools

Python utilities for managing the big data stack. Run with `uv run tools/<script>.py`.

## Setup
- `uv sync` in project root to install deps.
- Copy .env.example to .env and customize.

## Scripts
- dashboard.py: Live overview and port-forwards.
- cluster-setup.py: Apply YAMLs sequentially.
- data-gen.py: Produce test data to Kafka/HDFS.
- query-hive.py: Execute Hive SQL queries.
- utils.py: Shared Kubernetes client helpers.

## Usage Examples
uv run tools/dashboard.py
uv run tools/data-gen.py --topic test --messages 10

Extend as needed for custom workflows.

## PySpark Notebook-Style Workflows

### spark-notebook-example.py

Interactive data analysis script that demonstrates a notebook-style workflow using PySpark, similar to Jupyter notebooks but as a standalone Python script. It connects to your Spark cluster, explores Hive tables, reads from HDFS, performs transformations, and saves results.

**Installation Requirements:**
```bash
# Install PySpark (matching your cluster version)
pip install pyspark==4.0.1

# For visualizations (optional but recommended)
pip install matplotlib pandas seaborn

# If using uv (from your project setup)
uv add pyspark==4.0.1 matplotlib pandas seaborn
```

**Prerequisites (Port Forwards):**
```bash
# Spark Master
kubectl port-forward -n bigdata svc/spark-master 7077:7077

# Hive Metastore (for Spark SQL)
kubectl port-forward -n bigdata svc/hive-server-service 9083:9083

# HDFS NameNode
kubectl port-forward -n bigdata svc/namenode 9000:9000

# Optional: Spark UI for monitoring
kubectl port-forward -n bigdata svc/spark-master 8080:8080
```

**Usage:**
```bash
# Run the complete analysis
python tools/spark-notebook-example.py

# Run interactively (comment/uncomment sections as needed)
# Edit the script to focus on specific sections, then run:
python tools/spark-notebook-example.py
```

**What It Demonstrates:**

1. **Spark Session Setup**: Connects to your cluster with Hive support enabled
2. **Hive Exploration**: Lists databases, tables, and describes schemas
3. **Data Loading**: Reads from Hive tables and HDFS files
4. **Data Profiling**: Counts, null checks, and basic statistics
5. **Transformations**: Cleans data, creates derived columns, registers temp views
6. **Analysis**: GroupBy aggregations, SQL queries, and insights
7. **HDFS Output**: Saves results as Parquet, JSON, and Hive tables
8. **Visualization**: Creates plots using matplotlib/seaborn (saved as PNG)
9. **Cleanup**: Proper session shutdown and summary

**Example Output Structure:**
```
[1] SETUP SPARK SESSION WITH HIVE INTEGRATION
✓ Spark Session created: 4.0.1
✓ Connected to Spark Master: spark://localhost:7077

[2] EXPLORE HIVE METASTORE
Available Databases:
+------------+
|databaseName|
+------------+
|default     |
|openbeer    |
+------------+
Total databases: 2

[3] LOAD AND EXPLORE BREWERIES DATA FROM HIVE
✓ Loaded 2,400 brewery records
First 10 breweries:
+----+--------------------+-------------+-----+---+
|NUM |NAME                |CITY         |STATE|ID |
+----+--------------------+-------------+-----+---+
|null|NorthGate Brewing   |Minneapolis  |MN   |0  |
...

[7] SAVE RESULTS TO HDFS
✓ State insights saved as Hive table: notebook_results.state_insights
✓ Parquet file saved to: hdfs://localhost:9000/spark-notebook-output/state_insights.parquet
```

**Customization:**
- **Focus on sections**: Comment out sections you don't need
- **Add your data**: Modify table names and HDFS paths for your datasets
- **Extend analysis**: Add new sections for machine learning, joins, window functions
- **Interactive development**: Run specific sections by commenting others

**Verification:**
After running, check results:
```bash
# Hive table
python tools/query-hive.py "SELECT * FROM notebook_results.state_insights LIMIT 5"

# HDFS output  
hdfs dfs -ls hdfs://localhost:9000/spark-notebook-output/

# Spark UI monitoring
# Visit http://localhost:8080 while the script runs
```

**Troubleshooting:**
- **Connection errors**: Verify all port forwards are running
- **Hive not found**: Ensure openbeer database and breweries table exist
- **Memory issues**: Increase executor memory with `--conf spark.executor.memory=2g`
- **No visualizations**: Install matplotlib/pandas: `pip install matplotlib pandas`

This script provides a complete example of how to structure PySpark analysis workflows that can be developed iteratively, just like in Jupyter notebooks, but run as standalone scripts against your production Spark cluster.
