# Submit Spark Streaming job to the Docker Spark cluster (Windows PowerShell)

Write-Host "========================================"
Write-Host "Submitting Spark Streaming Job to Cluster"
Write-Host "========================================"

# Get the spark-master container name dynamically
$sparkMaster = docker ps --filter "name=spark-master" --format "{{.Names}}" | Select-Object -First 1

if ([string]::IsNullOrEmpty($sparkMaster)) {
    Write-Host "Error: spark-master container not found!"
    exit 1
}

Write-Host "Using container: $sparkMaster"

docker exec $sparkMaster /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    --conf spark.executor.memory=1g `
    --conf spark.executor.cores=2 `
    --conf spark.cores.max=6 `
    --conf spark.sql.streaming.checkpointLocation=/mnt/spark-checkpoints `
    --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Duser.home=/tmp" `
    --conf "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp -Duser.home=/tmp" `
    /opt/spark/jobs/spark_processor.py
