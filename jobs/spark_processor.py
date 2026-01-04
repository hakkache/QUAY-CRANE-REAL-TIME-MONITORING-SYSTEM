from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, avg, max, min, sum, count,
    explode, expr, current_timestamp, lit, when, round as spark_round,
    to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, ArrayType, BooleanType, LongType
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Kafka Configuration (Docker internal network)
KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"

# Source Topics
SOURCE_TOPICS = {
    'sensors': 'crane_sensors',
    'alerts': 'crane_alerts', 
    'performance': 'crane_performance'
}

# Output Topics (will be created automatically by Kafka)
OUTPUT_TOPICS = {
    'crane_metrics_1min': 'crane_metrics_1min',        # 1-minute aggregated metrics
    'crane_metrics_5min': 'crane_metrics_5min',        # 5-minute aggregated metrics
    'crane_health_status': 'crane_health_status',      # Real-time health status
    'crane_anomalies': 'crane_anomalies',              # Detected anomalies
    'crane_throughput': 'crane_throughput',            # Container throughput metrics
    'alert_summary': 'alert_summary',                  # Aggregated alert summary
}

# Checkpoint locations
CHECKPOINT_BASE = "/mnt/spark-checkpoints"

# ============================================================================
# SCHEMAS
# ============================================================================

# Schema for crane_sensors topic
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("crane_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("container_type", StringType(), True),
    StructField("container_id", StringType(), True),
    StructField("load_weight_kg", DoubleType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("trolley_position_m", DoubleType(), True),
    StructField("hoist_height_m", DoubleType(), True),
    StructField("gantry_position_m", DoubleType(), True),
    StructField("trolley_speed_mps", DoubleType(), True),
    StructField("hoist_speed_mps", DoubleType(), True),
    StructField("motor_temperature_c", DoubleType(), True),
    StructField("gearbox_temperature_c", DoubleType(), True),
    StructField("ambient_temperature_c", DoubleType(), True),
    StructField("hydraulic_pressure_bar", DoubleType(), True),
    StructField("brake_pressure_bar", DoubleType(), True),
    StructField("power_consumption_kw", DoubleType(), True),
    StructField("voltage_v", DoubleType(), True),
    StructField("current_a", DoubleType(), True),
    StructField("power_factor", DoubleType(), True),
    StructField("vibration_x_axis_mms", DoubleType(), True),
    StructField("vibration_y_axis_mms", DoubleType(), True),
    StructField("vibration_z_axis_mms", DoubleType(), True),
    StructField("wind_speed_mps", DoubleType(), True),
    StructField("wind_direction_deg", IntegerType(), True),
    StructField("humidity_percent", DoubleType(), True),
    StructField("cycle_time_seconds", DoubleType(), True),
    StructField("operational_status", StringType(), True),
    StructField("wind_limited", BooleanType(), True),
    StructField("operating_hours", DoubleType(), True),
    StructField("hours_since_maintenance", DoubleType(), True),
    StructField("alert_count", IntegerType(), True),
    StructField("alerts", ArrayType(StringType()), True),
    StructField("health_score", DoubleType(), True),
])

# Schema for crane_alerts topic
alert_schema = StructType([
    StructField("crane_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("alerts", ArrayType(StringType()), True),
    StructField("alert_count", IntegerType(), True),
    StructField("motor_temperature_c", DoubleType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("health_score", DoubleType(), True),
])

# Schema for crane_performance topic
performance_schema = StructType([
    StructField("crane_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("container_type", StringType(), True),
    StructField("cycle_time_seconds", DoubleType(), True),
    StructField("load_weight_kg", DoubleType(), True),
    StructField("power_consumption_kw", DoubleType(), True),
])


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """Create Spark session configured for Kafka streaming."""
    return (SparkSession.builder
        .appName("QuayCraneStreamProcessor")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate())


# ============================================================================
# KAFKA READERS
# ============================================================================

def read_kafka_stream(spark, topic, schema):
    """Read from Kafka topic and parse JSON."""
    return (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100000)
        .load()
        .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
        .select(
            col("key"),
            from_json(col("value"), schema).alias("data"),
            col("kafka_timestamp")
        )
        .select("key", "data.*", "kafka_timestamp")
    )


# ============================================================================
# TRANSFORMATIONS
# ============================================================================

def compute_1min_metrics(sensor_df):
    """
    Compute 1-minute aggregated metrics per crane.
    Output: Average/Max/Min of key metrics over 1-minute windows.
    """
    return (sensor_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("crane_id")
        )
        .agg(
            # Load metrics
            spark_round(avg("load_weight_kg"), 2).alias("avg_load_kg"),
            spark_round(max("load_weight_kg"), 2).alias("max_load_kg"),
            spark_round(avg("load_percentage"), 2).alias("avg_load_pct"),
            
            # Temperature metrics
            spark_round(avg("motor_temperature_c"), 2).alias("avg_motor_temp"),
            spark_round(max("motor_temperature_c"), 2).alias("max_motor_temp"),
            spark_round(min("motor_temperature_c"), 2).alias("min_motor_temp"),
            
            # Power metrics
            spark_round(avg("power_consumption_kw"), 2).alias("avg_power_kw"),
            spark_round(max("power_consumption_kw"), 2).alias("max_power_kw"),
            spark_round(sum("power_consumption_kw"), 2).alias("total_power_kw"),
            
            # Vibration metrics
            spark_round(avg("vibration_x_axis_mms"), 3).alias("avg_vibration"),
            spark_round(max("vibration_x_axis_mms"), 3).alias("max_vibration"),
            
            # Hydraulics
            spark_round(avg("hydraulic_pressure_bar"), 2).alias("avg_hydraulic_pressure"),
            
            # Health
            spark_round(avg("health_score"), 2).alias("avg_health_score"),
            spark_round(min("health_score"), 2).alias("min_health_score"),
            
            # Counts
            count("*").alias("reading_count"),
            sum(when(col("operational_status") == "operating", 1).otherwise(0)).alias("operating_count"),
            sum("alert_count").alias("total_alerts"),
        )
        .select(
            col("crane_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_load_kg"), col("max_load_kg"), col("avg_load_pct"),
            col("avg_motor_temp"), col("max_motor_temp"), col("min_motor_temp"),
            col("avg_power_kw"), col("max_power_kw"), col("total_power_kw"),
            col("avg_vibration"), col("max_vibration"),
            col("avg_hydraulic_pressure"),
            col("avg_health_score"), col("min_health_score"),
            col("reading_count"), col("operating_count"), col("total_alerts"),
            current_timestamp().alias("processed_at")
        )
    )


def compute_5min_metrics(sensor_df):
    """
    Compute 5-minute aggregated metrics per crane.
    Useful for trend analysis and dashboards.
    """
    return (sensor_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("crane_id")
        )
        .agg(
            spark_round(avg("load_weight_kg"), 2).alias("avg_load_kg"),
            spark_round(avg("motor_temperature_c"), 2).alias("avg_motor_temp"),
            spark_round(max("motor_temperature_c"), 2).alias("max_motor_temp"),
            spark_round(avg("power_consumption_kw"), 2).alias("avg_power_kw"),
            spark_round(sum("power_consumption_kw"), 2).alias("total_power_kw"),
            spark_round(avg("vibration_x_axis_mms"), 3).alias("avg_vibration"),
            spark_round(avg("health_score"), 2).alias("avg_health_score"),
            count("*").alias("reading_count"),
            sum("alert_count").alias("total_alerts"),
            spark_round(avg("wind_speed_mps"), 2).alias("avg_wind_speed"),
        )
        .select(
            col("crane_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_load_kg"), col("avg_motor_temp"), col("max_motor_temp"),
            col("avg_power_kw"), col("total_power_kw"),
            col("avg_vibration"), col("avg_health_score"),
            col("reading_count"), col("total_alerts"), col("avg_wind_speed"),
            current_timestamp().alias("processed_at")
        )
    )


def compute_health_status(sensor_df):
    """
    Compute real-time health status per crane.
    Categorizes crane health: CRITICAL, WARNING, GOOD, EXCELLENT
    """
    return (sensor_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds"),
            col("crane_id")
        )
        .agg(
            spark_round(avg("health_score"), 2).alias("health_score"),
            spark_round(avg("motor_temperature_c"), 2).alias("motor_temp"),
            spark_round(avg("vibration_x_axis_mms"), 3).alias("vibration"),
            spark_round(avg("hydraulic_pressure_bar"), 2).alias("hydraulic_pressure"),
            max("alert_count").alias("max_alerts"),
            count("*").alias("readings"),
        )
        .withColumn("health_status",
            when(col("health_score") < 40, "CRITICAL")
            .when(col("health_score") < 60, "WARNING")
            .when(col("health_score") < 80, "GOOD")
            .otherwise("EXCELLENT")
        )
        .withColumn("requires_attention",
            when(
                (col("health_score") < 60) | 
                (col("motor_temp") > 70) | 
                (col("vibration") > 4.0) |
                (col("max_alerts") > 2),
                True
            ).otherwise(False)
        )
        .select(
            col("crane_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("health_score"), col("health_status"),
            col("motor_temp"), col("vibration"), col("hydraulic_pressure"),
            col("max_alerts"), col("requires_attention"), col("readings"),
            current_timestamp().alias("processed_at")
        )
    )


def detect_anomalies(sensor_df):
    """
    Detect anomalies based on thresholds.
    Outputs anomaly records with severity levels.
    """
    return (sensor_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        # Detect various anomalies
        .withColumn("temp_anomaly", col("motor_temperature_c") > 75)
        .withColumn("load_anomaly", col("load_percentage") > 90)
        .withColumn("vibration_anomaly", col("vibration_x_axis_mms") > 4.5)
        .withColumn("pressure_anomaly", 
            (col("hydraulic_pressure_bar") < 45) | (col("hydraulic_pressure_bar") > 260))
        .withColumn("power_anomaly", col("power_consumption_kw") > 100)
        # Filter only records with anomalies
        .filter(
            col("temp_anomaly") | col("load_anomaly") | 
            col("vibration_anomaly") | col("pressure_anomaly") | col("power_anomaly")
        )
        .withColumn("anomaly_type",
            when(col("temp_anomaly"), "HIGH_TEMPERATURE")
            .when(col("load_anomaly"), "OVERLOAD")
            .when(col("vibration_anomaly"), "HIGH_VIBRATION")
            .when(col("pressure_anomaly"), "PRESSURE_ABNORMAL")
            .when(col("power_anomaly"), "HIGH_POWER")
            .otherwise("UNKNOWN")
        )
        .withColumn("severity",
            when(col("motor_temperature_c") > 90, "CRITICAL")
            .when(col("load_percentage") > 95, "CRITICAL")
            .when(col("vibration_x_axis_mms") > 6.0, "CRITICAL")
            .when(col("temp_anomaly") | col("load_anomaly") | col("vibration_anomaly"), "HIGH")
            .otherwise("MEDIUM")
        )
        .select(
            col("crane_id"),
            col("datetime"),
            col("anomaly_type"),
            col("severity"),
            col("motor_temperature_c"),
            col("load_percentage"),
            col("vibration_x_axis_mms"),
            col("hydraulic_pressure_bar"),
            col("power_consumption_kw"),
            col("health_score"),
            col("operation_type"),
            current_timestamp().alias("detected_at")
        )
    )


def compute_throughput(performance_df):
    """
    Compute container throughput metrics per crane.
    Measures operational efficiency.
    """
    return (performance_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withWatermark("event_time", "1 minute")
        .filter(col("operation_type").isin("loading", "unloading"))
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("crane_id"),
            col("operation_type")
        )
        .agg(
            count("*").alias("container_count"),
            spark_round(avg("cycle_time_seconds"), 2).alias("avg_cycle_time"),
            spark_round(min("cycle_time_seconds"), 2).alias("min_cycle_time"),
            spark_round(max("cycle_time_seconds"), 2).alias("max_cycle_time"),
            spark_round(sum("load_weight_kg"), 2).alias("total_weight_kg"),
            spark_round(avg("power_consumption_kw"), 2).alias("avg_power_kw"),
        )
        .withColumn("containers_per_hour", 
            spark_round((col("container_count") / 5.0) * 60, 2))
        .withColumn("efficiency_score",
            spark_round(100 - (col("avg_cycle_time") - 30) / 2, 2))  # Baseline 30s = 100%
        .select(
            col("crane_id"),
            col("operation_type"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("container_count"), col("containers_per_hour"),
            col("avg_cycle_time"), col("min_cycle_time"), col("max_cycle_time"),
            col("total_weight_kg"), col("avg_power_kw"),
            col("efficiency_score"),
            current_timestamp().alias("processed_at")
        )
    )


def compute_alert_summary(alert_df):
    """
    Aggregate alert counts by type and crane.
    """
    return (alert_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withWatermark("event_time", "1 minute")
        .select(
            col("crane_id"),
            col("event_time"),
            col("health_score"),
            explode(col("alerts")).alias("alert_type")
        )
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("crane_id"),
            col("alert_type")
        )
        .agg(
            count("*").alias("alert_count"),
            spark_round(avg("health_score"), 2).alias("avg_health_score"),
        )
        .select(
            col("crane_id"),
            col("alert_type"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("alert_count"),
            col("avg_health_score"),
            current_timestamp().alias("processed_at")
        )
    )


# ============================================================================
# KAFKA WRITERS
# ============================================================================

def write_to_kafka(df, topic, checkpoint_name):
    """Write DataFrame to Kafka topic."""
    return (df
        .select(
            col("crane_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_name}")
        .outputMode("update")
        .start()
    )


def write_anomalies_to_kafka(df, topic, checkpoint_name):
    """Write anomalies to Kafka (append mode for alerts)."""
    return (df
        .select(
            col("crane_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_name}")
        .outputMode("append")
        .start()
    )


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("🚢 QUAY CRANE SPARK STREAMING PROCESSOR")
    print("=" * 80)
    
    # Create Spark session
    print("\n📡 Creating Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"   Spark version: {spark.version}")
    print(f"   Kafka brokers: {KAFKA_BROKERS}")
    
    # Read from source topics
    print("\n📥 Setting up Kafka source streams...")
    
    sensor_stream = read_kafka_stream(spark, SOURCE_TOPICS['sensors'], sensor_schema)
    print(f"   ✅ Reading from: {SOURCE_TOPICS['sensors']}")
    
    alert_stream = read_kafka_stream(spark, SOURCE_TOPICS['alerts'], alert_schema)
    print(f"   ✅ Reading from: {SOURCE_TOPICS['alerts']}")
    
    performance_stream = read_kafka_stream(spark, SOURCE_TOPICS['performance'], performance_schema)
    print(f"   ✅ Reading from: {SOURCE_TOPICS['performance']}")
    
    # Set up transformations and outputs
    print("\n🔄 Setting up transformations and output streams...")
    
    # 1. 1-minute metrics
    metrics_1min = compute_1min_metrics(sensor_stream)
    query_1min = write_to_kafka(metrics_1min, OUTPUT_TOPICS['crane_metrics_1min'], "metrics_1min")
    print(f"   ✅ {OUTPUT_TOPICS['crane_metrics_1min']} - 1-minute aggregated metrics")
    
    # 2. 5-minute metrics
    metrics_5min = compute_5min_metrics(sensor_stream)
    query_5min = write_to_kafka(metrics_5min, OUTPUT_TOPICS['crane_metrics_5min'], "metrics_5min")
    print(f"   ✅ {OUTPUT_TOPICS['crane_metrics_5min']} - 5-minute aggregated metrics")
    
    # 3. Health status
    health_status = compute_health_status(sensor_stream)
    query_health = write_to_kafka(health_status, OUTPUT_TOPICS['crane_health_status'], "health_status")
    print(f"   ✅ {OUTPUT_TOPICS['crane_health_status']} - Real-time health status")
    
    # 4. Anomaly detection
    anomalies = detect_anomalies(sensor_stream)
    query_anomalies = write_anomalies_to_kafka(anomalies, OUTPUT_TOPICS['crane_anomalies'], "anomalies")
    print(f"   ✅ {OUTPUT_TOPICS['crane_anomalies']} - Detected anomalies")
    
    # 5. Throughput metrics
    throughput = compute_throughput(performance_stream)
    query_throughput = write_to_kafka(throughput, OUTPUT_TOPICS['crane_throughput'], "throughput")
    print(f"   ✅ {OUTPUT_TOPICS['crane_throughput']} - Container throughput")
    
    # 6. Alert summary
    alert_summary = compute_alert_summary(alert_stream)
    query_alerts = write_to_kafka(alert_summary, OUTPUT_TOPICS['alert_summary'], "alert_summary")
    print(f"   ✅ {OUTPUT_TOPICS['alert_summary']} - Alert aggregations")
    
    print("\n" + "=" * 80)
    print("🚀 STREAMING STARTED - All processors running!")
    print("   Output topics:")
    for name, topic in OUTPUT_TOPICS.items():
        print(f"      • {topic}")
    print("\n   Press Ctrl+C to stop...")
    print("=" * 80)
    
    # Wait for all queries
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping all streams...")
        for query in spark.streams.active:
            query.stop()
        print("✅ All streams stopped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()