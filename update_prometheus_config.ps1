# Update Prometheus config with current container IDs
# This script dynamically maps container IDs to names for cAdvisor metrics

Write-Host "========================================"
Write-Host "Updating Prometheus Configuration"
Write-Host "========================================"

# Get container ID to name mapping
$containers = docker ps --no-trunc --format "{{.ID}},{{.Names}}" | ForEach-Object {
    $parts = $_ -split ","
    @{ Id = $parts[0]; Name = $parts[1] }
}

# Filter for relevant containers (kafka, spark)
$kafkaBrokers = $containers | Where-Object { $_.Name -match "kafka-broker" }
$kafkaControllers = $containers | Where-Object { $_.Name -match "kafka-controller" }
$sparkContainers = $containers | Where-Object { $_.Name -match "spark" }

Write-Host "`nFound containers:"
Write-Host "  Kafka Brokers: $($kafkaBrokers.Count)"
Write-Host "  Kafka Controllers: $($kafkaControllers.Count)"
Write-Host "  Spark: $($sparkContainers.Count)"

# Build metric_relabel_configs
$relabelConfigs = @()

foreach ($c in $kafkaBrokers) {
    $shortName = if ($c.Name -match "kafka-broker-(\d+)") { "kafka-broker-$($Matches[1])" } else { $c.Name }
    $relabelConfigs += @"
      # $shortName
      - source_labels: [id]
        regex: '/docker/$($c.Id)'
        target_label: name
        replacement: '$shortName'
"@
}

foreach ($c in $kafkaControllers) {
    $shortName = if ($c.Name -match "kafka-controller-(\d+)") { "kafka-controller-$($Matches[1])" } else { $c.Name }
    $relabelConfigs += @"
      # $shortName
      - source_labels: [id]
        regex: '/docker/$($c.Id)'
        target_label: name
        replacement: '$shortName'
"@
}

foreach ($c in $sparkContainers) {
    $shortName = if ($c.Name -match "spark-master") { "spark-master-1" }
    elseif ($c.Name -match "spark.worker.*-(\d+)-1") { "spark-worker-$($Matches[1])" }
    elseif ($c.Name -match "spark-worker-(\d+)") { "spark-worker-$($Matches[1])" }
    else { $c.Name -replace "endtoenddataengineeringproject-", "" }
    $relabelConfigs += @"
      # $shortName
      - source_labels: [id]
        regex: '/docker/$($c.Id)'
        target_label: name
        replacement: '$shortName'
"@
}

$relabelConfigText = $relabelConfigs -join "`n"

# Generate new prometheus.yml
$prometheusConfig = @"
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets: []

scrape_configs:
  # Prometheus self-monitoring
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Container metrics via cAdvisor with relabeling
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
    metric_relabel_configs:
$relabelConfigText

  # Kafka Exporter - topic/consumer group level metrics
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']
"@

# Write the config
$configPath = "monitoring\prometheus\prometheus.yml"
$prometheusConfig | Out-File -FilePath $configPath -Encoding UTF8 -NoNewline
Write-Host "`n✅ Updated $configPath"

# Restart Prometheus to apply changes
Write-Host "`nRestarting Prometheus..."
docker restart prometheus | Out-Null
Write-Host "✅ Prometheus restarted"

# Wait for Prometheus to be ready
Write-Host "`nWaiting for Prometheus to be ready..."
Start-Sleep -Seconds 5

# Verify
$testResult = Invoke-WebRequest -Uri "http://localhost:9090/api/v1/query?query=up" -UseBasicParsing -ErrorAction SilentlyContinue
if ($testResult.StatusCode -eq 200) {
    Write-Host "✅ Prometheus is ready!"
} else {
    Write-Host "⚠️ Prometheus may need more time to start"
}

Write-Host "`n========================================"
Write-Host "Configuration update complete!"
Write-Host "========================================"
