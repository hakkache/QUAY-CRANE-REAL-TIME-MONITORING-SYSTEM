# Kibana Exports - Quay Crane Monitoring

This folder contains all Kibana visualizations and dashboards for the Quay Crane monitoring system.

## Folder Structure

```
kibana_exports/
├── visualizations/           # Individual visualization JSON files
│   ├── viz-sensor-count.json
│   ├── viz-alert-count.json
│   ├── viz-anomaly-count.json
│   ├── viz-sensor-timeline.json
│   ├── viz-alert-timeline.json
│   ├── viz-anomaly-timeline.json
│   ├── viz-motor-temperature.json
│   ├── viz-power-consumption.json
│   ├── viz-alerts-by-type.json
│   ├── viz-alerts-by-crane.json
│   ├── viz-vibration-levels.json
│   ├── viz-hydraulic-pressure.json
│   ├── viz-wind-speed.json
│   ├── viz-crane-operations.json
│   ├── viz-health-score.json
│   ├── viz-load-distribution.json
│   ├── viz-anomalies-by-type.json
│   └── viz-throughput-timeline.json
├── dashboards/               # Dashboard JSON files
│   ├── dash-overview.json
│   ├── dash-alerts.json
│   ├── dash-equipment.json
│   └── dash-environment.json
├── kibana_import.ndjson      # Combined NDJSON file for bulk import
└── README.md                 # This file
```

## How to Import

### Option 1: Import via Kibana UI (Recommended)

1. Open Kibana at http://localhost:5601
2. Go to **Stack Management** → **Saved Objects**
3. Click **Import** button
4. Select `kibana_import.ndjson` file
5. Check "Automatically overwrite conflicts"
6. Click **Import**

### Option 2: Import via API (curl)

```bash
# Copy file to Kibana container
docker cp kibana_exports/kibana_import.ndjson kibana:/tmp/

# Import via API
docker exec kibana curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F file=@/tmp/kibana_import.ndjson
```

### Option 3: Import via PowerShell

```powershell
$file = Get-Item "kibana_exports\kibana_import.ndjson"
$uri = "http://localhost:5601/api/saved_objects/_import?overwrite=true"

$form = @{
    file = $file
}

Invoke-RestMethod -Uri $uri -Method Post -Headers @{"kbn-xsrf"="true"} -Form $form
```

## Visualizations Included

| ID | Title | Type | Data View |
|----|-------|------|-----------|
| viz-sensor-count | Total Sensor Events | Metric | crane-sensors |
| viz-alert-count | Total Alerts | Metric | crane-alerts |
| viz-anomaly-count | Total Anomalies | Metric | crane-anomalies |
| viz-sensor-timeline | Sensor Events Timeline | Line | crane-sensors |
| viz-alert-timeline | Alerts Timeline | Line | crane-alerts |
| viz-anomaly-timeline | Anomalies Timeline | Line | crane-anomalies |
| viz-motor-temperature | Motor Temperature Over Time | Line | crane-sensors |
| viz-power-consumption | Power Consumption Over Time | Area | crane-sensors |
| viz-alerts-by-type | Alerts by Type | Pie (Donut) | crane-alerts |
| viz-alerts-by-crane | Alerts by Crane | Bar | crane-alerts |
| viz-vibration-levels | Vibration Levels by Crane | Line | crane-sensors |
| viz-hydraulic-pressure | Hydraulic Pressure Over Time | Line | crane-sensors |
| viz-wind-speed | Wind Speed Over Time | Area | crane-sensors |
| viz-crane-operations | Crane Operations Distribution | Pie (Donut) | crane-sensors |
| viz-health-score | Average Health Score by Crane | Horizontal Bar | crane-sensors |
| viz-load-distribution | Load Distribution by Container Type | Bar | crane-sensors |
| viz-anomalies-by-type | Anomalies by Type | Pie (Donut) | crane-anomalies |
| viz-throughput-timeline | Throughput Over Time | Area | crane-throughput |

## Dashboards Included

| ID | Title | Description |
|----|-------|-------------|
| dash-overview | Quay Crane Overview Dashboard | Main overview with key metrics and timelines |
| dash-alerts | Alerts & Anomalies Dashboard | Focused on alerts and anomaly monitoring |
| dash-equipment | Equipment Health Dashboard | Equipment health, temperature, vibration |
| dash-environment | Environmental & Operations Dashboard | Wind, throughput, operational metrics |

## Prerequisites

Before importing, ensure these data views exist in Kibana:
- `crane-sensors`
- `crane-alerts`
- `crane-anomalies`
- `crane-throughput`

## Customization

Each visualization JSON file can be edited to customize:
- Chart titles and descriptions
- Aggregation fields and functions
- Color schemes
- Threshold lines
- Legend positions

Dashboard JSON files can be edited to customize:
- Panel layout (gridData)
- Time range defaults
- Refresh intervals
