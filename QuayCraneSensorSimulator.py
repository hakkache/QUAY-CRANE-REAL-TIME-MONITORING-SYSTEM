import random
import uuid
import time
import json
from datetime import datetime, timedelta
import math

class QuayCraneSensorSimulator:
    """
    Realistic Quay Crane sensor data generator following physical and operational constraints.
    Designed for real-time streaming to Kafka for processing with Spark/ELK/Prometheus.
    """
    
    def __init__(self):
        # Physical crane specifications (realistic STS crane parameters)
        self.crane_specs = {
            'QC_01': {'max_load': 65000, 'age_years': 8, 'maintenance_hours': 1240},
            'QC_02': {'max_load': 65000, 'age_years': 5, 'maintenance_hours': 850},
            'QC_03': {'max_load': 70000, 'age_years': 3, 'maintenance_hours': 420},
            'QC_04': {'max_load': 70000, 'age_years': 2, 'maintenance_hours': 180},
            'QC_05': {'max_load': 65000, 'age_years': 10, 'maintenance_hours': 2100},
            'QC_06': {'max_load': 80000, 'age_years': 1, 'maintenance_hours': 45},
        }
        
        # Container weights (kg) - realistic ISO container weights
        self.container_weights = {
            '20ft': {'tare': 2300, 'max_gross': 30480, 'typical_cargo': (8000, 24000)},
            '40ft': {'tare': 3800, 'max_gross': 32500, 'typical_cargo': (12000, 28000)},
            '40ft_hc': {'tare': 3900, 'max_gross': 32500, 'typical_cargo': (12000, 28000)},
            '45ft': {'tare': 4800, 'max_gross': 32500, 'typical_cargo': (15000, 27000)},
        }
        
        # Current crane states (persistent across calls)
        self.crane_states = {crane_id: self._initialize_crane_state() 
                            for crane_id in self.crane_specs.keys()}
    
    def _initialize_crane_state(self):
        """Initialize crane operational state"""
        return {
            'current_load': 0,
            'trolley_position': 0,  # meters from seaside
            'hoist_height': 0,  # meters above deck
            'gantry_position': random.uniform(0, 30),  # meters along rail
            'operating_hours': 0,
            'last_maintenance': time.time() - random.randint(0, 15*24*3600),
            'motor_temperature': 25,  # ambient start
            'hydraulic_pressure': 0,
            'is_operating': False,
        }
    
    def generate_sensor_reading(self, crane_id, operation_type='idle'):
        """
        Generate a single realistic sensor reading with physical constraints.
        
        This generates the actual data stream that would come from crane sensors.
        """
        timestamp = time.time()
        state = self.crane_states[crane_id]
        spec = self.crane_specs[crane_id]
        
        # Determine container details if operating
        if operation_type in ['loading', 'unloading']:
            container_type = random.choice(['20ft', '40ft', '40ft_hc', '45ft'])
            container_spec = self.container_weights[container_type]
            
            # Realistic cargo weight distribution (most containers 40-80% full)
            cargo_min, cargo_max = container_spec['typical_cargo']
            gross_weight = container_spec['tare'] + random.triangular(cargo_min, cargo_max, (cargo_min + cargo_max) * 0.6)
            
            state['current_load'] = gross_weight
            state['is_operating'] = True
        else:
            state['current_load'] = 0
            state['is_operating'] = False
            container_type = None
            gross_weight = 0
        
        # Physical movements (realistic crane kinematics)
        if state['is_operating']:
            # Trolley movement: 0-65m (seaside to landside)
            target_trolley = random.uniform(20, 55) if operation_type == 'loading' else random.uniform(0, 20)
            state['trolley_position'] = target_trolley + random.uniform(-2, 2)
            
            # Hoist height: 0-40m above deck
            if random.random() < 0.3:  # lifting/lowering
                state['hoist_height'] = random.uniform(8, 35)
            else:  # at pickup/dropoff height
                state['hoist_height'] = random.uniform(0, 3)
            
            # Gantry position along rail (0-30m)
            state['gantry_position'] += random.uniform(-0.5, 0.5)
            state['gantry_position'] = max(0, min(30, state['gantry_position']))
        
        # Motor temperature (physics-based heating/cooling)
        ambient_temp = random.uniform(15, 35)
        if state['is_operating']:
            # Heat generation proportional to load and movement
            load_factor = state['current_load'] / spec['max_load']
            heat_generation = 15 + (load_factor * 25)  # 15-40°C rise
            state['motor_temperature'] = min(
                state['motor_temperature'] + random.uniform(0.5, 2),
                ambient_temp + heat_generation
            )
        else:
            # Cooling when idle (exponential decay toward ambient)
            state['motor_temperature'] = (
                state['motor_temperature'] * 0.95 + ambient_temp * 0.05
            )
        
        # Hydraulic pressure (bar) - proportional to load
        if state['current_load'] > 0:
            load_factor = state['current_load'] / spec['max_load']
            base_pressure = 180 + (load_factor * 80)  # 180-260 bar under load
            state['hydraulic_pressure'] = base_pressure + random.uniform(-5, 5)
        else:
            state['hydraulic_pressure'] = 50 + random.uniform(-3, 3)  # idle pressure
        
        # Power consumption (kW) - realistic calculation
        if state['is_operating']:
            # Power = base + hoisting_power + trolley_power
            hoisting_power = (state['current_load'] * 9.81 * abs(state['hoist_height'] - 20)) / (60 * 1000)  # kW
            trolley_power = 15 + (state['current_load'] / spec['max_load']) * 30
            power_consumption = 50 + hoisting_power + trolley_power + random.uniform(-5, 5)
        else:
            power_consumption = random.uniform(8, 15)  # standby power
        
        # Vibration levels (mm/s RMS) - increases with age and load
        age_factor = spec['age_years'] / 10
        load_factor = state['current_load'] / spec['max_load'] if state['current_load'] > 0 else 0
        base_vibration = 1.5 + (age_factor * 1.0) + (load_factor * 2.5)
        vibration = base_vibration + random.uniform(-0.3, 0.3)
        
        # Wind speed effect on operations (m/s)
        wind_speed = abs(random.gauss(5, 3))  # typical coastal wind
        wind_limited = wind_speed > 15  # operations restricted above 15 m/s
        
        # Cycle time calculation (seconds) - physics-based
        if state['is_operating']:
            # Time = trolley_travel + hoist_time + positioning
            trolley_speed = 3.5  # m/s typical
            hoist_speed = 1.2  # m/s typical
            cycle_time = (
                abs(state['trolley_position'] - 30) / trolley_speed +
                state['hoist_height'] / hoist_speed +
                random.uniform(15, 35)  # positioning and lashing
            )
        else:
            cycle_time = 0
        
        # Alert generation based on thresholds (more sensitive for demo)
        alerts = []
        if state['motor_temperature'] > 55:  # Lowered from 85
            alerts.append('HIGH_MOTOR_TEMP')
        if state['current_load'] > spec['max_load'] * 0.70:  # Lowered from 0.95
            alerts.append('OVERLOAD_WARNING')
        if vibration > 3.5:  # Lowered from 5.0
            alerts.append('HIGH_VIBRATION')
        if wind_speed > 10:  # Lowered from 15
            alerts.append('WIND_LIMIT_EXCEEDED')
        if state['hydraulic_pressure'] < 45 or state['hydraulic_pressure'] > 250:  # Tighter range
            alerts.append('HYDRAULIC_PRESSURE_ABNORMAL')
        if spec['age_years'] > 7:  # Add maintenance alert for older cranes
            alerts.append('MAINTENANCE_DUE')
        if power_consumption > 80:  # High power consumption alert
            alerts.append('HIGH_POWER_CONSUMPTION')
        
        # Construct sensor reading (this is what goes to Kafka)
        sensor_data = {
            # Identifiers
            'sensor_id': f'{crane_id}_sensors',
            'crane_id': crane_id,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            
            # Operation context
            'operation_type': operation_type,
            'container_type': container_type,
            'container_id': f'CONT{random.randint(1000000, 9999999)}' if container_type else None,
            
            # Load sensors
            'load_weight_kg': round(state['current_load'], 2),
            'load_percentage': round((state['current_load'] / spec['max_load']) * 100, 2),
            
            # Position sensors
            'trolley_position_m': round(state['trolley_position'], 2),
            'hoist_height_m': round(state['hoist_height'], 2),
            'gantry_position_m': round(state['gantry_position'], 2),
            
            # Motion sensors
            'trolley_speed_mps': round(random.uniform(0, 3.5) if state['is_operating'] else 0, 2),
            'hoist_speed_mps': round(random.uniform(0, 1.2) if state['is_operating'] else 0, 2),
            
            # Temperature sensors
            'motor_temperature_c': round(state['motor_temperature'], 2),
            'gearbox_temperature_c': round(state['motor_temperature'] - random.uniform(5, 15), 2),
            'ambient_temperature_c': round(ambient_temp, 2),
            
            # Pressure sensors
            'hydraulic_pressure_bar': round(state['hydraulic_pressure'], 2),
            'brake_pressure_bar': round(random.uniform(8, 12) if state['is_operating'] else 10, 2),
            
            # Electrical sensors
            'power_consumption_kw': round(power_consumption, 2),
            'voltage_v': round(400 + random.uniform(-10, 10), 2),
            'current_a': round(power_consumption * 1000 / 400 / math.sqrt(3), 2),
            'power_factor': round(random.uniform(0.82, 0.95), 3),
            
            # Vibration sensors
            'vibration_x_axis_mms': round(vibration + random.uniform(-0.2, 0.2), 3),
            'vibration_y_axis_mms': round(vibration + random.uniform(-0.2, 0.2), 3),
            'vibration_z_axis_mms': round(vibration + random.uniform(-0.2, 0.2), 3),
            
            # Environmental sensors
            'wind_speed_mps': round(wind_speed, 2),
            'wind_direction_deg': random.randint(0, 359),
            'humidity_percent': round(random.uniform(45, 85), 1),
            
            # Performance metrics
            'cycle_time_seconds': round(cycle_time, 2),
            'operational_status': 'operating' if state['is_operating'] else 'idle',
            'wind_limited': wind_limited,
            
            # Maintenance indicators
            'operating_hours': round(state['operating_hours'], 2),
            'hours_since_maintenance': round((timestamp - state['last_maintenance']) / 3600, 2),
            
            # Alerts and warnings
            'alert_count': len(alerts),
            'alerts': alerts,
            'health_score': round(max(0, 100 - len(alerts) * 15 - (spec['age_years'] * 2)), 2),
        }
        
        # Update operating hours
        if state['is_operating']:
            state['operating_hours'] += random.uniform(0.01, 0.02)
        
        return sensor_data
    
    def generate_stream(self, duration_seconds=60, reading_interval=2):
        """
        Generate a continuous stream of sensor data (for Kafka producer).
        
        Args:
            duration_seconds: How long to generate data
            reading_interval: Seconds between readings (typical IoT: 1-5 seconds)
        """
        start_time = time.time()
        readings = []
        
        while time.time() - start_time < duration_seconds:
            # Each crane generates readings
            for crane_id in self.crane_specs.keys():
                # Realistic operation probability
                if random.random() < 0.4:  # 40% chance crane is operating
                    operation = random.choice(['loading', 'unloading'])
                else:
                    operation = 'idle'
                
                reading = self.generate_sensor_reading(crane_id, operation)
                readings.append(reading)
                
                # Print to console (simulating Kafka producer)
                print(f"[{reading['datetime']}] {crane_id} -> Kafka Topic: crane_sensors")
                print(f"  Load: {reading['load_weight_kg']}kg | Temp: {reading['motor_temperature_c']}°C | Power: {reading['power_consumption_kw']}kW")
                if reading['alerts']:
                    print(f"  ⚠️ ALERTS: {', '.join(reading['alerts'])}")
                print()
            
            time.sleep(reading_interval)
        
        return readings

# Example usage
if __name__ == "__main__":
    simulator = QuayCraneSensorSimulator()
    
    print("="*80)
    print("QUAY CRANE SENSOR DATA SIMULATOR")
    print("For: Kafka + Spark + Elasticsearch + Logstash + Kibana + Prometheus + Grafana")
    print("="*80)
    print()
    
    # Generate single reading for each crane
    print("Sample Sensor Readings:\n")
    for crane_id in ['QC_01', 'QC_02', 'QC_03']:
        operation = random.choice(['loading', 'unloading', 'idle'])
        data = simulator.generate_sensor_reading(crane_id, operation)
        
        print(f"📊 {crane_id} ({operation.upper()})")
        print(f"   Timestamp: {data['datetime']}")
        print(f"   Load: {data['load_weight_kg']} kg ({data['load_percentage']}%)")
        print(f"   Position: Trolley={data['trolley_position_m']}m, Hoist={data['hoist_height_m']}m")
        print(f"   Motors: {data['motor_temperature_c']}°C")
        print(f"   Hydraulics: {data['hydraulic_pressure_bar']} bar")
        print(f"   Power: {data['power_consumption_kw']} kW @ {data['voltage_v']}V")
        print(f"   Vibration: {data['vibration_x_axis_mms']} mm/s RMS")
        print(f"   Environment: Wind={data['wind_speed_mps']}m/s, Temp={data['ambient_temperature_c']}°C")
        print(f"   Health Score: {data['health_score']}/100")
        if data['alerts']:
            print(f"   ⚠️  Alerts: {', '.join(data['alerts'])}")
        print()
    
    print("\n" + "="*80)
    print("💡 Data Structure optimized for:")
    print("   • Kafka Topics: crane_sensors, crane_alerts, crane_performance")
    print("   • Spark Streaming: Real-time aggregation and anomaly detection")
    print("   • Elasticsearch: Time-series indexing and search")
    print("   • Prometheus: Metrics scraping (power, temp, vibration)")
    print("   • Grafana: Dashboards and visualization")
    print("   • Kibana: Log analysis and alerting")
    print("="*80)
    
    # Uncomment to generate continuous stream
    # print("\nGenerating 30-second data stream...\n")
    simulator.generate_stream(duration_seconds=30, reading_interval=2)