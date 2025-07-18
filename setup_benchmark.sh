#!/bin/bash
set -eux

# Create TimescaleDB benchmark script
cat > /mnt/benchmark/scripts/timescaledb_benchmark.py <<'TSDB_SCRIPT'
import psycopg2
import time
import threading
import random
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Load configuration
with open('/mnt/benchmark/config/benchmark_config.json', 'r') as f:
    config = json.load(f)

def create_connection():
    return psycopg2.connect(
        host=config['TIMESCALEDB_HOST'],
        database=config['TIMESCALEDB_DATABASE'],
        user=config['TIMESCALEDB_USER'],
        password=config['TIMESCALEDB_PASSWORD'],
        port=5432
    )

def setup_database():
    conn = create_connection()
    cur = conn.cursor()
    
    # Create the pv_benchmark table with solar inverter schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pv_benchmark (
            solarstations_id INT NOT NULL,
            device VARCHAR(255) NOT NULL,
            uhrzeit TIMESTAMP NOT NULL,
            s VARCHAR(255),
            adresse VARCHAR(255),
            serien_nummer VARCHAR(255),
            mpc VARCHAR(255),
            idc1 DECIMAL(10,3),
            idc2 DECIMAL(10,3),
            idc3 DECIMAL(10,3),
            udc1 DECIMAL(10,3),
            udc2 DECIMAL(10,3),
            udc3 DECIMAL(10,3),
            riso1 DECIMAL(10,3),
            riso2 DECIMAL(10,3),
            riso_plus DECIMAL(10,3),
            riso_minus DECIMAL(10,3),
            iac1 DECIMAL(10,3),
            iac2 DECIMAL(10,3),
            iac3 DECIMAL(10,3),
            uac1 DECIMAL(10,3),
            uac2 DECIMAL(10,3),
            uac3 DECIMAL(10,3),
            pac1 DECIMAL(10,3),
            pac2 DECIMAL(10,3),
            pac3 DECIMAL(10,3),
            pac1r DECIMAL(10,3),
            pac2r DECIMAL(10,3),
            pac3r DECIMAL(10,3),
            pac_tot DECIMAL(10,3),
            fac1 DECIMAL(10,3),
            fac2 DECIMAL(10,3),
            fac3 DECIMAL(10,3),
            eac DECIMAL(10,3),
            ton DECIMAL(10,3),
            tntcdc DECIMAL(10,3),
            r_mov1 DECIMAL(10,3),
            r_mov2 DECIMAL(10,3),
            tntcac DECIMAL(10,3),
            uacc DECIMAL(10,3),
            facc DECIMAL(10,3),
            e_total DECIMAL(15,3),
            ron_day DECIMAL(10,3),
            ron_tot DECIMAL(15,3),
            status_global INT,
            status_dc INT,
            lim_dc INT,
            status_ac INT,
            lim_ac INT,
            status_iso INT,
            dc_err INT,
            ac_err INT,
            sc_err INT,
            bulk_err INT,
            com_err INT,
            sc_dis INT,
            err_hw_dc INT,
            status_kalib INT,
            status_null INT,
            pdc1 DECIMAL(10,3),
            pdc2 DECIMAL(10,3),
            pdc3 DECIMAL(10,3),
            bus_v_plus DECIMAL(10,3),
            bus_v_minus DECIMAL(10,3),
            t_calc DECIMAL(10,3),
            t_sink DECIMAL(10,3),
            status_ac1 INT,
            status_ac2 INT,
            status_ac3 INT,
            status_ac4 INT,
            status_dc1 INT,
            status_dc2 INT,
            error_status INT,
            error_ac1 INT,
            global_err_1 INT,
            cpu_error INT,
            global_err_2 INT,
            limits_ac1 INT,
            limits_ac2 INT,
            global_err_3 INT,
            eint DECIMAL(10,3),
            limits_dc1 INT,
            limits_dc2 INT,
            qac1 DECIMAL(10,3),
            qac2 DECIMAL(10,3),
            qac3 DECIMAL(10,3),
            tamb DECIMAL(10,3),
            theat DECIMAL(10,3),
            status_1 INT,
            status_2 INT,
            status_3 INT,
            status_4 INT,
            internal_status_1 INT,
            internal_status_2 INT,
            internal_status_3 INT,
            internal_status_4 INT,
            event1 INT,
            operatingstate INT,
            actualpower DECIMAL(10,3),
            udc4 DECIMAL(10,3),
            idc4 DECIMAL(10,3),
            pdc4 DECIMAL(10,3),
            udc5 DECIMAL(10,3),
            idc5 DECIMAL(10,3),
            pdc5 DECIMAL(10,3),
            udc6 DECIMAL(10,3),
            idc6 DECIMAL(10,3),
            pdc6 DECIMAL(10,3),
            insertat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            protocoltype VARCHAR(50),
            pac1new DECIMAL(10,3),
            systeminserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Convert to hypertable for time-series optimization
    try:
        cur.execute("SELECT create_hypertable('pv_benchmark', 'uhrzeit')")
        print("Created hypertable on pv_benchmark")
    except Exception as e:
        print(f"Hypertable may already exist: {e}")
    
    # Create indexes for performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pv_benchmark_time ON pv_benchmark (uhrzeit)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pv_benchmark_device ON pv_benchmark (device)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pv_benchmark_station ON pv_benchmark (solarstations_id)")
    
    conn.commit()
    conn.close()
    print("TimescaleDB pv_benchmark table setup complete")

def generate_solar_data(base_time, offset_seconds):
    """Generate realistic solar inverter data"""
    timestamp = base_time + timedelta(seconds=offset_seconds)
    
    # Solar data varies by time of day (simulate sun patterns)
    hour = timestamp.hour
    is_daylight = 6 <= hour <= 18
    sun_factor = max(0, (1 - abs(hour - 12) / 6)) if is_daylight else 0
    
    # Base solar generation values
    base_power = sun_factor * random.uniform(0.8, 1.2) * 5000  # 5kW max
    
    return {
        'solarstations_id': random.randint(1, 100),
        'device': random.randint(1, 1000),
        'uhrzeit': timestamp,
        's': random.randint(0, 65535),
        'adresse': random.randint(1, 255),
        'serien_nummer': f"INV{random.randint(100000, 999999)}",
        'mpc': random.randint(0, 127),
        'idc1': random.uniform(0, 20) * sun_factor,
        'idc2': random.uniform(0, 20) * sun_factor,
        'idc3': random.uniform(0, 20) * sun_factor,
        'udc1': random.uniform(300, 800) if is_daylight else 0,
        'udc2': random.uniform(300, 800) if is_daylight else 0,
        'udc3': random.uniform(300, 800) if is_daylight else 0,
        'riso1': random.uniform(1000, 10000),
        'riso2': random.uniform(1000, 10000),
        'riso_plus': random.uniform(1000, 10000),
        'riso_minus': random.uniform(1000, 10000),
        'iac1': random.uniform(0, 25) * sun_factor,
        'iac2': random.uniform(0, 25) * sun_factor,
        'iac3': random.uniform(0, 25) * sun_factor,
        'uac1': 230 + random.uniform(-10, 10),
        'uac2': 230 + random.uniform(-10, 10),
        'uac3': 230 + random.uniform(-10, 10),
        'pac1': int(base_power / 3 + random.uniform(-100, 100)),
        'pac2': int(base_power / 3 + random.uniform(-100, 100)),
        'pac3': int(base_power / 3 + random.uniform(-100, 100)),
        'pac1r': random.randint(-1000, 1000),
        'pac2r': random.randint(-1000, 1000),
        'pac3r': random.randint(-1000, 1000),
        'pac_tot': int(base_power),
        'fac1': 50.0 + random.uniform(-0.5, 0.5),
        'fac2': 50.0 + random.uniform(-0.5, 0.5),
        'fac3': 50.0 + random.uniform(-0.5, 0.5),
        'eac': random.randint(0, 16777215),
        'ton': random.uniform(0, 100),
        'tntcdc': random.uniform(20, 80),
        'r_mov1': random.uniform(0, 1000),
        'r_mov2': random.uniform(0, 1000),
        'tntcac': random.uniform(20, 80),
        'uacc': random.uniform(200, 250),
        'facc': 50.0 + random.uniform(-1, 1),
        'e_total': random.randint(0, 16777215),
        'ron_day': random.uniform(0, 24),
        'ron_tot': random.uniform(0, 100000),
        'status_global': random.randint(0, 255),
        'status_dc': random.randint(0, 255),
        'lim_dc': random.randint(0, 255),
        'status_ac': random.randint(0, 255),
        'lim_ac': random.randint(0, 255),
        'status_iso': random.randint(0, 255),
        'dc_err': random.randint(0, 255),
        'ac_err': random.randint(0, 255),
        'sc_err': random.randint(0, 255),
        'bulk_err': random.randint(0, 255),
        'com_err': random.randint(0, 255),
        'sc_dis': random.randint(0, 255),
        'err_hw_dc': random.randint(0, 255),
        'status_kalib': random.randint(0, 255),
        'status_null': random.randint(0, 255),
        'pdc1': int(base_power / 3 * random.uniform(0.9, 1.1)),
        'pdc2': int(base_power / 3 * random.uniform(0.9, 1.1)),
        'pdc3': int(base_power / 3 * random.uniform(0.9, 1.1)),
        'bus_v_plus': random.uniform(300, 400),
        'bus_v_minus': random.uniform(-400, -300),
        't_calc': random.randint(0, 255),
        't_sink': random.randint(-32768, 32767),
        'status_ac1': random.randint(0, 255),
        'status_ac2': random.randint(0, 255),
        'status_ac3': random.randint(0, 255),
        'status_ac4': random.randint(0, 255),
        'status_dc1': random.randint(0, 255),
        'status_dc2': random.randint(0, 255),
        'error_status': random.randint(0, 255),
        'error_ac1': random.randint(0, 255),
        'global_err_1': random.randint(0, 255),
        'cpu_error': random.randint(0, 255),
        'global_err_2': random.randint(0, 255),
        'limits_ac1': random.randint(0, 255),
        'limits_ac2': random.randint(0, 255),
        'global_err_3': random.randint(0, 255),
        'eint': random.randint(0, 16777215),
        'limits_dc1': random.randint(0, 255),
        'limits_dc2': random.randint(0, 255),
        'qac1': random.uniform(0, 1000) * sun_factor,
        'qac2': random.uniform(0, 1000) * sun_factor,
        'qac3': random.uniform(0, 1000) * sun_factor,
        'tamb': random.uniform(15, 35),
        'theat': random.uniform(20, 80),
        'status_1': random.randint(0, 255),
        'status_2': random.randint(0, 255),
        'status_3': random.randint(0, 255),
        'status_4': random.randint(0, 255),
        'internal_status_1': random.randint(-128, 127),
        'internal_status_2': random.randint(-128, 127),
        'internal_status_3': random.randint(-128, 127),
        'internal_status_4': random.randint(-128, 127),
        'event1': random.randint(-2147483648, 2147483647),
        'operatingstate': random.randint(-2147483648, 2147483647),
        'actualpower': int(base_power),
        'udc4': random.uniform(300, 800) if is_daylight else 0,
        'idc4': random.uniform(0, 20) * sun_factor,
        'pdc4': random.uniform(0, 2000) * sun_factor,
        'udc5': random.uniform(300, 800) if is_daylight else 0,
        'idc5': random.uniform(0, 20) * sun_factor,
        'pdc5': random.uniform(0, 2000) * sun_factor,
        'udc6': random.uniform(300, 800) if is_daylight else 0,
        'idc6': random.uniform(0, 20) * sun_factor,
        'pdc6': random.uniform(0, 2000) * sun_factor,
        'insertat': datetime.now(),
        'protocoltype': random.choice([True, False]),
        'pac1new': int(base_power / 3 + random.uniform(-50, 50)),
        'systeminserted': datetime.now()
    }

def insert_data(thread_id, rows_per_thread):
    conn = create_connection()
    cur = conn.cursor()
    
    start_time = time.time()
    base_time = datetime.now() - timedelta(days=30)
    
    # Prepare the insert statement
    columns = [
        'solarstations_id', 'device', 'uhrzeit', 's', 'adresse', 'serien_nummer', 'mpc',
        'idc1', 'idc2', 'idc3', 'udc1', 'udc2', 'udc3', 'riso1', 'riso2', 'riso_plus', 'riso_minus',
        'iac1', 'iac2', 'iac3', 'uac1', 'uac2', 'uac3', 'pac1', 'pac2', 'pac3', 'pac1r', 'pac2r', 'pac3r',
        'pac_tot', 'fac1', 'fac2', 'fac3', 'eac', 'ton', 'tntcdc', 'r_mov1', 'r_mov2', 'tntcac',
        'uacc', 'facc', 'e_total', 'ron_day', 'ron_tot', 'status_global', 'status_dc', 'lim_dc',
        'status_ac', 'lim_ac', 'status_iso', 'dc_err', 'ac_err', 'sc_err', 'bulk_err', 'com_err',
        'sc_dis', 'err_hw_dc', 'status_kalib', 'status_null', 'pdc1', 'pdc2', 'pdc3',
        'bus_v_plus', 'bus_v_minus', 't_calc', 't_sink', 'status_ac1', 'status_ac2',
        'status_ac3', 'status_ac4', 'status_dc1', 'status_dc2', 'error_status', 'error_ac1',
        'global_err_1', 'cpu_error', 'global_err_2', 'limits_ac1', 'limits_ac2',
        'global_err_3', 'eint', 'limits_dc1', 'limits_dc2', 'qac1', 'qac2', 'qac3',
        'tamb', 'theat', 'status_1', 'status_2', 'status_3', 'status_4',
        'internal_status_1', 'internal_status_2', 'internal_status_3', 'internal_status_4',
        'event1', 'operatingstate', 'actualpower', 'udc4', 'idc4', 'pdc4', 'udc5', 'idc5', 'pdc5',
        'udc6', 'idc6', 'pdc6', 'insertat', 'protocoltype', 'pac1new', 'systeminserted'
    ]
    
    insert_sql = f"""
        INSERT INTO pv_benchmark ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """
    
    batch_data = []
    for i in range(rows_per_thread):
        data = generate_solar_data(base_time, i + thread_id * rows_per_thread)
        
        # Convert data to tuple for insertion
        row_data = (
            data['solarstations_id'], data['device'], data['uhrzeit'], data['s'],
            data['adresse'], data['serien_nummer'], data['mpc'], data['idc1'],
            data['idc2'], data['idc3'], data['udc1'], data['udc2'], data['udc3'],
            data['riso1'], data['riso2'], data['riso_plus'], data['riso_minus'],
            data['iac1'], data['iac2'], data['iac3'], data['uac1'], data['uac2'],
            data['uac3'], data['pac1'], data['pac2'], data['pac3'], data['pac1r'],
            data['pac2r'], data['pac3r'], data['pac_tot'], data['fac1'], data['fac2'],
            data['fac3'], data['eac'], data['ton'], data['tntcdc'], data['r_mov1'],
            data['r_mov2'], data['tntcac'], data['uacc'], data['facc'], data['e_total'],
            data['ron_day'], data['ron_tot'], data['status_global'], data['status_dc'],
            data['lim_dc'], data['status_ac'], data['lim_ac'], data['status_iso'],
            data['dc_err'], data['ac_err'], data['sc_err'], data['bulk_err'],
            data['com_err'], data['sc_dis'], data['err_hw_dc'], data['status_kalib'],
            data['status_null'], data['pdc1'], data['pdc2'], data['pdc3'],
            data['bus_v_plus'], data['bus_v_minus'], data['t_calc'], data['t_sink'],
            data['status_ac1'], data['status_ac2'], data['status_ac3'], data['status_ac4'],
            data['status_dc1'], data['status_dc2'], data['error_status'], data['error_ac1'],
            data['global_err_1'], data['cpu_error'], data['global_err_2'],
            data['limits_ac1'], data['limits_ac2'], data['global_err_3'], data['eint'],
            data['limits_dc1'], data['limits_dc2'], data['qac1'], data['qac2'],
            data['qac3'], data['tamb'], data['theat'], data['status_1'], data['status_2'],
            data['status_3'], data['status_4'], data['internal_status_1'],
            data['internal_status_2'], data['internal_status_3'], data['internal_status_4'],
            data['event1'], data['operatingstate'], data['actualpower'], data['udc4'],
            data['idc4'], data['pdc4'], data['udc5'], data['idc5'], data['pdc5'],
            data['udc6'], data['idc6'], data['pdc6'], data['insertat'],
            data['protocoltype'], data['pac1new'], data['systeminserted']
        )
        
        batch_data.append(row_data)
        
        # Insert in batches of 1000
        if len(batch_data) >= 1000:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
            batch_data = []
    
    # Insert remaining data
    if batch_data:
        cur.executemany(insert_sql, batch_data)
        conn.commit()
    
    conn.close()
    
    end_time = time.time()
    print(f"Thread {thread_id}: Inserted {rows_per_thread} rows in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    print("Starting TimescaleDB solar inverter benchmark...")
    setup_database()
    
    total_rows = int(config['BENCHMARK_ROWS'])
    num_threads = int(config['BENCHMARK_THREADS'])
    rows_per_thread = total_rows // num_threads
    
    start_time = time.time()
    
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=insert_data, args=(i, rows_per_thread))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"TimescaleDB Benchmark Complete:")
    print(f"Total rows: {total_rows}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Rows per second: {total_rows / total_time:.2f}")
TSDB_SCRIPT

# Create Aurora MySQL benchmark script
cat > /mnt/benchmark/scripts/aurora_benchmark.py <<'AURORA_SCRIPT'
#!/usr/bin/env python3
import pymysql
import json
import threading
import time
import random
from datetime import datetime, timedelta
import sys

def load_config():
    with open('/mnt/benchmark/config/benchmark_config.json', 'r') as f:
        return json.load(f)

def generate_solar_data(base_time, index):
    """Generate realistic solar inverter data with time-based patterns"""
    timestamp = base_time + timedelta(seconds=index * 10)
    hour = timestamp.hour
    
    # Solar generation pattern (daylight hours 6-18)
    is_daylight = 6 <= hour <= 18
    sun_factor = max(0, (1 - abs(hour - 12) / 6)) if is_daylight else 0
    
    # Base power generation
    base_power = random.uniform(0, 5000) * sun_factor
    
    return {
        'solarstations_id': random.randint(1, 100),
        'device': f'INV_{random.randint(1000, 9999)}',
        'uhrzeit': timestamp,
        's': random.randint(0, 1),
        'adresse': random.randint(1, 255),
        'serien_nummer': f'SN{random.randint(100000, 999999)}',
        'mpc': random.randint(0, 100),
        'idc1': random.uniform(0, 20) * sun_factor,
        'idc2': random.uniform(0, 20) * sun_factor,
        'idc3': random.uniform(0, 20) * sun_factor,
        'udc1': random.uniform(300, 800) if is_daylight else 0,
        'udc2': random.uniform(300, 800) if is_daylight else 0,
        'udc3': random.uniform(300, 800) if is_daylight else 0,
        'riso1': random.uniform(1000, 10000),
        'riso2': random.uniform(1000, 10000),
        'riso_plus': random.uniform(1000, 10000),
        'riso_minus': random.uniform(1000, 10000),
        'iac1': random.uniform(0, 25) * sun_factor,
        'iac2': random.uniform(0, 25) * sun_factor,
        'iac3': random.uniform(0, 25) * sun_factor,
        'uac1': random.uniform(220, 240),
        'uac2': random.uniform(220, 240),
        'uac3': random.uniform(220, 240),
        'pac1': int(base_power / 3 + random.uniform(-50, 50)),
        'pac2': int(base_power / 3 + random.uniform(-50, 50)),
        'pac3': int(base_power / 3 + random.uniform(-50, 50)),
        'pac1r': int(base_power / 3 * 0.1),
        'pac2r': int(base_power / 3 * 0.1),
        'pac3r': int(base_power / 3 * 0.1),
        'pac_tot': int(base_power),
        'fac1': random.uniform(49.8, 50.2),
        'fac2': random.uniform(49.8, 50.2),
        'fac3': random.uniform(49.8, 50.2),
        'eac': random.uniform(0, 50000),
        'ton': random.uniform(0, 86400),
        'tntcdc': random.uniform(20, 80),
        'r_mov1': random.uniform(0, 1000),
        'r_mov2': random.uniform(0, 1000),
        'tntcac': random.uniform(20, 80),
        'uacc': random.uniform(220, 240),
        'facc': random.uniform(49.8, 50.2),
        'e_total': random.uniform(0, 1000000),
        'ron_day': random.uniform(0, 86400),
        'ron_tot': random.uniform(0, 1000000),
        'status_global': random.randint(0, 65535),
        'status_dc': random.randint(0, 65535),
        'lim_dc': random.randint(0, 65535),
        'status_ac': random.randint(0, 65535),
        'lim_ac': random.randint(0, 65535),
        'status_iso': random.randint(0, 65535),
        'dc_err': random.randint(0, 65535),
        'ac_err': random.randint(0, 65535),
        'sc_err': random.randint(0, 65535),
        'bulk_err': random.randint(0, 65535),
        'com_err': random.randint(0, 65535),
        'sc_dis': random.randint(0, 65535),
        'err_hw_dc': random.randint(0, 65535),
        'status_kalib': random.randint(0, 65535),
        'status_null': random.randint(0, 65535),
        'pdc1': random.uniform(0, 2000) * sun_factor,
        'pdc2': random.uniform(0, 2000) * sun_factor,
        'pdc3': random.uniform(0, 2000) * sun_factor,
        'bus_v_plus': random.uniform(300, 400),
        'bus_v_minus': random.uniform(300, 400),
        't_calc': random.uniform(20, 80),
        't_sink': random.uniform(20, 80),
        'status_ac1': random.randint(-128, 127),
        'status_ac2': random.randint(-128, 127),
        'status_ac3': random.randint(-128, 127),
        'status_ac4': random.randint(-128, 127),
        'status_dc1': random.randint(-128, 127),
        'status_dc2': random.randint(-128, 127),
        'error_status': random.randint(-2147483648, 2147483647),
        'error_ac1': random.randint(-2147483648, 2147483647),
        'global_err_1': random.randint(-2147483648, 2147483647),
        'cpu_error': random.randint(-2147483648, 2147483647),
        'global_err_2': random.randint(-2147483648, 2147483647),
        'limits_ac1': random.randint(-2147483648, 2147483647),
        'limits_ac2': random.randint(-2147483648, 2147483647),
        'global_err_3': random.randint(-2147483648, 2147483647),
        'eint': random.randint(-2147483648, 2147483647),
        'limits_dc1': random.randint(-2147483648, 2147483647),
        'limits_dc2': random.randint(-2147483648, 2147483647),
        'qac1': random.uniform(-1000, 1000),
        'qac2': random.uniform(-1000, 1000),
        'qac3': random.uniform(-1000, 1000),
        'tamb': random.uniform(10, 40),
        'theat': random.uniform(20, 80),
        'status_1': random.randint(-128, 127),
        'status_2': random.randint(-128, 127),
        'status_3': random.randint(-128, 127),
        'status_4': random.randint(-128, 127),
        'internal_status_1': random.randint(-128, 127),
        'internal_status_2': random.randint(-128, 127),
        'internal_status_3': random.randint(-128, 127),
        'internal_status_4': random.randint(-128, 127),
        'event1': random.randint(-2147483648, 2147483647),
        'operatingstate': random.randint(-2147483648, 2147483647),
        'actualpower': int(base_power),
        'udc4': random.uniform(300, 800) if is_daylight else 0,
        'idc4': random.uniform(0, 20) * sun_factor,
        'pdc4': random.uniform(0, 2000) * sun_factor,
        'udc5': random.uniform(300, 800) if is_daylight else 0,
        'idc5': random.uniform(0, 20) * sun_factor,
        'pdc5': random.uniform(0, 2000) * sun_factor,
        'udc6': random.uniform(300, 800) if is_daylight else 0,
        'idc6': random.uniform(0, 20) * sun_factor,
        'pdc6': random.uniform(0, 2000) * sun_factor,
        'insertat': datetime.now(),
        'protocoltype': random.choice([True, False]),
        'pac1new': int(base_power / 3 + random.uniform(-50, 50)),
        'systeminserted': datetime.now()
    }

def insert_data(thread_id, rows_per_thread, config, base_time):
    """Insert data in a separate thread"""
    try:
        conn = pymysql.connect(
            host=config['AURORA_HOST'],
            user=config['AURORA_USER'],
            password=config['AURORA_PASSWORD'],
            database=config['AURORA_DATABASE'],
            autocommit=False
        )
        
        cur = conn.cursor()
        
        insert_sql = """
            INSERT INTO pv_benchmark (
                solarstations_id, device, uhrzeit, s, adresse, serien_nummer, mpc,
                idc1, idc2, idc3, udc1, udc2, udc3, riso1, riso2, riso_plus, riso_minus,
                iac1, iac2, iac3, uac1, uac2, uac3, pac1, pac2, pac3, pac1r, pac2r, pac3r,
                pac_tot, fac1, fac2, fac3, eac, ton, tntcdc, r_mov1, r_mov2, tntcac,
                uacc, facc, e_total, ron_day, ron_tot, status_global, status_dc, lim_dc,
                status_ac, lim_ac, status_iso, dc_err, ac_err, sc_err, bulk_err, com_err,
                sc_dis, err_hw_dc, status_kalib, status_null, pdc1, pdc2, pdc3,
                bus_v_plus, bus_v_minus, t_calc, t_sink, status_ac1, status_ac2,
                status_ac3, status_ac4, status_dc1, status_dc2, error_status, error_ac1,
                global_err_1, cpu_error, global_err_2, limits_ac1, limits_ac2,
                global_err_3, eint, limits_dc1, limits_dc2, qac1, qac2, qac3,
                tamb, theat, status_1, status_2, status_3, status_4,
                internal_status_1, internal_status_2, internal_status_3, internal_status_4,
                event1, operatingstate, actualpower, udc4, idc4, pdc4, udc5, idc5, pdc5,
                udc6, idc6, pdc6, insertat, protocoltype, pac1new, systeminserted
            ) VALUES ({})
        """.format(', '.join(['%s'] * 103))
        
        batch_data = []
        for i in range(rows_per_thread):
            data = generate_solar_data(base_time, i + thread_id * rows_per_thread)
            
            row_data = (
                data['solarstations_id'], data['device'], data['uhrzeit'], data['s'],
                data['adresse'], data['serien_nummer'], data['mpc'], data['idc1'],
                data['idc2'], data['idc3'], data['udc1'], data['udc2'], data['udc3'],
                data['riso1'], data['riso2'], data['riso_plus'], data['riso_minus'],
                data['iac1'], data['iac2'], data['iac3'], data['uac1'], data['uac2'],
                data['uac3'], data['pac1'], data['pac2'], data['pac3'], data['pac1r'],
                data['pac2r'], data['pac3r'], data['pac_tot'], data['fac1'], data['fac2'],
                data['fac3'], data['eac'], data['ton'], data['tntcdc'], data['r_mov1'],
                data['r_mov2'], data['tntcac'], data['uacc'], data['facc'], data['e_total'],
                data['ron_day'], data['ron_tot'], data['status_global'], data['status_dc'],
                data['lim_dc'], data['status_ac'], data['lim_ac'], data['status_iso'],
                data['dc_err'], data['ac_err'], data['sc_err'], data['bulk_err'],
                data['com_err'], data['sc_dis'], data['err_hw_dc'], data['status_kalib'],
                data['status_null'], data['pdc1'], data['pdc2'], data['pdc3'],
                data['bus_v_plus'], data['bus_v_minus'], data['t_calc'], data['t_sink'],
                data['status_ac1'], data['status_ac2'], data['status_ac3'], data['status_ac4'],
                data['status_dc1'], data['status_dc2'], data['error_status'], data['error_ac1'],
                data['global_err_1'], data['cpu_error'], data['global_err_2'],
                data['limits_ac1'], data['limits_ac2'], data['global_err_3'], data['eint'],
                data['limits_dc1'], data['limits_dc2'], data['qac1'], data['qac2'],
                data['qac3'], data['tamb'], data['theat'], data['status_1'], data['status_2'],
                data['status_3'], data['status_4'], data['internal_status_1'],
                data['internal_status_2'], data['internal_status_3'], data['internal_status_4'],
                data['event1'], data['operatingstate'], data['actualpower'], data['udc4'],
                data['idc4'], data['pdc4'], data['udc5'], data['idc5'], data['pdc5'],
                data['udc6'], data['idc6'], data['pdc6'], data['insertat'],
                data['protocoltype'], data['pac1new'], data['systeminserted']
            )
            
            batch_data.append(row_data)
            
            # Insert in batches of 1000
            if len(batch_data) >= 1000:
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                batch_data = []
        
        # Insert remaining data
        if batch_data:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Thread {thread_id} error: {e}")

def main():
    print("Starting Aurora MySQL solar inverter benchmark...")
    
    config = load_config()
    
    # Connect to Aurora MySQL
    conn = pymysql.connect(
        host=config['AURORA_HOST'],
        user=config['AURORA_USER'],
        password=config['AURORA_PASSWORD'],
        database=config['AURORA_DATABASE']
    )
    
    cur = conn.cursor()
    
    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pv_benchmark (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        solarstations_id INT,
        device VARCHAR(50),
        uhrzeit TIMESTAMP,
        s TINYINT,
        adresse TINYINT UNSIGNED,
        serien_nummer VARCHAR(50),
        mpc TINYINT UNSIGNED,
        idc1 FLOAT, idc2 FLOAT, idc3 FLOAT,
        udc1 FLOAT, udc2 FLOAT, udc3 FLOAT,
        riso1 FLOAT, riso2 FLOAT, riso_plus FLOAT, riso_minus FLOAT,
        iac1 FLOAT, iac2 FLOAT, iac3 FLOAT,
        uac1 FLOAT, uac2 FLOAT, uac3 FLOAT,
        pac1 INT, pac2 INT, pac3 INT,
        pac1r INT, pac2r INT, pac3r INT,
        pac_tot INT,
        fac1 FLOAT, fac2 FLOAT, fac3 FLOAT,
        eac FLOAT, ton FLOAT, tntcdc FLOAT,
        r_mov1 FLOAT, r_mov2 FLOAT, tntcac FLOAT,
        uacc FLOAT, facc FLOAT, e_total FLOAT,
        ron_day FLOAT, ron_tot FLOAT,
        status_global SMALLINT UNSIGNED, status_dc SMALLINT UNSIGNED, lim_dc SMALLINT UNSIGNED,
        status_ac SMALLINT UNSIGNED, lim_ac SMALLINT UNSIGNED, status_iso SMALLINT UNSIGNED,
        dc_err SMALLINT UNSIGNED, ac_err SMALLINT UNSIGNED, sc_err SMALLINT UNSIGNED,
        bulk_err SMALLINT UNSIGNED, com_err SMALLINT UNSIGNED, sc_dis SMALLINT UNSIGNED,
        err_hw_dc SMALLINT UNSIGNED, status_kalib SMALLINT UNSIGNED, status_null SMALLINT UNSIGNED,
        pdc1 FLOAT, pdc2 FLOAT, pdc3 FLOAT,
        bus_v_plus FLOAT, bus_v_minus FLOAT, t_calc FLOAT, t_sink FLOAT,
        status_ac1 TINYINT, status_ac2 TINYINT, status_ac3 TINYINT, status_ac4 TINYINT,
        status_dc1 TINYINT, status_dc2 TINYINT,
        error_status INT, error_ac1 INT, global_err_1 INT, cpu_error INT, global_err_2 INT,
        limits_ac1 INT, limits_ac2 INT, global_err_3 INT, eint INT,
        limits_dc1 INT, limits_dc2 INT,
        qac1 FLOAT, qac2 FLOAT, qac3 FLOAT,
        tamb FLOAT, theat FLOAT,
        status_1 TINYINT, status_2 TINYINT, status_3 TINYINT, status_4 TINYINT,
        internal_status_1 TINYINT, internal_status_2 TINYINT, internal_status_3 TINYINT, internal_status_4 TINYINT,
        event1 INT, operatingstate INT, actualpower INT,
        udc4 FLOAT, idc4 FLOAT, pdc4 FLOAT,
        udc5 FLOAT, idc5 FLOAT, pdc5 FLOAT,
        udc6 FLOAT, idc6 FLOAT, pdc6 FLOAT,
        insertat TIMESTAMP,
        protocoltype BOOLEAN,
        pac1new INT,
        systeminserted TIMESTAMP,
        INDEX idx_uhrzeit (uhrzeit),
        INDEX idx_device (device),
        INDEX idx_solarstations_id (solarstations_id)
    ) ENGINE=InnoDB 
    PARTITION BY RANGE COLUMNS (`uhrzeit`)
    (
    PARTITION `p2023` VALUES LESS THAN ('2023-01-01 00:00:00'),
    PARTITION `p2024` VALUES LESS THAN ('2024-01-01 00:00:00'), 
    PARTITION `p2025` VALUES LESS THAN ('2025-01-01 00:00:00'),
    PARTITION `p2026` VALUES LESS THAN ('2026-01-01 00:00:00'), 
    PARTITION `p2027` VALUES LESS THAN ('2027-01-01 00:00:00'), 
    PARTITION `p2028` VALUES LESS THAN ('2028-01-01 00:00:00'), 
    PARTITION `pfuture` VALUES LESS THAN (MAXVALUE)
    );
    """
    
    # cur.execute(create_table_sql)
    print("Aurora MySQL pv_benchmark table setup complete")
    
    # Get benchmark parameters
    total_rows = int(config['BENCHMARK_ROWS'])
    num_threads = int(config['BENCHMARK_THREADS'])
    rows_per_thread = total_rows // num_threads
    
    base_time = datetime.now() - timedelta(days=30)
    
    print(f"Inserting {total_rows} rows using {num_threads} threads...")
    
    start_time = time.time()
    
    # Create and start threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=insert_data, args=(i, rows_per_thread, config, base_time))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Get final count
    cur.execute("SELECT COUNT(*) FROM pv_benchmark")
    final_count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    print(f"Aurora MySQL Benchmark Complete:")
    print(f"Total rows: {final_count}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Rows per second: {final_count/total_time:.2f}")

if __name__ == "__main__":
    main()
AURORA_SCRIPT

chmod +x /mnt/benchmark/scripts/aurora_benchmark.py

# Create main benchmark runner script
cat > /mnt/benchmark/run_benchmark.sh <<'BENCHMARK_SCRIPT'
#!/bin/bash
set -e

# Parse JSON config for shell script
BENCHMARK_ROWS=$(python3 -c "import json; print(json.load(open('/mnt/benchmark/config/benchmark_config.json'))['BENCHMARK_ROWS'])")
BENCHMARK_THREADS=$(python3 -c "import json; print(json.load(open('/mnt/benchmark/config/benchmark_config.json'))['BENCHMARK_THREADS'])")

echo "Starting Database Benchmark Comparison"
echo "======================================="
echo "Rows: $BENCHMARK_ROWS"
echo "Threads: $BENCHMARK_THREADS"
echo "Start Time: $(date)"
echo ""

# Create results directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="/mnt/benchmark/results/$TIMESTAMP"
mkdir -p "$RESULTS_DIR"

# Function to run benchmark and capture metrics
run_benchmark() {
    local db_name=$1
    local script_path=$2
    local result_file="$RESULTS_DIR/${db_name}_results.txt"
    
    echo "Running $db_name benchmark..."
    echo "================================" > "$result_file"
    echo "$db_name Benchmark Results" >> "$result_file"
    echo "Start Time: $(date)" >> "$result_file"
    echo "" >> "$result_file"
    
    # Capture system metrics before
    echo "System Metrics (Before):" >> "$result_file"
    free -h >> "$result_file"
    echo "" >> "$result_file"
    
    # Run the benchmark
    start_time=$(date +%s)
    python3 "$script_path" 2>&1 | tee -a "$result_file"
    end_time=$(date +%s)
    
    # Calculate total time
    total_time=$((end_time - start_time))
    echo "" >> "$result_file"
    echo "Total Execution Time: ${total_time} seconds" >> "$result_file"
    echo "End Time: $(date)" >> "$result_file"
    
    # Capture system metrics after
    echo "" >> "$result_file"
    echo "System Metrics (After):" >> "$result_file"
    free -h >> "$result_file"
    
    echo "$db_name benchmark completed in ${total_time} seconds"
    echo ""
}

# Run TimescaleDB benchmark
run_benchmark "TimescaleDB" "/mnt/benchmark/scripts/timescaledb_benchmark.py"

# Wait a bit between benchmarks
echo "Waiting 30 seconds before next benchmark..."
sleep 10

# Run Aurora MySQL benchmark
run_benchmark "Aurora_MySQL" "/mnt/benchmark/scripts/aurora_benchmark.py"

# Generate comparison report
echo "Generating comparison report..."
cat > "$RESULTS_DIR/comparison_report.txt" <<'REPORT'
Database Benchmark Comparison Report
====================================

This report compares the performance of TimescaleDB vs Aurora MySQL
for solar inverter data ingestion with the pv_benchmark schema.

Test Configuration:
- Rows: $BENCHMARK_ROWS
- Threads: $BENCHMARK_THREADS
- Schema: Solar inverter pv_benchmark (103 columns)
- Data: Realistic solar generation patterns with time-based variations

Results Summary:
- TimescaleDB: See TimescaleDB_results.txt
- Aurora MySQL: See Aurora_MySQL_results.txt

Key Metrics to Compare:
1. Total insertion time
2. Rows per second throughput
3. Memory usage patterns
4. System resource utilization

REPORT

echo "Benchmark completed! Results saved to: $RESULTS_DIR"
echo "View results with: ls -la $RESULTS_DIR"
BENCHMARK_SCRIPT

chmod +x /mnt/benchmark/run_benchmark.sh

echo "Benchmark setup complete!"
