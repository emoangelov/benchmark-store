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
        'systeminserted': 0
    }

def insert_data(thread_id, rows_per_thread):
    conn = create_connection()
    cur = conn.cursor()
    
    start_time = time.time()
    base_time = datetime.now() - timedelta(days=30)
    
    # Prepare the insert statement
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
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

# Create Aurora MySQL benchmark script (truncated for brevity - similar structure)
cat > /mnt/benchmark/scripts/aurora_benchmark.py <<'AURORA_SCRIPT'
import pymysql
import time
import threading
import random
import json
from datetime import datetime, timedelta

# Load configuration
with open('/mnt/benchmark/config/benchmark_config.json', 'r') as f:
    config = json.load(f)

def create_connection():
    return pymysql.connect(
        host=config['AURORA_HOST'],
        user=config['AURORA_USER'],
        password=config['AURORA_PASSWORD'],
        database=config['AURORA_DATABASE'],
        port=3306,
        autocommit=False
    )

# Similar implementation as TimescaleDB but for MySQL
# ... (implementation details similar to above)

if __name__ == "__main__":
    print("Starting Aurora MySQL solar inverter benchmark...")
    # Implementation here
AURORA_SCRIPT

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
sleep 30

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
