#!/bin/bash
set -e

# Helper to kill process on a port
kill_port() {
    PORT=$1
    if command -v lsof >/dev/null; then
        PIDS=$(lsof -t -i:$PORT || true)
        if [ ! -z "$PIDS" ]; then
            echo "Force killing process(es) on port $PORT: $PIDS"
            kill -9 $PIDS 2>/dev/null || true
        fi
    fi
}

# Cleanup function
cleanup() {
    echo "Stopping servers..."
    if [ ! -z "$TQ_PID" ]; then kill $TQ_PID 2>/dev/null || true; fi
    if [ ! -z "$MEM_PID" ]; then kill $MEM_PID 2>/dev/null || true; fi
    if [ ! -z "$REDIS_PID" ]; then kill $REDIS_PID 2>/dev/null || true; fi
    
    # Force cleanup ports
    kill_port 11221
    kill_port 6380
    kill_port 11222

    rm -rf /tmp/tqsession-bench
    rm -rf /tmp/redis-bench
    rm -f max_rss.tmp results.tmp
    rm -f tqsession-server benchmark-tool
    rm -f redis_bench.log
}
trap cleanup EXIT

# Check dependencies
if ! command -v memcached &> /dev/null; then
    echo "Error: memcached is not installed or not in PATH."
    exit 1
fi
if ! command -v redis-server &> /dev/null; then
    echo "Error: redis-server is not installed or not in PATH."
    exit 1
fi

# Build
echo "Building TQSession and Benchmark Tool..."
go build -o tqsession-server ../../cmd/tqsession
go build -o benchmark-tool .

# Output file
OUTPUT="getset_benchmark.csv"
echo "Mode,Backend,Protocol,Operation,RPS,TimePerReq(ms),MaxMemory(MB)" > $OUTPUT

# Benchmark Configuration
CLIENTS=10
REQUESTS=100000
SIZE=10240
KEYS=100000

# Function to monitor max RSS of a PID
start_monitor() {
    PID=$1
    echo 0 > max_rss.tmp
    (
        while true; do
            if ! kill -0 $PID 2>/dev/null; then break; fi
            rss=$(ps -o rss= -p $PID 2>/dev/null | tr -d ' ' || echo 0)
            if [ -z "$rss" ]; then rss=0; fi
            
            cur_max=$(cat max_rss.tmp)
            if [ "$rss" -gt "$cur_max" ]; then
                echo $rss > max_rss.tmp
            fi
            sleep 0.1
        done
    ) &
    MONITOR_PID=$!
}

stop_monitor() {
    if [ ! -z "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
        wait $MONITOR_PID 2>/dev/null || true
    fi
    MAX_KB=$(cat max_rss.tmp)
    echo $((MAX_KB / 1024))
}

run_benchmark_set() {
    SYNC_MODE=$1
    SYNC_INTERVAL=$2
    REDIS_FLAGS=$3
    ENABLE_MEMCACHED=$4

    echo "==========================================================="
    echo "Running Benchmark Mode: $SYNC_MODE"
    echo "==========================================================="

    # Ensure ports are free
    kill_port 11221
    kill_port 6380
    kill_port 11222

    # --- Start TQSession ---
    echo "Starting TQSession (Sync Mode: $SYNC_MODE, Interval: $SYNC_INTERVAL)..."
    rm -rf /tmp/tqsession-bench
    mkdir -p /tmp/tqsession-bench
    
    # Generate config file with current sync mode
    cat > /tmp/tqsession-bench.conf << CONF
[server]
listen = :11221

[storage]
data-dir = /tmp/tqsession-bench
shards = 16
sync-mode = $SYNC_MODE
sync-interval = $SYNC_INTERVAL
max-data-size = 1GB
CONF
    
    ./tqsession-server -config /tmp/tqsession-bench.conf > /dev/null 2>&1 &
    TQ_PID=$!

    # --- Start Redis ---
    echo "Starting Redis (Flags: $REDIS_FLAGS)..."
    mkdir -p /tmp/redis-bench
    rm -f /tmp/redis-bench/dump.rdb /tmp/redis-bench/appendonly.aof
    redis-server --port 6380 --dir /tmp/redis-bench --daemonize no --protected-mode no $REDIS_FLAGS > /dev/null 2>&1 &
    REDIS_PID=$!

    # --- Start Memcached ---
    MEM_PID=""
    if [ "$ENABLE_MEMCACHED" = "true" ]; then
        echo "Starting Memcached..."
        memcached -p 11222 -m 2048 -u $(whoami) > /dev/null 2>&1 &
        MEM_PID=$!
    fi

    # Wait for startup
    sleep 3

    # --- Run Benchmarks ---

    # TQSession
    echo "Benchmarking TQSession..."
    start_monitor $TQ_PID
    ./benchmark-tool -host localhost:11221 -protocol memcache -label "TQSession" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
    MEM_MB=$(stop_monitor)
    awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT

    # Redis
    echo "Benchmarking Redis..."
    start_monitor $REDIS_PID
    ./benchmark-tool -host localhost:6380 -protocol redis -label "Redis" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
    MEM_MB=$(stop_monitor)
    awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT

    # Memcached
    if [ "$ENABLE_MEMCACHED" = "true" ]; then
        echo "Benchmarking Memcached..."
        start_monitor $MEM_PID
        ./benchmark-tool -host localhost:11222 -protocol memcache -label "Memcached" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
        MEM_MB=$(stop_monitor)
        awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT
    fi

    # Cleanup processes for next round
    kill $TQ_PID 2>/dev/null || true
    kill $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then kill $MEM_PID 2>/dev/null || true; fi
    wait $TQ_PID 2>/dev/null || true
    wait $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then wait $MEM_PID 2>/dev/null || true; fi
}

# 1. Mode: none
run_benchmark_set "none" "1s" "--save \"\" --appendonly no" "true"

# 2. Mode: periodic (1s sync)  
run_benchmark_set "periodic" "1s" "--appendonly yes --appendfsync everysec" "false"

# 3. Mode: always (fsync every write)
run_benchmark_set "always" "1s" "--appendonly yes --appendfsync always" "false"

echo "---------------------------------------------------"
echo "Benchmark completed. Results saved to $OUTPUT"
echo "---------------------------------------------------"
column -s, -t $OUTPUT

# Generate PNG
echo ""
echo "Generating visualization..."

python3 << 'EOF'
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def annotate_bars(ax):
    for p in ax.patches:
        if p.get_height() > 0:
            ax.annotate(f'{int(p.get_height())}', 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='bottom', fontsize=8, rotation=90, xytext=(0, 5), 
                        textcoords='offset points')

# Load data
df = pd.read_csv('getset_benchmark.csv')
df.columns = [c.strip() for c in df.columns]
for col in ['Mode', 'Backend', 'Protocol', 'Operation']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# Prepare Data
write_df = df[df['Operation'] == 'SET']
write_pivot = write_df.pivot(index='Mode', columns='Backend', values='RPS')
modes_order = ['none', 'periodic', 'always']
existing_modes = [m for m in modes_order if m in write_pivot.index]
write_pivot = write_pivot.reindex(existing_modes)

read_df = df[(df['Operation'] == 'GET') & (df['Mode'] == 'none')]
read_pivot = read_df.pivot(index='Mode', columns='Backend', values='RPS')

mem_df = df[(df['Operation'] == 'SET') & (df['Mode'] == 'none')]
mem_pivot = mem_df.pivot(index='Mode', columns='Backend', values='MaxMemory(MB)')

# Plotting
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 6), gridspec_kw={'width_ratios': [3, 1, 1]})

# Plot 1: Write Performance
write_pivot.plot(kind='bar', ax=ax1, width=0.9, rot=0, legend=True)
ax1.set_title('Write Performance (SET)')
ax1.set_ylabel('Requests Per Second (RPS)')
ax1.set_xlabel('Persistence Mode')
ax1.grid(axis='y', linestyle='--', alpha=0.7)
ax1.legend(title='Backend', loc='upper right')
annotate_bars(ax1)

# Plot 2: Read Performance
read_pivot.plot(kind='bar', ax=ax2, width=0.9, rot=0, legend=False)
ax2.set_title('Read Performance (GET)')
ax2.set_xlabel('Persistence Mode')
ax2.grid(axis='y', linestyle='--', alpha=0.7)
annotate_bars(ax2)

# Plot 3: Memory Usage
mem_pivot.plot(kind='bar', ax=ax3, width=0.9, rot=0, legend=False)
ax3.set_title('Peak Memory Usage')
ax3.set_ylabel('Megabytes (MB)')
ax3.set_xlabel('Persistence Mode')
ax3.grid(axis='y', linestyle='--', alpha=0.7)
annotate_bars(ax3)

# Increase y-limit to fit vertical labels
for ax in (ax1, ax2, ax3):
    ylim = ax.get_ylim()
    ax.set_ylim(0, ylim[1] * 1.15)

plt.suptitle('TQSession Performance Benchmark', fontsize=16)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('getset_benchmark.png', dpi=150, bbox_inches='tight')
print(f"Saved: getset_benchmark.png")
EOF

echo ""
echo "Done!"