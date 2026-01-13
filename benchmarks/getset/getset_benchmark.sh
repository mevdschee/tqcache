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

    rm -rf /tmp/tqcache-bench
    rm -rf /tmp/redis-bench
    rm -f max_rss.tmp cpu_start.tmp cpu_time.tmp results.tmp
    rm -f tqcache-server benchmark-tool
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
echo "Building TQCache and Benchmark Tool..."
go build -o tqcache-server ../../cmd/tqcache
go build -o benchmark-tool .

# Benchmark Configuration
CLIENTS=10
REQUESTS=100000
SIZE=10240
KEYS=100000

# Get CPU time (user + system) from /proc/PID/stat in jiffies
get_cpu_time() {
    PID=$1
    if [ -f /proc/$PID/stat ]; then
        # Fields 14 and 15 are utime and stime in jiffies
        awk '{print $14 + $15}' /proc/$PID/stat 2>/dev/null || echo 0
    else
        echo 0
    fi
}

# Function to monitor max RSS of a PID and track CPU time
start_monitor() {
    PID=$1
    echo 0 > max_rss.tmp
    # Record start CPU time and wall clock
    get_cpu_time $PID > cpu_start.tmp
    echo $(date +%s%N) >> cpu_start.tmp
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
    MONITOR_TARGET_PID=$1
    if [ ! -z "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
        wait $MONITOR_PID 2>/dev/null || true
    fi
    
    # Get max memory
    MAX_KB=$(cat max_rss.tmp)
    MAX_MB=$((MAX_KB / 1024))
    
    # Calculate CPU percentage
    CPU_START=$(head -1 cpu_start.tmp)
    TIME_START=$(tail -1 cpu_start.tmp)
    CPU_END=$(get_cpu_time $MONITOR_TARGET_PID)
    TIME_END=$(date +%s%N)
    
    # CPU time in jiffies (typically 100 Hz = 10ms per jiffy)
    CPU_JIFFIES=$((CPU_END - CPU_START))
    # Wall time in nanoseconds, convert to centiseconds (100ths of a second, same as jiffies at 100Hz)
    WALL_NS=$((TIME_END - TIME_START))
    WALL_CS=$((WALL_NS / 10000000))
    
    if [ "$WALL_CS" -gt 0 ]; then
        # CPU percentage = (cpu_jiffies / wall_centiseconds) * 100
        CPU_PCT=$((CPU_JIFFIES * 100 / WALL_CS))
    else
        CPU_PCT=0
    fi
    
    echo "$MAX_MB,$CPU_PCT"
}

run_benchmark_set() {
    SYNC_MODE=$1
    SYNC_INTERVAL=$2
    REDIS_FLAGS=$3
    ENABLE_MEMCACHED=$4
    REQ_COUNT=${5:-$REQUESTS}  # Use 5th param or default to REQUESTS
    SHARD_COUNT=$6

    echo "==========================================================="
    echo "Running Benchmark Mode: $SYNC_MODE (requests: $REQ_COUNT, shards: $SHARD_COUNT)"
    echo "==========================================================="

    # Ensure ports are free
    kill_port 11221
    kill_port 6380
    kill_port 11222

    # --- Start TQCache ---
    echo "Starting TQCache (Sync Mode: $SYNC_MODE, Interval: $SYNC_INTERVAL, Shards: $SHARD_COUNT)..."
    rm -rf /tmp/tqcache-bench
    mkdir -p /tmp/tqcache-bench
    
    # Generate config file with current sync mode and shard count
    cat > /tmp/tqcache-bench.conf << CONF
[server]
listen = :11221

[storage]
data-dir = /tmp/tqcache-bench
shards = $SHARD_COUNT
sync-mode = $SYNC_MODE
sync-interval = $SYNC_INTERVAL
max-data-size = 1GB
CONF
    
    ./tqcache-server -config /tmp/tqcache-bench.conf > /dev/null 2>&1 &
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

    # TQCache
    echo "Benchmarking TQCache..."
    start_monitor $TQ_PID
    ./benchmark-tool -host localhost:11221 -protocol memcache -label "TQCache" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQ_COUNT -size $SIZE -keys $KEYS -csv > results.tmp
    STATS=$(stop_monitor $TQ_PID)
    awk -v stats="$STATS" '{print $0 "," stats}' results.tmp >> $OUTPUT

    # Redis
    echo "Benchmarking Redis..."
    start_monitor $REDIS_PID
    ./benchmark-tool -host localhost:6380 -protocol redis -label "Redis" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQ_COUNT -size $SIZE -keys $KEYS -csv > results.tmp
    STATS=$(stop_monitor $REDIS_PID)
    awk -v stats="$STATS" '{print $0 "," stats}' results.tmp >> $OUTPUT

    # Memcached
    if [ "$ENABLE_MEMCACHED" = "true" ]; then
        echo "Benchmarking Memcached..."
        start_monitor $MEM_PID
        ./benchmark-tool -host localhost:11222 -protocol memcache -label "Memcached" -mode "$SYNC_MODE" -clients $CLIENTS -requests $REQ_COUNT -size $SIZE -keys $KEYS -csv > results.tmp
        STATS=$(stop_monitor $MEM_PID)
        awk -v stats="$STATS" '{print $0 "," stats}' results.tmp >> $OUTPUT
    fi

    # Cleanup processes for next round
    kill $TQ_PID 2>/dev/null || true
    kill $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then kill $MEM_PID 2>/dev/null || true; fi
    wait $TQ_PID 2>/dev/null || true
    wait $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then wait $MEM_PID 2>/dev/null || true; fi
}

generate_visualization() {
    SHARD_COUNT=$1
    CSV_FILE="getset_benchmark_${SHARD_COUNT}.csv"
    PNG_FILE="getset_benchmark_${SHARD_COUNT}.png"

python3 << EOF
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
df = pd.read_csv('${CSV_FILE}')
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

cpu_df = df[(df['Operation'] == 'SET') & (df['Mode'] == 'none')]
cpu_pivot = cpu_df.pivot(index='Mode', columns='Backend', values='CPU(%)')

# Plotting - 2x2 grid
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))

# Top row: Performance
# Plot 1: Write Performance (SET)
write_pivot.plot(kind='bar', ax=ax1, width=0.9, rot=0, legend=True)
ax1.set_title('Write Performance (SET)')
ax1.set_ylabel('Requests Per Second (RPS)')
ax1.set_xlabel('Persistence Mode')
ax1.grid(axis='y', linestyle='--', alpha=0.7)
ax1.legend(title='Backend', loc='upper right')
annotate_bars(ax1)

# Plot 2: Read Performance (GET)
read_pivot.plot(kind='bar', ax=ax2, width=0.9, rot=0, legend=False)
ax2.set_title('Read Performance (GET)')
ax2.set_ylabel('Requests Per Second (RPS)')
ax2.set_xlabel('Persistence Mode')
ax2.grid(axis='y', linestyle='--', alpha=0.7)
annotate_bars(ax2)

# Bottom row: Resource Usage
# Plot 3: Memory Usage
mem_pivot.plot(kind='bar', ax=ax3, width=0.9, rot=0, legend=False)
ax3.set_title('Peak Memory Usage')
ax3.set_ylabel('Megabytes (MB)')
ax3.set_xlabel('Persistence Mode')
ax3.grid(axis='y', linestyle='--', alpha=0.7)
annotate_bars(ax3)

# Plot 4: CPU Usage
cpu_pivot.plot(kind='bar', ax=ax4, width=0.9, rot=0, legend=False)
ax4.set_title('CPU Usage')
ax4.set_ylabel('CPU (%)')
ax4.set_xlabel('Persistence Mode')
ax4.grid(axis='y', linestyle='--', alpha=0.7)
annotate_bars(ax4)

# Increase y-limit to fit vertical labels
for ax in (ax1, ax2, ax3, ax4):
    ylim = ax.get_ylim()
    ax.set_ylim(0, ylim[1] * 1.15)

plt.suptitle('TQCache Performance Benchmark (Shards: ${SHARD_COUNT})', fontsize=16)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('${PNG_FILE}', dpi=150, bbox_inches='tight')
print(f"Saved: ${PNG_FILE}")
EOF
}

# Main loop: run benchmarks for shards 4, 8, 12, 16, 24, 32
for SHARD_COUNT in 4 8 12 16 24 32; do
    echo ""
    echo "###########################################################"
    echo "# BENCHMARKING WITH $SHARD_COUNT SHARD(S)"
    echo "###########################################################"
    echo ""

    # Output file for this shard count
    OUTPUT="getset_benchmark_${SHARD_COUNT}.csv"
    echo "Mode,Backend,Protocol,Operation,RPS,TimePerReq(ms),MaxMemory(MB),CPU(%)" > $OUTPUT

    # 1. Mode: none
    run_benchmark_set "none" "1s" "--save \"\" --appendonly no" "true" $REQUESTS $SHARD_COUNT

    # 2. Mode: periodic (1s sync)  
    run_benchmark_set "periodic" "1s" "--appendonly yes --appendfsync everysec" "false" $REQUESTS $SHARD_COUNT

    # 3. Mode: always (fsync every write) - reduced requests for faster benchmark
    run_benchmark_set "always" "1s" "--appendonly yes --appendfsync always" "false" 10000 $SHARD_COUNT

    echo "---------------------------------------------------"
    echo "Benchmark completed for $SHARD_COUNT shard(s). Results saved to $OUTPUT"
    echo "---------------------------------------------------"
    column -s, -t $OUTPUT

    # Generate PNG
    echo ""
    echo "Generating visualization for $SHARD_COUNT shard(s)..."
    generate_visualization $SHARD_COUNT
done

echo ""
echo "============================================="
echo "All benchmarks completed!"
echo "Generated files:"
for i in 1 2 4 8; do
    echo "  - getset_benchmark_${i}.csv"
    echo "  - getset_benchmark_${i}.png"
done
echo "============================================="
echo "Done!"