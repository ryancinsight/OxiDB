# OxiDB Production Deployment Guide

## Table of Contents
1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Performance Tuning](#performance-tuning)
5. [Security](#security)
6. [Monitoring](#monitoring)
7. [Backup and Recovery](#backup-and-recovery)
8. [High Availability](#high-availability)
9. [Troubleshooting](#troubleshooting)
10. [Best Practices](#best-practices)

## System Requirements

### Hardware Requirements
- **CPU**: Minimum 2 cores, recommended 4+ cores for production workloads
- **RAM**: Minimum 4GB, recommended 16GB+ for optimal buffer pool performance
- **Storage**: SSD recommended for database files
  - 10GB minimum for system
  - Additional space based on data requirements
  - 2x data size for WAL and recovery operations

### Software Requirements
- **Operating System**: Linux (kernel 4.9+), macOS 10.14+, Windows 10+
- **Rust**: 1.70+ (for building from source)
- **File System**: ext4, XFS, APFS, or NTFS with journaling enabled

## Installation

### From Source
```bash
# Clone the repository
git clone https://github.com/yourusername/oxidb.git
cd oxidb

# Build in release mode with optimizations
cargo build --release

# Run tests to verify installation
cargo test --release

# Install system-wide (optional)
cargo install --path .
```

### Using Pre-built Binaries
```bash
# Download the latest release
wget https://github.com/yourusername/oxidb/releases/latest/download/oxidb-linux-amd64.tar.gz

# Extract
tar -xzvf oxidb-linux-amd64.tar.gz

# Move to system path
sudo mv oxidb /usr/local/bin/
```

## Configuration

### Basic Configuration (Oxidb.toml)
```toml
# Database file location
database_file_path = "/var/lib/oxidb/main.db"

# Index storage location
index_base_path = "/var/lib/oxidb/indexes/"

# WAL configuration
wal_enabled = true
wal_directory = "/var/lib/oxidb/wal/"
wal_sync_mode = "fsync"  # Options: fsync, fdatasync, none

# Cache configuration
cache_size_mb = 1024  # 1GB buffer pool
cache_eviction_policy = "lru"  # Options: lru, lfu, clock

# Transaction settings
default_isolation_level = "ReadCommitted"  # Options: ReadUncommitted, ReadCommitted, RepeatableRead, Serializable
lock_timeout_ms = 5000
deadlock_detection_interval_ms = 1000

# Performance monitoring
monitoring_enabled = true
monitoring_sample_rate = 0.1  # Sample 10% of queries
monitoring_retention_days = 7

# Connection pool
max_connections = 100
connection_timeout_ms = 30000
```

### Advanced Configuration

#### Memory Management
```toml
[memory]
# Page size (must match compilation setting)
page_size = 4096

# Buffer pool configuration
buffer_pool_size_mb = 2048
buffer_pool_partitions = 8  # For reduced contention

# Query memory limits
max_query_memory_mb = 512
temp_buffer_size_mb = 256
```

#### Indexing Configuration
```toml
[indexing]
# B+ Tree settings
btree_node_size = 4096
btree_fill_factor = 0.7

# HNSW vector index settings
hnsw_m = 16
hnsw_ef_construction = 200
hnsw_max_m = 32
hnsw_seed = 42
```

## Performance Tuning

### Query Optimization

1. **Create Appropriate Indexes**
```sql
-- For frequent lookups
CREATE INDEX idx_users_email ON users(email);

-- For range queries
CREATE INDEX idx_orders_created ON orders(created_at);

-- For composite queries
CREATE INDEX idx_posts_user_date ON posts(user_id, created_at);

-- For vector similarity
CREATE INDEX idx_products_embedding ON products USING hnsw(embedding);
```

2. **Analyze Query Plans**
```sql
-- Check query execution plan
EXPLAIN SELECT * FROM users WHERE email = 'user@example.com';

-- Analyze with statistics
EXPLAIN ANALYZE SELECT * FROM orders WHERE created_at > '2024-01-01';
```

3. **Optimize Table Statistics**
```sql
-- Update table statistics
ANALYZE users;
ANALYZE orders;

-- Vacuum to reclaim space
VACUUM users;
VACUUM FULL orders;  -- More aggressive, locks table
```

### System-Level Optimizations

1. **File System Tuning**
```bash
# Disable access time updates
mount -o remount,noatime,nodiratime /var/lib/oxidb

# Increase file descriptor limits
ulimit -n 65536

# Set swappiness for database server
echo 10 > /proc/sys/vm/swappiness
```

2. **CPU Affinity**
```bash
# Pin OxiDB to specific CPU cores
taskset -c 0-7 oxidb-server
```

3. **Network Optimization**
```bash
# Increase network buffers
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
```

## Security

### Authentication and Authorization

1. **User Management**
```sql
-- Create user with password
CREATE USER 'app_user' WITH PASSWORD 'secure_password';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE ON database.* TO 'app_user';
GRANT ALL PRIVILEGES ON database.users TO 'admin_user';

-- Revoke permissions
REVOKE DELETE ON database.sensitive_table FROM 'app_user';
```

2. **Connection Security**
```toml
[security]
# Enable TLS for connections
tls_enabled = true
tls_cert_file = "/etc/oxidb/server.crt"
tls_key_file = "/etc/oxidb/server.key"
tls_ca_file = "/etc/oxidb/ca.crt"

# Authentication settings
auth_method = "scram-sha-256"  # Options: trust, password, scram-sha-256
password_encryption = "scram-sha-256"
```

### Data Encryption

1. **Encryption at Rest**
```toml
[encryption]
# Enable transparent data encryption
data_encryption_enabled = true
encryption_algorithm = "aes-256-gcm"
key_management_service = "local"  # Options: local, aws-kms, vault

# Key rotation
key_rotation_interval_days = 90
```

2. **Backup Encryption**
```bash
# Encrypt backups
oxidb-backup --encrypt --key-file /secure/backup.key
```

## Monitoring

### Performance Monitoring

1. **Enable Built-in Monitoring**
```rust
// In your application
let mut conn = Connection::open("database.db")?;
conn.enable_performance_monitoring();

// Get performance report
let report = conn.get_performance_report()?;
println!("{}", report);
```

2. **Metrics Export**
```toml
[monitoring.export]
# Prometheus export
prometheus_enabled = true
prometheus_port = 9090

# StatsD export
statsd_enabled = true
statsd_host = "localhost"
statsd_port = 8125
```

3. **Key Metrics to Monitor**
- Query execution time (p50, p95, p99)
- Transactions per second
- Lock wait time
- Buffer pool hit ratio
- Disk I/O operations
- Connection pool utilization
- WAL size and checkpoint frequency

### Logging

```toml
[logging]
# Log levels: trace, debug, info, warn, error
log_level = "info"
log_file = "/var/log/oxidb/oxidb.log"
log_rotation = "daily"
log_retention_days = 30

# Slow query log
slow_query_log_enabled = true
slow_query_threshold_ms = 1000
```

## Backup and Recovery

### Backup Strategies

1. **Online Backup**
```bash
# Full backup while database is running
oxidb-backup --type full --destination /backup/full_$(date +%Y%m%d).bak

# Incremental backup
oxidb-backup --type incremental --since-lsn 12345 --destination /backup/incr_$(date +%Y%m%d).bak
```

2. **Point-in-Time Recovery**
```bash
# Restore to specific timestamp
oxidb-restore --backup /backup/full_20240101.bak --target-time "2024-01-15 14:30:00"

# Restore to specific LSN
oxidb-restore --backup /backup/full_20240101.bak --target-lsn 98765
```

3. **Automated Backup Script**
```bash
#!/bin/bash
# backup.sh - Run daily via cron

BACKUP_DIR="/backup/oxidb"
DB_PATH="/var/lib/oxidb/main.db"
RETENTION_DAYS=30

# Perform backup
oxidb-backup --type full --source $DB_PATH --destination $BACKUP_DIR/full_$(date +%Y%m%d_%H%M%S).bak

# Cleanup old backups
find $BACKUP_DIR -name "*.bak" -mtime +$RETENTION_DAYS -delete

# Verify backup
oxidb-backup --verify --backup $BACKUP_DIR/full_$(date +%Y%m%d)*.bak
```

## High Availability

### Replication Setup

1. **Primary Configuration**
```toml
[replication]
role = "primary"
replication_enabled = true
wal_level = "replica"
max_wal_senders = 5
wal_keep_segments = 64
```

2. **Replica Configuration**
```toml
[replication]
role = "replica"
primary_host = "primary.example.com"
primary_port = 5432
replication_slot = "replica1"
hot_standby = true
```

### Load Balancing

```nginx
# nginx.conf for read replica load balancing
upstream oxidb_replicas {
    least_conn;
    server replica1.example.com:5432 weight=1;
    server replica2.example.com:5432 weight=1;
    server replica3.example.com:5432 weight=1;
}
```

## Troubleshooting

### Common Issues

1. **Database Corruption**
```bash
# Check database integrity
oxidb-check --database /var/lib/oxidb/main.db

# Repair if needed (backup first!)
oxidb-repair --database /var/lib/oxidb/main.db --force
```

2. **Performance Issues**
```sql
-- Check running queries
SELECT * FROM oxidb_stat_activity WHERE state = 'active';

-- Kill long-running query
KILL QUERY 12345;

-- Check lock contention
SELECT * FROM oxidb_locks WHERE granted = false;
```

3. **Recovery from Crash**
```bash
# OxiDB automatically performs recovery on startup
# To manually trigger recovery:
oxidb-recover --wal-dir /var/lib/oxidb/wal/ --database /var/lib/oxidb/main.db
```

### Debug Mode

```toml
[debug]
# Enable debug logging
debug_mode = true
trace_queries = true
log_lock_waits = true
log_checkpoints = true
```

## Best Practices

### Development
1. **Use Connection Pooling**: Reuse connections instead of creating new ones
2. **Prepare Statements**: Use parameterized queries to prevent SQL injection
3. **Batch Operations**: Group multiple operations in transactions
4. **Index Wisely**: Don't over-index; each index has maintenance overhead

### Operations
1. **Regular Maintenance**:
   - Run VACUUM weekly
   - Update statistics with ANALYZE
   - Monitor WAL size and checkpoint frequency
   
2. **Capacity Planning**:
   - Monitor growth trends
   - Plan for 20% headroom in storage
   - Scale before hitting resource limits

3. **Testing**:
   - Test backup restoration regularly
   - Perform chaos testing
   - Load test before major deployments

### Security
1. **Principle of Least Privilege**: Grant minimum required permissions
2. **Regular Updates**: Keep OxiDB and OS updated
3. **Audit Logging**: Enable and monitor audit logs
4. **Network Security**: Use TLS and firewall rules

## Appendix

### Performance Benchmarks

Typical performance on modern hardware (4 cores, 16GB RAM, SSD):
- Point queries: 50,000+ QPS
- Range scans: 10,000+ QPS  
- Inserts: 30,000+ TPS
- Updates: 25,000+ TPS
- Vector similarity (HNSW): 1,000+ QPS

### Resource Planning

| Workload Type | CPU Cores | RAM | Storage IOPS |
|--------------|-----------|-----|--------------|
| Light (< 100 QPS) | 2 | 4GB | 1000 |
| Medium (100-1000 QPS) | 4 | 16GB | 5000 |
| Heavy (1000-10000 QPS) | 8+ | 32GB+ | 10000+ |
| Analytics | 16+ | 64GB+ | 20000+ |

### Support and Resources

- Documentation: https://oxidb.io/docs
- Community Forum: https://forum.oxidb.io
- GitHub Issues: https://github.com/yourusername/oxidb/issues
- Commercial Support: support@oxidb.io