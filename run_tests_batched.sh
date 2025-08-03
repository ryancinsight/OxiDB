#!/bin/bash

# Run tests in smaller batches to reduce resource contention
# This helps avoid hanging when running all 790+ tests at once

set -e

echo "Running OxiDB tests in batches..."

# Test modules to run
TEST_MODULES=(
    "storage::engine"
    "indexing::btree"
    "indexing::blink_tree"
    "indexing::hash"
    "indexing::hnsw"
    "indexing::rtree"
    "query::sql"
    "query::executor"
    "transaction"
    "vector"
    "rag"
    "optimizer"
    "performance"
    "api"
)

FAILED_MODULES=()
TOTAL_TESTS=0
PASSED_TESTS=0

for module in "${TEST_MODULES[@]}"; do
    echo "========================================="
    echo "Testing module: $module"
    echo "========================================="
    
    # Run with limited threads to reduce contention
    if cargo test --lib --release "$module" -- --test-threads=2 --nocapture 2>&1 | tee test_output.tmp; then
        # Extract test count
        TESTS_RUN=$(grep -oE '[0-9]+ passed' test_output.tmp | grep -oE '[0-9]+' || echo "0")
        TOTAL_TESTS=$((TOTAL_TESTS + TESTS_RUN))
        PASSED_TESTS=$((PASSED_TESTS + TESTS_RUN))
        echo "✓ Module $module passed ($TESTS_RUN tests)"
    else
        FAILED_MODULES+=("$module")
        echo "✗ Module $module failed"
    fi
    
    # Small delay between modules to let resources clean up
    sleep 0.5
done

rm -f test_output.tmp

echo "========================================="
echo "Test Summary:"
echo "========================================="
echo "Total tests run: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"

if [ ${#FAILED_MODULES[@]} -eq 0 ]; then
    echo "All test modules passed! ✓"
    exit 0
else
    echo "Failed modules:"
    for module in "${FAILED_MODULES[@]}"; do
        echo "  - $module"
    done
    exit 1
fi