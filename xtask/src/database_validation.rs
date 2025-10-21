//! Database engineering validation for OxiDB
//!
//! This module provides database-specific validation rules and pattern checks
//! following ACID principles, transaction safety, and database engineering best practices.

use anyhow::Result;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use walkdir::WalkDir;

/// Database engineering violations found during analysis
#[derive(Debug)]
pub struct DatabaseViolations {
    pub acid_violations: Vec<String>,
    pub transaction_safety: Vec<String>,
    pub index_violations: Vec<String>,
    pub sql_injection_risks: Vec<String>,
    pub performance_issues: Vec<String>,
    pub memory_safety: Vec<String>,
}

impl DatabaseViolations {
    pub fn new() -> Self {
        Self {
            acid_violations: Vec::new(),
            transaction_safety: Vec::new(),
            index_violations: Vec::new(),
            sql_injection_risks: Vec::new(),
            performance_issues: Vec::new(),
            memory_safety: Vec::new(),
        }
    }

    pub fn total_violations(&self) -> usize {
        self.acid_violations.len()
            + self.transaction_safety.len()
            + self.index_violations.len()
            + self.sql_injection_risks.len()
            + self.performance_issues.len()
            + self.memory_safety.len()
    }
}

/// Check for database engineering violations
pub fn check_database_patterns() -> Result<DatabaseViolations> {
    let mut violations = DatabaseViolations::new();

    println!("🔍 Checking database engineering patterns...");

    for entry in WalkDir::new("src").into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let content = fs::read_to_string(path)?;

            check_acid_compliance(&content, path.display().to_string(), &mut violations)?;
            check_transaction_safety(&content, path.display().to_string(), &mut violations)?;
            check_sql_injection_prevention(&content, path.display().to_string(), &mut violations)?;
            check_index_patterns(&content, path.display().to_string(), &mut violations)?;
            check_performance_patterns(&content, path.display().to_string(), &mut violations)?;
            check_memory_safety(&content, path.display().to_string(), &mut violations)?;
        }
    }

    Ok(violations)
}

/// Check ACID compliance patterns
fn check_acid_compliance(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for transaction boundary violations
    let transaction_regex =
        Regex::new(r"(?i)(begin|start)\s+(?:transaction)?.*?(?:commit|rollback)")?;
    let begin_count =
        content.matches("begin").count() + content.matches("start_transaction").count();
    let commit_count = content.matches("commit").count() + content.matches("rollback").count();

    if begin_count > commit_count + 2 {
        // Allow some tolerance
        violations.acid_violations.push(format!(
            "{}: Potential transaction leak - {} begin statements but only {} commit/rollback",
            file_path, begin_count, commit_count
        ));
    }

    // Check for proper error handling in transaction code
    if content.contains("transaction") && !content.contains("Result<") && !content.contains("?") {
        violations.acid_violations.push(format!(
            "{}: Transaction code should use Result types for proper error handling",
            file_path
        ));
    }

    // Check for atomic operations
    if content.contains("batch_insert") || content.contains("bulk_operation") {
        if !content.contains("transaction") && !content.contains("atomic") {
            violations.acid_violations.push(format!(
                "{}: Bulk operations should be wrapped in transactions for atomicity",
                file_path
            ));
        }
    }

    Ok(())
}

/// Check transaction safety patterns
fn check_transaction_safety(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for deadlock prevention patterns
    if content.contains("lock") && content.contains("transaction") {
        if !content.contains("timeout") && !content.contains("deadlock") {
            violations.transaction_safety.push(format!(
                "{}: Locking code should include deadlock prevention or timeout handling",
                file_path
            ));
        }
    }

    // Check for proper lock ordering
    let lock_pattern = Regex::new(r"\.lock\(\)|\.try_lock\(\)")?;
    if lock_pattern.is_match(content) {
        // This is a simplified check - in real implementation, we'd analyze lock acquisition order
        if content.matches(".lock()").count() > 1 && !content.contains("// Lock order:") {
            violations.transaction_safety.push(format!(
                "{}: Multiple locks acquired - consider documenting lock ordering to prevent deadlocks",
                file_path
            ));
        }
    }

    // Check for isolation level considerations
    if content.contains("read_committed") || content.contains("serializable") {
        if !content.contains("isolation") && !content.contains("mvcc") {
            violations.transaction_safety.push(format!(
                "{}: Isolation level code should explicitly handle MVCC or isolation concerns",
                file_path
            ));
        }
    }

    Ok(())
}

/// Check for SQL injection prevention
fn check_sql_injection_prevention(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for string concatenation in SQL
    let sql_concat_regex = Regex::new(r#"(SELECT|INSERT|UPDATE|DELETE).*\+.*["']"#)?;
    if sql_concat_regex.is_match(content) {
        violations.sql_injection_risks.push(format!(
            "{}: Potential SQL injection risk - string concatenation detected in SQL statement",
            file_path
        ));
    }

    // Check for format! macro with user input
    let format_sql_regex = Regex::new(r#"format!\s*\(\s*"[^"]*(?:SELECT|INSERT|UPDATE|DELETE)"#)?;
    if format_sql_regex.is_match(content) {
        violations.sql_injection_risks.push(format!(
            "{}: Potential SQL injection risk - format! macro with SQL statement",
            file_path
        ));
    }

    // Check for prepared statement usage
    if content.contains("execute") && content.contains("sql") && !content.contains("prepare") {
        if content.contains("user_input") || content.contains("request") {
            violations.sql_injection_risks.push(format!(
                "{}: Consider using prepared statements for user input in SQL execution",
                file_path
            ));
        }
    }

    Ok(())
}

/// Check indexing patterns
fn check_index_patterns(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for sequential scan warnings
    if content.contains("scan") && content.contains("table") && !content.contains("index") {
        violations.index_violations.push(format!(
            "{}: Sequential table scan detected - consider adding appropriate indexes",
            file_path
        ));
    }

    // Check for proper index usage in joins
    if content.contains("join") && !content.contains("index") && !content.contains("hash") {
        violations.index_violations.push(format!(
            "{}: Join operation without index usage - may cause performance issues",
            file_path
        ));
    }

    // Check for covering index opportunities
    if content.contains("SELECT") && content.contains("WHERE") && content.contains("ORDER BY") {
        violations.index_violations.push(format!(
            "{}: Complex query detected - consider covering indexes for better performance",
            file_path
        ));
    }

    Ok(())
}

/// Check performance patterns
fn check_performance_patterns(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for N+1 query problems
    if content.contains("for") && content.contains("query") && content.contains("select") {
        violations.performance_issues.push(format!(
            "{}: Potential N+1 query problem - consider batch operations or joins",
            file_path
        ));
    }

    // Check for missing LIMIT clauses
    let select_regex = Regex::new(r"(?i)SELECT.*FROM.*WHERE")?;
    if select_regex.is_match(content) && !content.contains("LIMIT") && !content.contains("limit") {
        violations.performance_issues.push(format!(
            "{}: SELECT query without LIMIT - could return unbounded results",
            file_path
        ));
    }

    // Check for inefficient iteration patterns
    if content.contains("for row in") && content.contains("collect()") {
        violations.performance_issues.push(format!(
            "{}: Consider using iterators with lazy evaluation instead of collecting all rows",
            file_path
        ));
    }

    // Check for buffer pool size considerations
    if content.contains("buffer_pool") && !content.contains("size") && !content.contains("capacity")
    {
        violations.performance_issues.push(format!(
            "{}: Buffer pool code should consider size limits and capacity management",
            file_path
        ));
    }

    Ok(())
}

/// Check memory safety patterns specific to databases
fn check_memory_safety(
    content: &str,
    file_path: String,
    violations: &mut DatabaseViolations,
) -> Result<()> {
    // Check for excessive cloning in hot paths
    let clone_count = content.matches(".clone()").count();
    let lines = content.lines().count();
    if clone_count > 0 && lines > 0 && (clone_count as f64 / lines as f64) > 0.05 {
        violations.memory_safety.push(format!(
            "{}: High clone density ({} clones in {} lines) - consider using references or Cow",
            file_path, clone_count, lines
        ));
    }

    // Check for potential memory leaks in long-running operations
    if content.contains("Vec::new()") && content.contains("loop") && !content.contains("clear()") {
        violations.memory_safety.push(format!(
            "{}: Vector in loop without clear() - potential memory accumulation",
            file_path
        ));
    }

    // Check for string interning opportunities
    if content.contains("String::from") && content.contains("sql") {
        let string_count = content.matches("String::from").count();
        if string_count > 5 {
            violations.memory_safety.push(format!(
                "{}: {} String::from calls - consider string interning for SQL keywords",
                file_path, string_count
            ));
        }
    }

    // Check for proper resource cleanup
    if content.contains("connection") || content.contains("file") {
        if !content.contains("drop") && !content.contains("close") && !content.contains("RAII") {
            violations.memory_safety.push(format!(
                "{}: Resource management code should ensure proper cleanup (RAII pattern)",
                file_path
            ));
        }
    }

    Ok(())
}

/// Generate database engineering report
pub fn generate_database_report(violations: &DatabaseViolations) -> String {
    let mut report = String::new();

    report.push_str("# Database Engineering Analysis Report\n\n");

    if violations.total_violations() == 0 {
        report
            .push_str("✅ **All database engineering patterns are following best practices!**\n\n");
        return report;
    }

    report
        .push_str(&format!("📊 **Total Violations Found:** {}\n\n", violations.total_violations()));

    if !violations.acid_violations.is_empty() {
        report.push_str("## 🔒 ACID Compliance Violations\n\n");
        for violation in &violations.acid_violations {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    if !violations.transaction_safety.is_empty() {
        report.push_str("## ⚡ Transaction Safety Issues\n\n");
        for violation in &violations.transaction_safety {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    if !violations.sql_injection_risks.is_empty() {
        report.push_str("## 🛡️ SQL Injection Risks\n\n");
        for violation in &violations.sql_injection_risks {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    if !violations.index_violations.is_empty() {
        report.push_str("## 📇 Index Usage Issues\n\n");
        for violation in &violations.index_violations {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    if !violations.performance_issues.is_empty() {
        report.push_str("## ⚡ Performance Issues\n\n");
        for violation in &violations.performance_issues {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    if !violations.memory_safety.is_empty() {
        report.push_str("## 🧠 Memory Safety Issues\n\n");
        for violation in &violations.memory_safety {
            report.push_str(&format!("- {}\n", violation));
        }
        report.push('\n');
    }

    report.push_str("## 💡 Recommendations\n\n");
    report.push_str("1. **ACID Compliance**: Ensure all database operations are properly wrapped in transactions\n");
    report.push_str(
        "2. **Transaction Safety**: Implement deadlock prevention and proper lock ordering\n",
    );
    report.push_str(
        "3. **SQL Injection Prevention**: Use prepared statements and parameter binding\n",
    );
    report.push_str("4. **Index Optimization**: Add appropriate indexes for query patterns\n");
    report.push_str("5. **Performance**: Implement batching, lazy evaluation, and proper limits\n");
    report.push_str("6. **Memory Safety**: Use RAII patterns, avoid excessive cloning, consider string interning\n\n");

    report
}
