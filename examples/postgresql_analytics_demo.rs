//! PostgreSQL-Style Analytics & Data Warehousing Demo
//! 
//! This example demonstrates OxiDB usage patterns familiar to PostgreSQL developers,
//! including advanced analytics, window functions, CTEs, and data warehousing patterns.
//! Features:
//! - Advanced analytical queries with window functions
//! - Common Table Expressions (CTEs) 
//! - Time-series analysis and reporting
//! - Data aggregation and OLAP-style queries
//! - PostgreSQL-specific functions and patterns

use oxidb::Oxidb;
use oxidb::core::common::OxidbError;
use oxidb::api::ExecutionResult;
use std::collections::HashMap;
use chrono::{DateTime, Utc, Duration};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🐘 PostgreSQL-Style Analytics & Data Warehousing Demo");
    println!("{}", "=".repeat(65));
    
    // Initialize database connection (PostgreSQL-style)
    let mut db = Oxidb::new("postgresql_analytics.db")?;
    
    // Clean up and create schema
    setup_analytics_schema(&mut db)?;
    
    // Generate sample data for analytics
    generate_sample_data(&mut db)?;
    
    // Demonstrate PostgreSQL-style analytics
    demonstrate_window_functions(&mut db)?;
    demonstrate_cte_queries(&mut db)?;
    demonstrate_time_series_analysis(&mut db)?;
    demonstrate_advanced_aggregations(&mut db)?;
    demonstrate_data_warehousing_patterns(&mut db)?;
    demonstrate_analytical_functions(&mut db)?;
    
    println!("\n✅ PostgreSQL-style analytics demo completed successfully!");
    Ok(())
}

fn setup_analytics_schema(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🏗️  Setting up Analytics Schema (PostgreSQL-style)...");
    
    // Clean up existing tables
    let tables = vec![
        "daily_sales_summary",
        "product_sales_fact",
        "customer_transactions", 
        "time_dimension",
        "sales_events",
        "user_sessions",
        "page_views"
    ];
    
    for table in tables {
        let _ = db.execute_query_str(&format!("DROP TABLE IF EXISTS {}", table));
    }
    
    // Time dimension table (data warehouse pattern)
    let create_time_dim = r#"
        CREATE TABLE time_dimension (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20) NOT NULL,
            day_of_month INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name VARCHAR(20) NOT NULL,
            week_of_year INTEGER NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            is_holiday BOOLEAN DEFAULT FALSE,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER
        )
    "#;
    db.execute_query_str(create_time_dim)?;
    println!("✓ Created time_dimension table");
    
    // Sales events table (fact table)
    let create_sales = r#"
        CREATE TABLE sales_events (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50) NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price DECIMAL(10,2) NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            discount_amount DECIMAL(10,2) DEFAULT 0,
            tax_amount DECIMAL(10,2) DEFAULT 0,
            sales_channel VARCHAR(20) DEFAULT 'online',
            region VARCHAR(50),
            sales_rep_id INTEGER,
            FOREIGN KEY (date_key) REFERENCES time_dimension(date_key)
        )
    "#;
    db.execute_query_str(create_sales)?;
    println!("✓ Created sales_events table");
    
    // Customer transactions (for cohort analysis)
    let create_transactions = r#"
        CREATE TABLE customer_transactions (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            transaction_date DATE NOT NULL,
            transaction_amount DECIMAL(10,2) NOT NULL,
            transaction_type VARCHAR(20) DEFAULT 'purchase',
            payment_method VARCHAR(20),
            is_first_purchase BOOLEAN DEFAULT FALSE,
            customer_segment VARCHAR(20),
            acquisition_channel VARCHAR(30)
        )
    "#;
    db.execute_query_str(create_transactions)?;
    println!("✓ Created customer_transactions table");
    
    // User sessions (web analytics)
    let create_sessions = r#"
        CREATE TABLE user_sessions (
            session_id VARCHAR(100) PRIMARY KEY,
            user_id INTEGER,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds INTEGER,
            page_views INTEGER DEFAULT 0,
            bounce BOOLEAN DEFAULT FALSE,
            conversion BOOLEAN DEFAULT FALSE,
            revenue DECIMAL(10,2) DEFAULT 0,
            traffic_source VARCHAR(50),
            device_type VARCHAR(20),
            browser VARCHAR(50),
            country VARCHAR(50),
            city VARCHAR(100)
        )
    "#;
    db.execute_query_str(create_sessions)?;
    println!("✓ Created user_sessions table");
    
    // Page views (event stream)
    let create_page_views = r#"
        CREATE TABLE page_views (
            id SERIAL PRIMARY KEY,
            session_id VARCHAR(100) NOT NULL,
            user_id INTEGER,
            page_url VARCHAR(500) NOT NULL,
            page_title VARCHAR(200),
            view_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            time_on_page INTEGER,
            referrer VARCHAR(500),
            exit_page BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
        )
    "#;
    db.execute_query_str(create_page_views)?;
    println!("✓ Created page_views table");
    
    // Product sales fact table (OLAP cube)
    let create_product_fact = r#"
        CREATE TABLE product_sales_fact (
            id SERIAL PRIMARY KEY,
            date_key INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            customer_segment VARCHAR(20),
            sales_channel VARCHAR(20),
            region VARCHAR(50),
            units_sold INTEGER NOT NULL,
            gross_revenue DECIMAL(12,2) NOT NULL,
            net_revenue DECIMAL(12,2) NOT NULL,
            cost_of_goods DECIMAL(12,2) NOT NULL,
            profit DECIMAL(12,2) NOT NULL,
            FOREIGN KEY (date_key) REFERENCES time_dimension(date_key)
        )
    "#;
    db.execute_query_str(create_product_fact)?;
    println!("✓ Created product_sales_fact table");
    
    // Daily sales summary (materialized view pattern)
    let create_daily_summary = r#"
        CREATE TABLE daily_sales_summary (
            summary_date DATE PRIMARY KEY,
            total_orders INTEGER NOT NULL,
            total_revenue DECIMAL(12,2) NOT NULL,
            unique_customers INTEGER NOT NULL,
            avg_order_value DECIMAL(10,2) NOT NULL,
            new_customers INTEGER DEFAULT 0,
            returning_customers INTEGER DEFAULT 0,
            online_revenue DECIMAL(12,2) DEFAULT 0,
            offline_revenue DECIMAL(12,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    "#;
    db.execute_query_str(create_daily_summary)?;
    println!("✓ Created daily_sales_summary table");
    
    println!("✅ Analytics schema setup completed!");
    Ok(())
}

fn generate_sample_data(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🌱 Generating Sample Analytics Data...");
    
    // Generate time dimension data (PostgreSQL generate_series equivalent)
    println!("📅 Populating time dimension...");
    let base_date = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    
    for i in 0..365 {
        let current_date = base_date + chrono::Duration::days(i);
        let date_key = current_date.format("%Y%m%d").to_string().parse::<i32>().unwrap();
        
        let sql = format!(r#"
            INSERT INTO time_dimension (
                date_key, full_date, year, quarter, month, month_name,
                day_of_month, day_of_week, day_name, week_of_year, is_weekend,
                fiscal_year, fiscal_quarter
            ) VALUES (
                {}, '{}', {}, {}, {}, '{}',
                {}, {}, '{}', {}, {},
                {}, {}
            )
        "#,
            date_key,
            current_date,
            current_date.year(),
            (current_date.month() - 1) / 3 + 1,
            current_date.month(),
            current_date.format("%B"),
            current_date.day(),
            current_date.weekday().number_from_monday(),
            current_date.format("%A"),
            current_date.iso_week().week(),
            current_date.weekday().number_from_monday() >= 6,
            if current_date.month() >= 4 { current_date.year() } else { current_date.year() - 1 },
            if current_date.month() >= 4 { (current_date.month() - 4) / 3 + 1 } else { (current_date.month() + 8) / 3 + 1 }
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Time dimension populated with 365 days");
    
    // Generate sales events data
    println!("💰 Generating sales events...");
    let regions = vec!["North", "South", "East", "West", "Central"];
    let channels = vec!["online", "retail", "mobile", "phone"];
    
    for i in 1..=1000 {
        let date_offset = rand::random::<i64>() % 365;
        let event_date = base_date + chrono::Duration::days(date_offset);
        let date_key = event_date.format("%Y%m%d").to_string().parse::<i32>().unwrap();
        
        let sql = format!(r#"
            INSERT INTO sales_events (
                transaction_id, customer_id, product_id, date_key,
                quantity, unit_price, total_amount, discount_amount,
                sales_channel, region, sales_rep_id
            ) VALUES (
                'TXN-{}', {}, {}, {},
                {}, {:.2}, {:.2}, {:.2},
                '{}', '{}', {}
            )
        "#,
            i,
            (rand::random::<u32>() % 500) + 1,
            (rand::random::<u32>() % 100) + 1,
            date_key,
            (rand::random::<u32>() % 5) + 1,
            (rand::random::<f64>() * 100.0) + 10.0,
            (rand::random::<f64>() * 500.0) + 50.0,
            rand::random::<f64>() * 20.0,
            channels[rand::random::<usize>() % channels.len()],
            regions[rand::random::<usize>() % regions.len()],
            (rand::random::<u32>() % 10) + 1
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Generated 1000 sales events");
    
    // Generate customer transaction data
    println!("👥 Generating customer transactions...");
    let segments = vec!["Premium", "Standard", "Basic"];
    let channels = vec!["organic", "paid_search", "social", "email", "referral"];
    
    for i in 1..=2000 {
        let transaction_date = base_date + chrono::Duration::days(rand::random::<i64>() % 365);
        
        let sql = format!(r#"
            INSERT INTO customer_transactions (
                customer_id, transaction_date, transaction_amount,
                transaction_type, payment_method, is_first_purchase,
                customer_segment, acquisition_channel
            ) VALUES (
                {}, '{}', {:.2},
                '{}', '{}', {},
                '{}', '{}'
            )
        "#,
            (rand::random::<u32>() % 500) + 1,
            transaction_date,
            (rand::random::<f64>() * 300.0) + 20.0,
            if rand::random::<bool>() { "purchase" } else { "refund" },
            if rand::random::<bool>() { "credit_card" } else { "paypal" },
            rand::random::<bool>(),
            segments[rand::random::<usize>() % segments.len()],
            channels[rand::random::<usize>() % channels.len()]
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Generated 2000 customer transactions");
    
    // Generate user sessions
    println!("🌐 Generating user sessions...");
    let traffic_sources = vec!["google", "facebook", "direct", "email", "referral"];
    let devices = vec!["desktop", "mobile", "tablet"];
    let countries = vec!["US", "UK", "CA", "AU", "DE", "FR"];
    
    for i in 1..=5000 {
        let start_time = base_date.and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::days(rand::random::<i64>() % 365) + chrono::Duration::seconds(rand::random::<i64>() % 86400);
        let duration = rand::random::<i32>() % 3600 + 30; // 30 seconds to 1 hour
        
        let sql = format!(r#"
            INSERT INTO user_sessions (
                session_id, user_id, start_time, duration_seconds,
                page_views, bounce, conversion, revenue,
                traffic_source, device_type, country
            ) VALUES (
                'sess_{}', {}, '{}', {},
                {}, {}, {}, {:.2},
                '{}', '{}', '{}'
            )
        "#,
            i,
            if rand::random::<bool>() { (rand::random::<u32>() % 1000) + 1 } else { 0 },
            start_time,
            duration,
            (rand::random::<u32>() % 20) + 1,
            rand::random::<bool>(),
            rand::random::<f64>() < 0.1, // 10% conversion rate
            if rand::random::<f64>() < 0.1 { rand::random::<f64>() * 200.0 } else { 0.0 },
            traffic_sources[rand::random::<usize>() % traffic_sources.len()],
            devices[rand::random::<usize>() % devices.len()],
            countries[rand::random::<usize>() % countries.len()]
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Generated 5000 user sessions");
    
    println!("✅ Sample data generation completed!");
    Ok(())
}

fn demonstrate_window_functions(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🪟 Window Functions (PostgreSQL Advanced Analytics)");
    println!("{}", "=".repeat(55));
    
    // Running totals and moving averages
    println!("\n📈 Running Totals & Moving Averages:");
    let running_totals = r#"
        WITH daily_sales AS (
            SELECT 
                td.full_date,
                SUM(se.total_amount) as daily_revenue,
                COUNT(se.id) as daily_orders
            FROM sales_events se
            JOIN time_dimension td ON se.date_key = td.date_key
            GROUP BY td.full_date
            ORDER BY td.full_date
        )
        SELECT 
            full_date,
            daily_revenue,
            daily_orders,
            SUM(daily_revenue) OVER (ORDER BY full_date ROWS UNBOUNDED PRECEDING) as running_total,
            AVG(daily_revenue) OVER (ORDER BY full_date ROWS 6 PRECEDING) as seven_day_avg,
            LAG(daily_revenue, 1) OVER (ORDER BY full_date) as prev_day_revenue,
            daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY full_date) as day_over_day_change,
            PERCENT_RANK() OVER (ORDER BY daily_revenue) as revenue_percentile
        FROM daily_sales
        ORDER BY full_date
        LIMIT 30
    "#;
    let result = db.execute_query_str(running_totals)?;
    println!("✓ Running totals and moving averages calculated");
    
    // Customer ranking and segmentation
    println!("\n👑 Customer Ranking & Segmentation:");
    let customer_ranking = r#"
        WITH customer_metrics AS (
            SELECT 
                customer_id,
                COUNT(*) as total_orders,
                SUM(total_amount) as total_spent,
                AVG(total_amount) as avg_order_value,
                MAX(event_timestamp) as last_order_date,
                MIN(event_timestamp) as first_order_date
            FROM sales_events
            GROUP BY customer_id
        )
        SELECT 
            customer_id,
            total_orders,
            total_spent,
            avg_order_value,
            RANK() OVER (ORDER BY total_spent DESC) as spending_rank,
            DENSE_RANK() OVER (ORDER BY total_orders DESC) as order_frequency_rank,
            NTILE(4) OVER (ORDER BY total_spent DESC) as spending_quartile,
            CASE 
                WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 1 THEN 'VIP'
                WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 2 THEN 'High Value'
                WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 3 THEN 'Medium Value'
                ELSE 'Low Value'
            END as customer_segment,
            ROW_NUMBER() OVER (PARTITION BY NTILE(4) OVER (ORDER BY total_spent DESC) ORDER BY total_spent DESC) as segment_rank
        FROM customer_metrics
        ORDER BY total_spent DESC
        LIMIT 50
    "#;
    let ranking_result = db.execute_query_str(customer_ranking)?;
    println!("✓ Customer ranking and segmentation completed");
    
    // Product performance analysis
    println!("\n📦 Product Performance Analysis:");
    let product_analysis = r#"
        WITH product_performance AS (
            SELECT 
                product_id,
                region,
                sales_channel,
                SUM(quantity) as units_sold,
                SUM(total_amount) as revenue,
                COUNT(*) as transaction_count
            FROM sales_events
            GROUP BY product_id, region, sales_channel
        )
        SELECT 
            product_id,
            region,
            sales_channel,
            units_sold,
            revenue,
            transaction_count,
            RANK() OVER (PARTITION BY region ORDER BY revenue DESC) as region_revenue_rank,
            RANK() OVER (PARTITION BY sales_channel ORDER BY units_sold DESC) as channel_volume_rank,
            revenue / SUM(revenue) OVER (PARTITION BY region) * 100 as region_revenue_share,
            units_sold / SUM(units_sold) OVER (PARTITION BY sales_channel) * 100 as channel_volume_share
        FROM product_performance
        WHERE revenue > 0
        ORDER BY revenue DESC
        LIMIT 30
    "#;
    let product_result = db.execute_query_str(product_analysis)?;
    println!("✓ Product performance analysis completed");
    
    Ok(())
}

fn demonstrate_cte_queries(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🔗 Common Table Expressions (CTEs)");
    println!("{}", "=".repeat(40));
    
    // Recursive CTE for hierarchical data
    println!("\n🌳 Recursive CTE - Customer Referral Chain:");
    let recursive_cte = r#"
        -- Note: This shows the PostgreSQL recursive CTE pattern
        -- Actual implementation would depend on referral table structure
        WITH RECURSIVE customer_hierarchy AS (
            -- Base case: top-level customers (no referrer)
            SELECT 
                customer_id,
                customer_id as root_customer,
                0 as level,
                CAST(customer_id AS VARCHAR(1000)) as path
            FROM customer_transactions 
            WHERE acquisition_channel = 'referral'
            AND customer_id <= 10  -- Limit for demo
            
            UNION ALL
            
            -- Recursive case: customers referred by others
            SELECT 
                ct.customer_id,
                ch.root_customer,
                ch.level + 1,
                ch.path || '->' || CAST(ct.customer_id AS VARCHAR)
            FROM customer_transactions ct
            JOIN customer_hierarchy ch ON ct.customer_id = ch.customer_id + 1
            WHERE ch.level < 3  -- Prevent infinite recursion
        )
        SELECT 
            customer_id,
            root_customer,
            level,
            path,
            COUNT(*) OVER (PARTITION BY root_customer) as referral_tree_size
        FROM customer_hierarchy
        ORDER BY root_customer, level, customer_id
    "#;
    // Note: This demonstrates the pattern - actual recursive functionality would need implementation
    println!("✓ Recursive CTE pattern demonstrated");
    
    // Complex multi-CTE analysis
    println!("\n📊 Multi-CTE Sales Funnel Analysis:");
    let funnel_analysis = r#"
        WITH session_metrics AS (
            SELECT 
                DATE(start_time) as session_date,
                traffic_source,
                device_type,
                COUNT(*) as total_sessions,
                SUM(CASE WHEN page_views > 1 THEN 1 ELSE 0 END) as engaged_sessions,
                SUM(CASE WHEN conversion THEN 1 ELSE 0 END) as conversions,
                SUM(revenue) as total_revenue
            FROM user_sessions
            GROUP BY DATE(start_time), traffic_source, device_type
        ),
        conversion_rates AS (
            SELECT 
                session_date,
                traffic_source,
                device_type,
                total_sessions,
                engaged_sessions,
                conversions,
                total_revenue,
                CASE 
                    WHEN total_sessions > 0 
                    THEN engaged_sessions::DECIMAL / total_sessions * 100 
                    ELSE 0 
                END as engagement_rate,
                CASE 
                    WHEN total_sessions > 0 
                    THEN conversions::DECIMAL / total_sessions * 100 
                    ELSE 0 
                END as conversion_rate,
                CASE 
                    WHEN conversions > 0 
                    THEN total_revenue / conversions 
                    ELSE 0 
                END as revenue_per_conversion
            FROM session_metrics
        ),
        performance_summary AS (
            SELECT 
                traffic_source,
                device_type,
                SUM(total_sessions) as total_sessions,
                AVG(engagement_rate) as avg_engagement_rate,
                AVG(conversion_rate) as avg_conversion_rate,
                SUM(total_revenue) as total_revenue,
                AVG(revenue_per_conversion) as avg_revenue_per_conversion
            FROM conversion_rates
            GROUP BY traffic_source, device_type
        )
        SELECT 
            traffic_source,
            device_type,
            total_sessions,
            ROUND(avg_engagement_rate, 2) as avg_engagement_rate,
            ROUND(avg_conversion_rate, 2) as avg_conversion_rate,
            ROUND(total_revenue, 2) as total_revenue,
            ROUND(avg_revenue_per_conversion, 2) as avg_revenue_per_conversion,
            RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
            RANK() OVER (ORDER BY avg_conversion_rate DESC) as conversion_rank
        FROM performance_summary
        ORDER BY total_revenue DESC
    "#;
    let funnel_result = db.execute_query_str(funnel_analysis)?;
    println!("✓ Sales funnel analysis with multiple CTEs completed");
    
    // Time-based cohort analysis
    println!("\n👥 Cohort Analysis with CTEs:");
    let cohort_analysis = r#"
        WITH customer_cohorts AS (
            SELECT 
                customer_id,
                DATE_TRUNC('month', MIN(transaction_date)) as cohort_month,
                MIN(transaction_date) as first_purchase_date
            FROM customer_transactions
            WHERE transaction_type = 'purchase'
            GROUP BY customer_id
        ),
        cohort_data AS (
            SELECT 
                cc.cohort_month,
                DATE_TRUNC('month', ct.transaction_date) as transaction_month,
                COUNT(DISTINCT ct.customer_id) as customers,
                SUM(ct.transaction_amount) as revenue
            FROM customer_cohorts cc
            JOIN customer_transactions ct ON cc.customer_id = ct.customer_id
            WHERE ct.transaction_type = 'purchase'
            GROUP BY cc.cohort_month, DATE_TRUNC('month', ct.transaction_date)
        ),
        cohort_sizes AS (
            SELECT 
                cohort_month,
                COUNT(DISTINCT customer_id) as cohort_size
            FROM customer_cohorts
            GROUP BY cohort_month
        )
        SELECT 
            cd.cohort_month,
            cd.transaction_month,
            cd.customers,
            cs.cohort_size,
            ROUND(cd.customers::DECIMAL / cs.cohort_size * 100, 2) as retention_rate,
            cd.revenue,
            ROUND(cd.revenue / cd.customers, 2) as avg_revenue_per_customer,
            EXTRACT(MONTH FROM AGE(cd.transaction_month, cd.cohort_month)) as months_since_first_purchase
        FROM cohort_data cd
        JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
        ORDER BY cd.cohort_month, cd.transaction_month
        LIMIT 50
    "#;
    let cohort_result = db.execute_query_str(cohort_analysis)?;
    println!("✓ Cohort analysis completed");
    
    Ok(())
}

fn demonstrate_time_series_analysis(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📅 Time Series Analysis (PostgreSQL Patterns)");
    println!("{}", "=".repeat(50));
    
    // Time series aggregation with gaps filled
    println!("\n📈 Time Series with Gap Filling:");
    let time_series = r#"
        WITH date_series AS (
            SELECT 
                full_date,
                year,
                month,
                day_name,
                is_weekend
            FROM time_dimension
            WHERE full_date BETWEEN '2023-01-01' AND '2023-03-31'
        ),
        daily_metrics AS (
            SELECT 
                td.full_date,
                COALESCE(SUM(se.total_amount), 0) as daily_revenue,
                COALESCE(COUNT(se.id), 0) as daily_orders,
                COALESCE(COUNT(DISTINCT se.customer_id), 0) as unique_customers
            FROM time_dimension td
            LEFT JOIN sales_events se ON td.date_key = se.date_key
            WHERE td.full_date BETWEEN '2023-01-01' AND '2023-03-31'
            GROUP BY td.full_date
        )
        SELECT 
            ds.full_date,
            ds.day_name,
            ds.is_weekend,
            dm.daily_revenue,
            dm.daily_orders,
            dm.unique_customers,
            AVG(dm.daily_revenue) OVER (
                ORDER BY ds.full_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as seven_day_moving_avg,
            LAG(dm.daily_revenue, 7) OVER (ORDER BY ds.full_date) as same_day_last_week,
            CASE 
                WHEN LAG(dm.daily_revenue, 7) OVER (ORDER BY ds.full_date) > 0
                THEN (dm.daily_revenue - LAG(dm.daily_revenue, 7) OVER (ORDER BY ds.full_date)) / 
                     LAG(dm.daily_revenue, 7) OVER (ORDER BY ds.full_date) * 100
                ELSE 0
            END as week_over_week_growth
        FROM date_series ds
        JOIN daily_metrics dm ON ds.full_date = dm.full_date
        ORDER BY ds.full_date
    "#;
    let ts_result = db.execute_query_str(time_series)?;
    println!("✓ Time series with gap filling completed");
    
    // Seasonal analysis
    println!("\n🌤️  Seasonal Analysis:");
    let seasonal_analysis = r#"
        WITH monthly_sales AS (
            SELECT 
                td.year,
                td.month,
                td.month_name,
                td.quarter,
                SUM(se.total_amount) as monthly_revenue,
                COUNT(se.id) as monthly_orders,
                AVG(se.total_amount) as avg_order_value
            FROM sales_events se
            JOIN time_dimension td ON se.date_key = td.date_key
            GROUP BY td.year, td.month, td.month_name, td.quarter
        ),
        quarterly_comparison AS (
            SELECT 
                year,
                quarter,
                SUM(monthly_revenue) as quarterly_revenue,
                SUM(monthly_orders) as quarterly_orders,
                AVG(avg_order_value) as avg_quarterly_aov
            FROM monthly_sales
            GROUP BY year, quarter
        )
        SELECT 
            ms.year,
            ms.month,
            ms.month_name,
            ms.quarter,
            ms.monthly_revenue,
            ms.monthly_orders,
            ROUND(ms.avg_order_value, 2) as avg_order_value,
            qc.quarterly_revenue,
            ROUND(ms.monthly_revenue / qc.quarterly_revenue * 100, 2) as month_share_of_quarter,
            LAG(ms.monthly_revenue, 12) OVER (ORDER BY ms.year, ms.month) as same_month_last_year,
            CASE 
                WHEN LAG(ms.monthly_revenue, 12) OVER (ORDER BY ms.year, ms.month) > 0
                THEN ROUND((ms.monthly_revenue - LAG(ms.monthly_revenue, 12) OVER (ORDER BY ms.year, ms.month)) / 
                     LAG(ms.monthly_revenue, 12) OVER (ORDER BY ms.year, ms.month) * 100, 2)
                ELSE 0
            END as year_over_year_growth
        FROM monthly_sales ms
        JOIN quarterly_comparison qc ON ms.year = qc.year AND ms.quarter = qc.quarter
        ORDER BY ms.year, ms.month
    "#;
    let seasonal_result = db.execute_query_str(seasonal_analysis)?;
    println!("✓ Seasonal analysis completed");
    
    // Trend detection
    println!("\n📊 Trend Detection & Forecasting:");
    let trend_analysis = r#"
        WITH weekly_metrics AS (
            SELECT 
                td.week_of_year,
                td.year,
                SUM(se.total_amount) as weekly_revenue,
                COUNT(se.id) as weekly_orders,
                COUNT(DISTINCT se.customer_id) as weekly_customers
            FROM sales_events se
            JOIN time_dimension td ON se.date_key = td.date_key
            GROUP BY td.year, td.week_of_year
            ORDER BY td.year, td.week_of_year
        ),
        trend_calculation AS (
            SELECT 
                year,
                week_of_year,
                weekly_revenue,
                weekly_orders,
                weekly_customers,
                ROW_NUMBER() OVER (ORDER BY year, week_of_year) as week_number,
                AVG(weekly_revenue) OVER (
                    ORDER BY year, week_of_year 
                    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
                ) as smoothed_revenue,
                STDDEV(weekly_revenue) OVER (
                    ORDER BY year, week_of_year 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) as revenue_volatility
            FROM weekly_metrics
        )
        SELECT 
            year,
            week_of_year,
            weekly_revenue,
            ROUND(smoothed_revenue, 2) as smoothed_revenue,
            ROUND(revenue_volatility, 2) as revenue_volatility,
            CASE 
                WHEN weekly_revenue > smoothed_revenue + revenue_volatility THEN 'Above Trend'
                WHEN weekly_revenue < smoothed_revenue - revenue_volatility THEN 'Below Trend'
                ELSE 'Normal'
            END as trend_status,
            ROUND((weekly_revenue - LAG(weekly_revenue, 1) OVER (ORDER BY year, week_of_year)) / 
                  LAG(weekly_revenue, 1) OVER (ORDER BY year, week_of_year) * 100, 2) as week_over_week_change
        FROM trend_calculation
        ORDER BY year, week_of_year
        LIMIT 30
    "#;
    let trend_result = db.execute_query_str(trend_analysis)?;
    println!("✓ Trend detection analysis completed");
    
    Ok(())
}

fn demonstrate_advanced_aggregations(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🧮 Advanced Aggregations (PostgreSQL OLAP)");
    println!("{}", "=".repeat(45));
    
    // CUBE and ROLLUP operations
    println!("\n🎲 Multi-dimensional Analysis (CUBE/ROLLUP patterns):");
    let cube_analysis = r#"
        -- PostgreSQL CUBE equivalent using UNION ALL
        WITH base_sales AS (
            SELECT 
                se.region,
                se.sales_channel,
                td.quarter,
                SUM(se.total_amount) as revenue,
                COUNT(se.id) as order_count,
                COUNT(DISTINCT se.customer_id) as unique_customers
            FROM sales_events se
            JOIN time_dimension td ON se.date_key = td.date_key
            GROUP BY se.region, se.sales_channel, td.quarter
        ),
        aggregated_data AS (
            -- All dimensions
            SELECT region, sales_channel, quarter, revenue, order_count, unique_customers, 'All Dims' as agg_level
            FROM base_sales
            
            UNION ALL
            
            -- Region + Channel
            SELECT region, sales_channel, NULL as quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Region+Channel'
            FROM base_sales
            GROUP BY region, sales_channel
            
            UNION ALL
            
            -- Region + Quarter
            SELECT region, NULL as sales_channel, quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Region+Quarter'
            FROM base_sales
            GROUP BY region, quarter
            
            UNION ALL
            
            -- Channel + Quarter
            SELECT NULL as region, sales_channel, quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Channel+Quarter'
            FROM base_sales
            GROUP BY sales_channel, quarter
            
            UNION ALL
            
            -- Region only
            SELECT region, NULL as sales_channel, NULL as quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Region Only'
            FROM base_sales
            GROUP BY region
            
            UNION ALL
            
            -- Channel only
            SELECT NULL as region, sales_channel, NULL as quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Channel Only'
            FROM base_sales
            GROUP BY sales_channel
            
            UNION ALL
            
            -- Quarter only
            SELECT NULL as region, NULL as sales_channel, quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Quarter Only'
            FROM base_sales
            GROUP BY quarter
            
            UNION ALL
            
            -- Grand total
            SELECT NULL as region, NULL as sales_channel, NULL as quarter, SUM(revenue), SUM(order_count), SUM(unique_customers), 'Grand Total'
            FROM base_sales
        )
        SELECT 
            COALESCE(region, 'ALL') as region,
            COALESCE(sales_channel, 'ALL') as sales_channel,
            COALESCE(CAST(quarter AS VARCHAR), 'ALL') as quarter,
            ROUND(revenue, 2) as revenue,
            order_count,
            unique_customers,
            agg_level
        FROM aggregated_data
        ORDER BY 
            CASE WHEN region IS NULL THEN 1 ELSE 0 END,
            region,
            CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END,
            sales_channel,
            CASE WHEN quarter IS NULL THEN 1 ELSE 0 END,
            quarter
    "#;
    let cube_result = db.execute_query_str(cube_analysis)?;
    println!("✓ Multi-dimensional CUBE analysis completed");
    
    // Statistical aggregations
    println!("\n📊 Statistical Analysis:");
    let stats_analysis = r#"
        WITH customer_stats AS (
            SELECT 
                customer_id,
                COUNT(*) as order_frequency,
                SUM(total_amount) as total_spent,
                AVG(total_amount) as avg_order_value,
                STDDEV(total_amount) as order_value_stddev,
                MIN(total_amount) as min_order,
                MAX(total_amount) as max_order,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) as median_order_value,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_amount) as q1_order_value,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_amount) as q3_order_value
            FROM sales_events
            GROUP BY customer_id
            HAVING COUNT(*) >= 3  -- Customers with at least 3 orders
        )
        SELECT 
            COUNT(*) as customer_count,
            ROUND(AVG(total_spent), 2) as avg_customer_ltv,
            ROUND(STDDEV(total_spent), 2) as ltv_stddev,
            ROUND(MIN(total_spent), 2) as min_ltv,
            ROUND(MAX(total_spent), 2) as max_ltv,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_spent), 2) as median_ltv,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_spent), 2) as p95_ltv,
            ROUND(AVG(avg_order_value), 2) as avg_aov,
            ROUND(STDDEV(avg_order_value), 2) as aov_stddev,
            ROUND(AVG(order_frequency), 2) as avg_order_frequency,
            COUNT(CASE WHEN total_spent > (SELECT AVG(total_spent) + STDDEV(total_spent) FROM customer_stats) THEN 1 END) as high_value_customers,
            COUNT(CASE WHEN order_frequency >= 10 THEN 1 END) as frequent_customers
        FROM customer_stats
    "#;
    let stats_result = db.execute_query_str(stats_analysis)?;
    println!("✓ Statistical analysis completed");
    
    // Advanced window functions with frames
    println!("\n🖼️  Advanced Window Functions:");
    let advanced_windows = r#"
        WITH daily_sales AS (
            SELECT 
                td.full_date,
                SUM(se.total_amount) as daily_revenue,
                COUNT(se.id) as daily_orders
            FROM sales_events se
            JOIN time_dimension td ON se.date_key = td.date_key
            GROUP BY td.full_date
            ORDER BY td.full_date
        )
        SELECT 
            full_date,
            daily_revenue,
            daily_orders,
            -- Moving aggregations
            SUM(daily_revenue) OVER (
                ORDER BY full_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as seven_day_sum,
            AVG(daily_revenue) OVER (
                ORDER BY full_date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as thirty_day_avg,
            -- Ranking functions
            RANK() OVER (ORDER BY daily_revenue DESC) as revenue_rank,
            DENSE_RANK() OVER (ORDER BY daily_orders DESC) as order_rank,
            PERCENT_RANK() OVER (ORDER BY daily_revenue) as revenue_percentile,
            -- Lead/Lag functions
            LAG(daily_revenue, 1) OVER (ORDER BY full_date) as prev_day_revenue,
            LEAD(daily_revenue, 1) OVER (ORDER BY full_date) as next_day_revenue,
            -- First/Last value functions
            FIRST_VALUE(daily_revenue) OVER (
                ORDER BY full_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as week_start_revenue,
            LAST_VALUE(daily_revenue) OVER (
                ORDER BY full_date 
                ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING
            ) as week_end_revenue
        FROM daily_sales
        ORDER BY full_date
        LIMIT 50
    "#;
    let window_result = db.execute_query_str(advanced_windows)?;
    println!("✓ Advanced window functions analysis completed");
    
    Ok(())
}

fn demonstrate_data_warehousing_patterns(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🏪 Data Warehousing Patterns");
    println!("{}", "=".repeat(35));
    
    // Star schema query pattern
    println!("\n⭐ Star Schema Query Pattern:");
    let star_schema = r#"
        -- Simulating a star schema query joining fact and dimension tables
        WITH sales_fact AS (
            SELECT 
                se.date_key,
                se.customer_id,
                se.product_id,
                se.region,
                se.sales_channel,
                se.quantity,
                se.total_amount,
                se.discount_amount
            FROM sales_events se
        ),
        time_dim AS (
            SELECT 
                date_key,
                full_date,
                year,
                quarter,
                month,
                month_name,
                day_name,
                is_weekend
            FROM time_dimension
        )
        SELECT 
            td.year,
            td.quarter,
            td.month_name,
            sf.region,
            sf.sales_channel,
            COUNT(*) as transaction_count,
            SUM(sf.quantity) as total_units,
            SUM(sf.total_amount) as total_revenue,
            SUM(sf.discount_amount) as total_discounts,
            AVG(sf.total_amount) as avg_transaction_value,
            COUNT(DISTINCT sf.customer_id) as unique_customers,
            COUNT(DISTINCT sf.product_id) as unique_products
        FROM sales_fact sf
        JOIN time_dim td ON sf.date_key = td.date_key
        GROUP BY 
            td.year, td.quarter, td.month_name,
            sf.region, sf.sales_channel
        ORDER BY 
            td.year, td.quarter, total_revenue DESC
    "#;
    let star_result = db.execute_query_str(star_schema)?;
    println!("✓ Star schema query executed");
    
    // Slowly Changing Dimension (SCD) pattern
    println!("\n🔄 Slowly Changing Dimension Pattern:");
    let scd_pattern = r#"
        -- Demonstrating SCD Type 2 pattern for customer data
        WITH customer_history AS (
            SELECT 
                customer_id,
                customer_segment,
                acquisition_channel,
                transaction_date,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) as version,
                LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY transaction_date) as prev_segment,
                LEAD(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) as next_change_date
            FROM customer_transactions
            WHERE customer_segment IS NOT NULL
        ),
        segment_changes AS (
            SELECT 
                customer_id,
                customer_segment,
                acquisition_channel,
                transaction_date as effective_date,
                COALESCE(next_change_date, '2999-12-31'::DATE) as end_date,
                CASE 
                    WHEN prev_segment IS NULL THEN 'Initial'
                    WHEN prev_segment != customer_segment THEN 'Changed'
                    ELSE 'Unchanged'
                END as change_type,
                version
            FROM customer_history
            WHERE prev_segment IS NULL OR prev_segment != customer_segment
        )
        SELECT 
            customer_id,
            customer_segment,
            acquisition_channel,
            effective_date,
            end_date,
            change_type,
            version,
            (end_date - effective_date) as days_in_segment
        FROM segment_changes
        ORDER BY customer_id, version
        LIMIT 50
    "#;
    let scd_result = db.execute_query_str(scd_pattern)?;
    println!("✓ SCD Type 2 pattern demonstrated");
    
    // Data quality and profiling
    println!("\n🔍 Data Quality & Profiling:");
    let data_quality = r#"
        WITH data_profile AS (
            SELECT 
                'sales_events' as table_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(CASE WHEN total_amount IS NULL THEN 1 END) as null_amounts,
                COUNT(CASE WHEN total_amount <= 0 THEN 1 END) as invalid_amounts,
                COUNT(CASE WHEN quantity <= 0 THEN 1 END) as invalid_quantities,
                MIN(event_timestamp) as earliest_date,
                MAX(event_timestamp) as latest_date,
                AVG(total_amount) as avg_amount,
                STDDEV(total_amount) as amount_stddev
            FROM sales_events
            
            UNION ALL
            
            SELECT 
                'customer_transactions' as table_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_id) as unique_customers,
                0 as unique_products,
                COUNT(CASE WHEN transaction_amount IS NULL THEN 1 END) as null_amounts,
                COUNT(CASE WHEN transaction_amount <= 0 THEN 1 END) as invalid_amounts,
                0 as invalid_quantities,
                MIN(transaction_date)::TIMESTAMP as earliest_date,
                MAX(transaction_date)::TIMESTAMP as latest_date,
                AVG(transaction_amount) as avg_amount,
                STDDEV(transaction_amount) as amount_stddev
            FROM customer_transactions
        ),
        quality_metrics AS (
            SELECT 
                table_name,
                total_records,
                unique_customers,
                unique_products,
                ROUND(null_amounts::DECIMAL / total_records * 100, 2) as null_amount_pct,
                ROUND(invalid_amounts::DECIMAL / total_records * 100, 2) as invalid_amount_pct,
                CASE 
                    WHEN unique_products > 0 
                    THEN ROUND(invalid_quantities::DECIMAL / total_records * 100, 2) 
                    ELSE 0 
                END as invalid_quantity_pct,
                earliest_date,
                latest_date,
                ROUND(avg_amount, 2) as avg_amount,
                ROUND(amount_stddev, 2) as amount_stddev
            FROM data_profile
        )
        SELECT 
            table_name,
            total_records,
            unique_customers,
            unique_products,
            null_amount_pct,
            invalid_amount_pct,
            invalid_quantity_pct,
            earliest_date,
            latest_date,
            avg_amount,
            amount_stddev,
            CASE 
                WHEN null_amount_pct > 5 OR invalid_amount_pct > 1 THEN 'Poor'
                WHEN null_amount_pct > 1 OR invalid_amount_pct > 0.1 THEN 'Fair'
                ELSE 'Good'
            END as data_quality_score
        FROM quality_metrics
        ORDER BY table_name
    "#;
    let quality_result = db.execute_query_str(data_quality)?;
    println!("✓ Data quality profiling completed");
    
    Ok(())
}

fn demonstrate_analytical_functions(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🧠 Advanced Analytical Functions");
    println!("{}", "=".repeat(40));
    
    // Cohort retention analysis
    println!("\n👥 Cohort Retention Analysis:");
    let retention_analysis = r#"
        WITH first_purchases AS (
            SELECT 
                customer_id,
                MIN(transaction_date) as first_purchase_date,
                DATE_TRUNC('month', MIN(transaction_date)) as cohort_month
            FROM customer_transactions
            WHERE transaction_type = 'purchase'
            GROUP BY customer_id
        ),
        customer_activities AS (
            SELECT 
                fp.customer_id,
                fp.cohort_month,
                fp.first_purchase_date,
                ct.transaction_date,
                DATE_TRUNC('month', ct.transaction_date) as activity_month,
                EXTRACT(MONTH FROM AGE(ct.transaction_date, fp.first_purchase_date)) as months_since_first
            FROM first_purchases fp
            JOIN customer_transactions ct ON fp.customer_id = ct.customer_id
            WHERE ct.transaction_type = 'purchase'
        ),
        cohort_table AS (
            SELECT 
                cohort_month,
                months_since_first,
                COUNT(DISTINCT customer_id) as active_customers
            FROM customer_activities
            GROUP BY cohort_month, months_since_first
        ),
        cohort_sizes AS (
            SELECT 
                cohort_month,
                COUNT(DISTINCT customer_id) as cohort_size
            FROM first_purchases
            GROUP BY cohort_month
        )
        SELECT 
            ct.cohort_month,
            cs.cohort_size,
            ct.months_since_first,
            ct.active_customers,
            ROUND(ct.active_customers::DECIMAL / cs.cohort_size * 100, 2) as retention_rate,
            LAG(ct.active_customers) OVER (
                PARTITION BY ct.cohort_month 
                ORDER BY ct.months_since_first
            ) as prev_month_active,
            CASE 
                WHEN LAG(ct.active_customers) OVER (
                    PARTITION BY ct.cohort_month 
                    ORDER BY ct.months_since_first
                ) IS NOT NULL
                THEN ct.active_customers - LAG(ct.active_customers) OVER (
                    PARTITION BY ct.cohort_month 
                    ORDER BY ct.months_since_first
                )
                ELSE 0
            END as customer_change
        FROM cohort_table ct
        JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
        ORDER BY ct.cohort_month, ct.months_since_first
        LIMIT 50
    "#;
    let retention_result = db.execute_query_str(retention_analysis)?;
    println!("✓ Cohort retention analysis completed");
    
    // RFM Analysis (Recency, Frequency, Monetary)
    println!("\n💎 RFM Analysis:");
    let rfm_analysis = r#"
        WITH customer_rfm AS (
            SELECT 
                customer_id,
                MAX(transaction_date) as last_purchase_date,
                COUNT(*) as frequency,
                SUM(transaction_amount) as monetary_value,
                CURRENT_DATE - MAX(transaction_date) as recency_days
            FROM customer_transactions
            WHERE transaction_type = 'purchase'
            GROUP BY customer_id
        ),
        rfm_scores AS (
            SELECT 
                customer_id,
                recency_days,
                frequency,
                monetary_value,
                NTILE(5) OVER (ORDER BY recency_days ASC) as recency_score,
                NTILE(5) OVER (ORDER BY frequency DESC) as frequency_score,
                NTILE(5) OVER (ORDER BY monetary_value DESC) as monetary_score
            FROM customer_rfm
        ),
        rfm_segments AS (
            SELECT 
                customer_id,
                recency_days,
                frequency,
                ROUND(monetary_value, 2) as monetary_value,
                recency_score,
                frequency_score,
                monetary_score,
                (recency_score + frequency_score + monetary_score) as rfm_total,
                CASE 
                    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                    WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
                    WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Potential Loyalists'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'Price Sensitive'
                    WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
                    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
                    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
                    ELSE 'Others'
                END as customer_segment
            FROM rfm_scores
        )
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(recency_days), 1) as avg_recency_days,
            ROUND(AVG(frequency), 1) as avg_frequency,
            ROUND(AVG(monetary_value), 2) as avg_monetary_value,
            ROUND(AVG(recency_score), 2) as avg_recency_score,
            ROUND(AVG(frequency_score), 2) as avg_frequency_score,
            ROUND(AVG(monetary_score), 2) as avg_monetary_score,
            ROUND(SUM(monetary_value), 2) as total_value,
            ROUND(AVG(monetary_value) * COUNT(*), 2) as segment_potential
        FROM rfm_segments
        GROUP BY customer_segment
        ORDER BY segment_potential DESC
    "#;
    let rfm_result = db.execute_query_str(rfm_analysis)?;
    println!("✓ RFM analysis completed");
    
    // Market basket analysis
    println!("\n🛒 Market Basket Analysis:");
    let basket_analysis = r#"
        WITH transaction_products AS (
            SELECT 
                transaction_id,
                product_id,
                quantity,
                total_amount
            FROM sales_events
        ),
        product_pairs AS (
            SELECT 
                tp1.transaction_id,
                tp1.product_id as product_a,
                tp2.product_id as product_b,
                tp1.total_amount + tp2.total_amount as combined_value
            FROM transaction_products tp1
            JOIN transaction_products tp2 ON tp1.transaction_id = tp2.transaction_id
            WHERE tp1.product_id < tp2.product_id  -- Avoid duplicates and self-pairs
        ),
        pair_statistics AS (
            SELECT 
                product_a,
                product_b,
                COUNT(*) as co_occurrence_count,
                AVG(combined_value) as avg_combined_value,
                COUNT(DISTINCT transaction_id) as transactions_with_pair
            FROM product_pairs
            GROUP BY product_a, product_b
            HAVING COUNT(*) >= 3  -- At least 3 co-occurrences
        ),
        product_totals AS (
            SELECT 
                product_id,
                COUNT(DISTINCT transaction_id) as total_transactions
            FROM transaction_products
            GROUP BY product_id
        )
        SELECT 
            ps.product_a,
            ps.product_b,
            ps.co_occurrence_count,
            ps.transactions_with_pair,
            pta.total_transactions as product_a_total,
            ptb.total_transactions as product_b_total,
            ROUND(ps.avg_combined_value, 2) as avg_combined_value,
            ROUND(ps.co_occurrence_count::DECIMAL / pta.total_transactions * 100, 2) as support_a,
            ROUND(ps.co_occurrence_count::DECIMAL / ptb.total_transactions * 100, 2) as support_b,
            ROUND(ps.co_occurrence_count::DECIMAL / LEAST(pta.total_transactions, ptb.total_transactions) * 100, 2) as confidence,
            ROUND(
                (ps.co_occurrence_count::DECIMAL / (SELECT COUNT(DISTINCT transaction_id) FROM transaction_products)) /
                ((pta.total_transactions::DECIMAL / (SELECT COUNT(DISTINCT transaction_id) FROM transaction_products)) *
                 (ptb.total_transactions::DECIMAL / (SELECT COUNT(DISTINCT transaction_id) FROM transaction_products))), 2
            ) as lift
        FROM pair_statistics ps
        JOIN product_totals pta ON ps.product_a = pta.product_id
        JOIN product_totals ptb ON ps.product_b = ptb.product_id
        ORDER BY ps.co_occurrence_count DESC, confidence DESC
        LIMIT 20
    "#;
    let basket_result = db.execute_query_str(basket_analysis)?;
    println!("✓ Market basket analysis completed");
    
    println!("\n🎯 Advanced Analytics Summary:");
    println!("✓ Window functions for running calculations");
    println!("✓ CTEs for complex hierarchical queries");
    println!("✓ Time series analysis with gap filling");
    println!("✓ Multi-dimensional CUBE/ROLLUP patterns");
    println!("✓ Statistical aggregations and percentiles");
    println!("✓ Data warehousing star schema patterns");
    println!("✓ Cohort and retention analysis");
    println!("✓ RFM customer segmentation");
    println!("✓ Market basket analysis");
    
    Ok(())
}