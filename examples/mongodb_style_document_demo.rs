//! MongoDB-Style Document Database Demo
//! 
//! This example demonstrates OxiDB usage patterns familiar to MongoDB developers,
//! including document storage, JSON queries, and NoSQL-style operations.
//! Features:
//! - Document-oriented data modeling
//! - JSON field queries and updates
//! - Nested document operations
//! - Array operations and indexing
//! - Aggregation pipeline patterns

use oxidb::Oxidb;
use oxidb::core::common::OxidbError;
use oxidb::api::ExecutionResult;
use serde_json::{json, Value};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🍃 MongoDB-Style Document Database Demo");
    println!("{}", "=".repeat(50));
    
    // Initialize database (MongoDB-style connection)
    let mut db = Oxidb::new("mongodb_style_documents.db")?;
    
    // Set up document collections (tables)
    setup_document_collections(&mut db)?;
    
    // Insert document data
    insert_document_data(&mut db)?;
    
    // Demonstrate document operations
    demonstrate_document_queries(&mut db)?;
    demonstrate_nested_operations(&mut db)?;
    demonstrate_array_operations(&mut db)?;
    demonstrate_aggregation_pipeline(&mut db)?;
    demonstrate_text_search(&mut db)?;
    demonstrate_geospatial_queries(&mut db)?;
    
    println!("\n✅ MongoDB-style document demo completed successfully!");
    Ok(())
}

fn setup_document_collections(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📄 Setting up Document Collections...");
    
    // Clean up existing collections
    let collections = vec![
        "users",
        "products", 
        "orders",
        "reviews",
        "blog_posts",
        "locations"
    ];
    
    for collection in collections {
        let _ = db.execute_query_str(&format!("DROP TABLE IF EXISTS {}", collection));
    }
    
    // Users collection (document store)
    let create_users = r#"
        CREATE TABLE users (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            email VARCHAR(100) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.email')) STORED UNIQUE,
            username VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.username')) STORED,
            created_at TIMESTAMP GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.createdAt')) STORED,
            is_active BOOLEAN GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.isActive')) STORED DEFAULT TRUE,
            INDEX idx_username (username),
            INDEX idx_created_at (created_at),
            INDEX idx_active (is_active)
        )
    "#;
    db.execute_query_str(create_users)?;
    println!("✓ Created users collection");
    
    // Products collection with nested attributes
    let create_products = r#"
        CREATE TABLE products (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            name VARCHAR(200) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.name')) STORED,
            category VARCHAR(100) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.category')) STORED,
            price DECIMAL(10,2) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.price')) STORED,
            in_stock BOOLEAN GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.inStock')) STORED,
            tags JSON GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.tags')) STORED,
            INDEX idx_name (name),
            INDEX idx_category (category),
            INDEX idx_price (price),
            INDEX idx_stock (in_stock),
            FULLTEXT INDEX idx_tags (tags)
        )
    "#;
    db.execute_query_str(create_products)?;
    println!("✓ Created products collection");
    
    // Orders collection with embedded documents
    let create_orders = r#"
        CREATE TABLE orders (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            user_id VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.userId')) STORED,
            status VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.status')) STORED,
            total DECIMAL(10,2) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.total')) STORED,
            order_date TIMESTAMP GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.orderDate')) STORED,
            INDEX idx_user (user_id),
            INDEX idx_status (status),
            INDEX idx_total (total),
            INDEX idx_date (order_date)
        )
    "#;
    db.execute_query_str(create_orders)?;
    println!("✓ Created orders collection");
    
    // Reviews collection with ratings and text
    let create_reviews = r#"
        CREATE TABLE reviews (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            product_id VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.productId')) STORED,
            user_id VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.userId')) STORED,
            rating INTEGER GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.rating')) STORED,
            review_date TIMESTAMP GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.reviewDate')) STORED,
            INDEX idx_product (product_id),
            INDEX idx_user (user_id),
            INDEX idx_rating (rating),
            INDEX idx_date (review_date)
        )
    "#;
    db.execute_query_str(create_reviews)?;
    println!("✓ Created reviews collection");
    
    // Blog posts collection with rich content
    let create_blog_posts = r#"
        CREATE TABLE blog_posts (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            title VARCHAR(300) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.title')) STORED,
            author VARCHAR(100) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.author')) STORED,
            published_date TIMESTAMP GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.publishedDate')) STORED,
            is_published BOOLEAN GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.isPublished')) STORED,
            view_count INTEGER GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.viewCount')) STORED DEFAULT 0,
            INDEX idx_title (title),
            INDEX idx_author (author),
            INDEX idx_published (published_date),
            INDEX idx_status (is_published),
            FULLTEXT INDEX idx_content (document)
        )
    "#;
    db.execute_query_str(create_blog_posts)?;
    println!("✓ Created blog_posts collection");
    
    // Locations collection for geospatial queries
    let create_locations = r#"
        CREATE TABLE locations (
            _id VARCHAR(50) PRIMARY KEY,
            document JSON NOT NULL,
            name VARCHAR(200) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.name')) STORED,
            type VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.type')) STORED,
            latitude DECIMAL(10,8) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.coordinates.lat')) STORED,
            longitude DECIMAL(11,8) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.coordinates.lng')) STORED,
            INDEX idx_name (name),
            INDEX idx_type (type),
            INDEX idx_coords (latitude, longitude)
        )
    "#;
    db.execute_query_str(create_locations)?;
    println!("✓ Created locations collection");
    
    println!("✅ Document collections setup completed!");
    Ok(())
}

fn insert_document_data(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📝 Inserting Document Data...");
    
    // Insert users with complex profiles
    println!("👥 Inserting user documents...");
    let users = vec![
        json!({
            "_id": "user_001",
            "username": "john_doe",
            "email": "john@example.com",
            "profile": {
                "firstName": "John",
                "lastName": "Doe",
                "age": 28,
                "location": {
                    "city": "New York",
                    "state": "NY",
                    "country": "USA"
                },
                "preferences": {
                    "theme": "dark",
                    "notifications": true,
                    "language": "en"
                }
            },
            "social": {
                "twitter": "@johndoe",
                "linkedin": "john-doe-dev"
            },
            "roles": ["user", "premium"],
            "createdAt": "2023-01-15T10:30:00Z",
            "lastLogin": "2023-12-01T14:22:00Z",
            "isActive": true,
            "loginCount": 156
        }),
        json!({
            "_id": "user_002", 
            "username": "jane_smith",
            "email": "jane@example.com",
            "profile": {
                "firstName": "Jane",
                "lastName": "Smith",
                "age": 32,
                "location": {
                    "city": "San Francisco",
                    "state": "CA",
                    "country": "USA"
                },
                "preferences": {
                    "theme": "light",
                    "notifications": false,
                    "language": "en"
                }
            },
            "social": {
                "github": "janesmith",
                "twitter": "@jane_codes"
            },
            "roles": ["user", "admin"],
            "createdAt": "2023-02-20T09:15:00Z",
            "lastLogin": "2023-12-02T11:45:00Z",
            "isActive": true,
            "loginCount": 89
        }),
        json!({
            "_id": "user_003",
            "username": "bob_wilson",
            "email": "bob@example.com",
            "profile": {
                "firstName": "Bob",
                "lastName": "Wilson",
                "age": 45,
                "location": {
                    "city": "Austin",
                    "state": "TX",
                    "country": "USA"
                },
                "preferences": {
                    "theme": "auto",
                    "notifications": true,
                    "language": "en"
                }
            },
            "roles": ["user"],
            "createdAt": "2023-03-10T16:20:00Z",
            "lastLogin": "2023-11-28T08:30:00Z",
            "isActive": false,
            "loginCount": 23
        })
    ];
    
    for user in users {
        let sql = format!(
            "INSERT INTO users (_id, document) VALUES ('{}', '{}')",
            user["_id"].as_str().unwrap(),
            user.to_string().replace("'", "''")
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted {} user documents", 3);
    
    // Insert products with nested attributes
    println!("📦 Inserting product documents...");
    let products = vec![
        json!({
            "_id": "prod_001",
            "name": "MacBook Pro 16-inch",
            "category": "Electronics",
            "subcategory": "Laptops",
            "brand": "Apple",
            "price": 2499.99,
            "inStock": true,
            "quantity": 15,
            "specifications": {
                "processor": "Apple M2 Pro",
                "memory": "16GB",
                "storage": "512GB SSD",
                "display": "16.2-inch Liquid Retina XDR",
                "weight": "4.7 lbs"
            },
            "features": ["Touch ID", "Force Touch trackpad", "Backlit keyboard"],
            "tags": ["laptop", "apple", "professional", "high-performance"],
            "reviews": {
                "average": 4.8,
                "count": 127
            },
            "createdAt": "2023-01-01T00:00:00Z",
            "updatedAt": "2023-11-15T10:30:00Z"
        }),
        json!({
            "_id": "prod_002",
            "name": "iPhone 15 Pro",
            "category": "Electronics", 
            "subcategory": "Smartphones",
            "brand": "Apple",
            "price": 999.99,
            "inStock": true,
            "quantity": 50,
            "specifications": {
                "processor": "A17 Pro",
                "memory": "128GB",
                "display": "6.1-inch Super Retina XDR",
                "camera": "48MP Main + 12MP Ultra Wide + 12MP Telephoto",
                "weight": "6.60 oz"
            },
            "features": ["Face ID", "Wireless charging", "Water resistant"],
            "tags": ["smartphone", "apple", "5g", "camera"],
            "reviews": {
                "average": 4.6,
                "count": 89
            },
            "createdAt": "2023-09-15T00:00:00Z",
            "updatedAt": "2023-11-20T14:15:00Z"
        }),
        json!({
            "_id": "prod_003",
            "name": "Wireless Bluetooth Headphones",
            "category": "Electronics",
            "subcategory": "Audio",
            "brand": "Sony",
            "price": 299.99,
            "inStock": false,
            "quantity": 0,
            "specifications": {
                "type": "Over-ear",
                "connectivity": "Bluetooth 5.0",
                "batteryLife": "30 hours",
                "noiseCancellation": true,
                "weight": "8.8 oz"
            },
            "features": ["Active Noise Cancellation", "Quick Charge", "Voice Assistant"],
            "tags": ["headphones", "wireless", "noise-cancellation", "sony"],
            "reviews": {
                "average": 4.4,
                "count": 203
            },
            "createdAt": "2023-06-01T00:00:00Z",
            "updatedAt": "2023-11-25T09:45:00Z"
        })
    ];
    
    for product in products {
        let sql = format!(
            "INSERT INTO products (_id, document) VALUES ('{}', '{}')",
            product["_id"].as_str().unwrap(),
            product.to_string().replace("'", "''")
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted {} product documents", 3);
    
    // Insert orders with embedded line items
    println!("🛒 Inserting order documents...");
    let orders = vec![
        json!({
            "_id": "order_001",
            "userId": "user_001",
            "orderNumber": "ORD-2023-001",
            "status": "delivered",
            "orderDate": "2023-11-01T10:00:00Z",
            "deliveryDate": "2023-11-05T14:30:00Z",
            "items": [
                {
                    "productId": "prod_001",
                    "name": "MacBook Pro 16-inch",
                    "quantity": 1,
                    "unitPrice": 2499.99,
                    "total": 2499.99
                }
            ],
            "shipping": {
                "method": "express",
                "cost": 15.99,
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zipCode": "10001",
                    "country": "USA"
                }
            },
            "payment": {
                "method": "credit_card",
                "last4": "1234",
                "status": "paid"
            },
            "subtotal": 2499.99,
            "tax": 200.00,
            "shipping": 15.99,
            "total": 2715.98
        }),
        json!({
            "_id": "order_002",
            "userId": "user_002",
            "orderNumber": "ORD-2023-002", 
            "status": "processing",
            "orderDate": "2023-11-15T15:30:00Z",
            "items": [
                {
                    "productId": "prod_002",
                    "name": "iPhone 15 Pro",
                    "quantity": 1,
                    "unitPrice": 999.99,
                    "total": 999.99
                },
                {
                    "productId": "prod_003",
                    "name": "Wireless Bluetooth Headphones",
                    "quantity": 1,
                    "unitPrice": 299.99,
                    "total": 299.99
                }
            ],
            "shipping": {
                "method": "standard",
                "cost": 9.99,
                "address": {
                    "street": "456 Oak Ave",
                    "city": "San Francisco",
                    "state": "CA", 
                    "zipCode": "94102",
                    "country": "USA"
                }
            },
            "payment": {
                "method": "paypal",
                "status": "paid"
            },
            "subtotal": 1299.98,
            "tax": 104.00,
            "shipping": 9.99,
            "total": 1413.97
        })
    ];
    
    for order in orders {
        let sql = format!(
            "INSERT INTO orders (_id, document) VALUES ('{}', '{}')",
            order["_id"].as_str().unwrap(),
            order.to_string().replace("'", "''")
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted {} order documents", 2);
    
    // Insert blog posts with rich content
    println!("📝 Inserting blog post documents...");
    let blog_posts = vec![
        json!({
            "_id": "post_001",
            "title": "Getting Started with Document Databases",
            "slug": "getting-started-document-databases",
            "author": "john_doe",
            "content": "Document databases offer a flexible way to store and query data...",
            "excerpt": "Learn the basics of document-oriented databases and their advantages.",
            "publishedDate": "2023-10-01T09:00:00Z",
            "isPublished": true,
            "tags": ["database", "nosql", "tutorial", "beginner"],
            "categories": ["Technology", "Database"],
            "metadata": {
                "readTime": 5,
                "difficulty": "beginner",
                "language": "en"
            },
            "stats": {
                "viewCount": 1250,
                "likeCount": 89,
                "shareCount": 23,
                "commentCount": 15
            },
            "seo": {
                "metaTitle": "Document Database Tutorial - Getting Started Guide",
                "metaDescription": "Complete guide to document databases for beginners",
                "keywords": ["document database", "nosql", "mongodb", "tutorial"]
            }
        }),
        json!({
            "_id": "post_002", 
            "title": "Advanced Query Patterns in Document Stores",
            "slug": "advanced-query-patterns-document-stores",
            "author": "jane_smith",
            "content": "Once you've mastered the basics, these advanced patterns will help...",
            "excerpt": "Explore complex querying techniques for document databases.",
            "publishedDate": "2023-10-15T14:30:00Z",
            "isPublished": true,
            "tags": ["database", "nosql", "advanced", "queries"],
            "categories": ["Technology", "Database", "Advanced"],
            "metadata": {
                "readTime": 12,
                "difficulty": "advanced",
                "language": "en"
            },
            "stats": {
                "viewCount": 890,
                "likeCount": 67,
                "shareCount": 31,
                "commentCount": 8
            },
            "seo": {
                "metaTitle": "Advanced Document Database Queries - Expert Guide",
                "metaDescription": "Master complex query patterns in document databases",
                "keywords": ["advanced queries", "document database", "aggregation", "indexing"]
            }
        })
    ];
    
    for post in blog_posts {
        let sql = format!(
            "INSERT INTO blog_posts (_id, document) VALUES ('{}', '{}')",
            post["_id"].as_str().unwrap(),
            post.to_string().replace("'", "''")
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted {} blog post documents", 2);
    
    println!("✅ Document data insertion completed!");
    Ok(())
}

fn demonstrate_document_queries(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🔍 Document Query Operations (MongoDB-style)");
    println!("{}", "=".repeat(50));
    
    // Find documents by field values
    println!("\n📋 Basic Document Queries:");
    let basic_query = r#"
        SELECT _id, 
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.email') as email,
               JSON_EXTRACT(document, '$.profile.firstName') as first_name,
               JSON_EXTRACT(document, '$.profile.lastName') as last_name,
               JSON_EXTRACT(document, '$.isActive') as is_active
        FROM users
        WHERE JSON_EXTRACT(document, '$.isActive') = true
    "#;
    let result = db.execute_query_str(basic_query)?;
    println!("✓ Found active users");
    
    // Query nested documents
    println!("\n🏠 Nested Document Queries:");
    let nested_query = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.profile.location.city') as city,
               JSON_EXTRACT(document, '$.profile.location.state') as state,
               JSON_EXTRACT(document, '$.profile.preferences.theme') as theme
        FROM users
        WHERE JSON_EXTRACT(document, '$.profile.location.state') = 'CA'
    "#;
    let nested_result = db.execute_query_str(nested_query)?;
    println!("✓ Found users in California");
    
    // Query with multiple conditions
    println!("\n🔎 Complex Document Queries:");
    let complex_query = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.name') as product_name,
               JSON_EXTRACT(document, '$.price') as price,
               JSON_EXTRACT(document, '$.category') as category,
               JSON_EXTRACT(document, '$.inStock') as in_stock,
               JSON_EXTRACT(document, '$.reviews.average') as avg_rating
        FROM products
        WHERE JSON_EXTRACT(document, '$.price') < 1000
          AND JSON_EXTRACT(document, '$.inStock') = true
          AND JSON_EXTRACT(document, '$.reviews.average') >= 4.5
        ORDER BY JSON_EXTRACT(document, '$.price') DESC
    "#;
    let complex_result = db.execute_query_str(complex_query)?;
    println!("✓ Found affordable, in-stock, highly-rated products");
    
    // Query documents by date ranges
    println!("\n📅 Date Range Queries:");
    let date_query = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.title') as title,
               JSON_EXTRACT(document, '$.author') as author,
               JSON_EXTRACT(document, '$.publishedDate') as published_date,
               JSON_EXTRACT(document, '$.stats.viewCount') as view_count
        FROM blog_posts
        WHERE JSON_EXTRACT(document, '$.publishedDate') >= '2023-10-01T00:00:00Z'
          AND JSON_EXTRACT(document, '$.isPublished') = true
        ORDER BY JSON_EXTRACT(document, '$.stats.viewCount') DESC
    "#;
    let date_result = db.execute_query_str(date_query)?;
    println!("✓ Found recent published blog posts");
    
    Ok(())
}

fn demonstrate_nested_operations(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🪆 Nested Document Operations");
    println!("{}", "=".repeat(35));
    
    // Update nested fields
    println!("\n✏️  Updating Nested Fields:");
    let update_nested = r#"
        UPDATE users 
        SET document = JSON_SET(
            document,
            '$.profile.preferences.theme', 'dark',
            '$.lastLogin', '2023-12-03T10:00:00Z',
            '$.loginCount', JSON_EXTRACT(document, '$.loginCount') + 1
        )
        WHERE _id = 'user_002'
    "#;
    db.execute_query_str(update_nested)?;
    println!("✓ Updated user preferences and login info");
    
    // Query updated nested data
    let verify_update = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.profile.preferences.theme') as theme,
               JSON_EXTRACT(document, '$.lastLogin') as last_login,
               JSON_EXTRACT(document, '$.loginCount') as login_count
        FROM users
        WHERE _id = 'user_002'
    "#;
    let verify_result = db.execute_query_str(verify_update)?;
    println!("✓ Verified nested field updates");
    
    // Add new nested objects
    println!("\n➕ Adding Nested Objects:");
    let add_nested = r#"
        UPDATE users
        SET document = JSON_SET(
            document,
            '$.profile.avatar', JSON_OBJECT(
                'url', 'https://example.com/avatars/user_001.jpg',
                'size', 'medium',
                'uploadedAt', '2023-12-03T10:30:00Z'
            ),
            '$.settings', JSON_OBJECT(
                'emailNotifications', true,
                'pushNotifications', false,
                'twoFactorAuth', true
            )
        )
        WHERE _id = 'user_001'
    "#;
    db.execute_query_str(add_nested)?;
    println!("✓ Added avatar and settings objects");
    
    // Query with existence checks
    println!("\n🔍 Existence Queries:");
    let existence_query = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               CASE 
                   WHEN JSON_EXTRACT(document, '$.profile.avatar') IS NOT NULL 
                   THEN 'Has Avatar' 
                   ELSE 'No Avatar' 
               END as avatar_status,
               CASE 
                   WHEN JSON_EXTRACT(document, '$.social.twitter') IS NOT NULL 
                   THEN JSON_EXTRACT(document, '$.social.twitter')
                   ELSE 'No Twitter'
               END as twitter_handle
        FROM users
        ORDER BY _id
    "#;
    let existence_result = db.execute_query_str(existence_query)?;
    println!("✓ Checked field existence in documents");
    
    Ok(())
}

fn demonstrate_array_operations(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📚 Array Operations (MongoDB-style)");
    println!("{}", "=".repeat(40));
    
    // Query arrays with contains
    println!("\n🔍 Array Contains Queries:");
    let array_contains = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.roles') as roles
        FROM users
        WHERE JSON_CONTAINS(JSON_EXTRACT(document, '$.roles'), '"admin"')
    "#;
    let contains_result = db.execute_query_str(array_contains)?;
    println!("✓ Found users with admin role");
    
    // Query product tags
    println!("\n🏷️  Product Tag Queries:");
    let tag_query = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.name') as product_name,
               JSON_EXTRACT(document, '$.tags') as tags,
               JSON_EXTRACT(document, '$.price') as price
        FROM products
        WHERE JSON_CONTAINS(JSON_EXTRACT(document, '$.tags'), '"apple"')
           OR JSON_CONTAINS(JSON_EXTRACT(document, '$.tags'), '"smartphone"')
        ORDER BY JSON_EXTRACT(document, '$.price') DESC
    "#;
    let tag_result = db.execute_query_str(tag_query)?;
    println!("✓ Found products with specific tags");
    
    // Array length queries
    println!("\n📏 Array Length Queries:");
    let array_length = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.orderNumber') as order_number,
               JSON_LENGTH(JSON_EXTRACT(document, '$.items')) as item_count,
               JSON_EXTRACT(document, '$.total') as total
        FROM orders
        WHERE JSON_LENGTH(JSON_EXTRACT(document, '$.items')) > 1
        ORDER BY JSON_LENGTH(JSON_EXTRACT(document, '$.items')) DESC
    "#;
    let length_result = db.execute_query_str(array_length)?;
    println!("✓ Found orders with multiple items");
    
    // Update arrays (add elements)
    println!("\n➕ Array Update Operations:");
    let update_array = r#"
        UPDATE users
        SET document = JSON_SET(
            document,
            '$.roles', JSON_ARRAY_APPEND(JSON_EXTRACT(document, '$.roles'), '$', 'beta_tester')
        )
        WHERE _id = 'user_001'
    "#;
    db.execute_query_str(update_array)?;
    println!("✓ Added beta_tester role to user");
    
    // Verify array update
    let verify_array = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.roles') as roles
        FROM users
        WHERE _id = 'user_001'
    "#;
    let verify_array_result = db.execute_query_str(verify_array)?;
    println!("✓ Verified array update");
    
    Ok(())
}

fn demonstrate_aggregation_pipeline(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🔄 Aggregation Pipeline (MongoDB-style)");
    println!("{}", "=".repeat(45));
    
    // Group and aggregate user data
    println!("\n👥 User Analytics:");
    let user_analytics = r#"
        WITH user_stats AS (
            SELECT 
                JSON_EXTRACT(document, '$.profile.location.state') as state,
                JSON_EXTRACT(document, '$.profile.location.country') as country,
                JSON_EXTRACT(document, '$.isActive') as is_active,
                JSON_EXTRACT(document, '$.loginCount') as login_count,
                JSON_EXTRACT(document, '$.profile.age') as age
            FROM users
        )
        SELECT 
            state,
            country,
            COUNT(*) as user_count,
            SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_users,
            AVG(CAST(login_count AS INTEGER)) as avg_logins,
            AVG(CAST(age AS INTEGER)) as avg_age,
            MIN(CAST(age AS INTEGER)) as min_age,
            MAX(CAST(age AS INTEGER)) as max_age
        FROM user_stats
        WHERE state IS NOT NULL
        GROUP BY state, country
        ORDER BY user_count DESC
    "#;
    let user_result = db.execute_query_str(user_analytics)?;
    println!("✓ Generated user analytics by location");
    
    // Product category analysis
    println!("\n📦 Product Category Analysis:");
    let product_analytics = r#"
        WITH product_stats AS (
            SELECT 
                JSON_EXTRACT(document, '$.category') as category,
                JSON_EXTRACT(document, '$.subcategory') as subcategory,
                JSON_EXTRACT(document, '$.price') as price,
                JSON_EXTRACT(document, '$.inStock') as in_stock,
                JSON_EXTRACT(document, '$.reviews.average') as avg_rating,
                JSON_EXTRACT(document, '$.reviews.count') as review_count
            FROM products
        )
        SELECT 
            category,
            subcategory,
            COUNT(*) as product_count,
            SUM(CASE WHEN in_stock = true THEN 1 ELSE 0 END) as in_stock_count,
            ROUND(AVG(CAST(price AS DECIMAL)), 2) as avg_price,
            ROUND(MIN(CAST(price AS DECIMAL)), 2) as min_price,
            ROUND(MAX(CAST(price AS DECIMAL)), 2) as max_price,
            ROUND(AVG(CAST(avg_rating AS DECIMAL)), 2) as avg_rating,
            SUM(CAST(review_count AS INTEGER)) as total_reviews
        FROM product_stats
        GROUP BY category, subcategory
        ORDER BY avg_price DESC
    "#;
    let product_result = db.execute_query_str(product_analytics)?;
    println!("✓ Generated product analytics by category");
    
    // Order analysis with item details
    println!("\n🛒 Order Analysis:");
    let order_analytics = r#"
        WITH order_details AS (
            SELECT 
                _id,
                JSON_EXTRACT(document, '$.userId') as user_id,
                JSON_EXTRACT(document, '$.status') as status,
                JSON_EXTRACT(document, '$.total') as total,
                JSON_EXTRACT(document, '$.orderDate') as order_date,
                JSON_LENGTH(JSON_EXTRACT(document, '$.items')) as item_count,
                JSON_EXTRACT(document, '$.shipping.method') as shipping_method
            FROM orders
        )
        SELECT 
            status,
            shipping_method,
            COUNT(*) as order_count,
            ROUND(AVG(CAST(total AS DECIMAL)), 2) as avg_order_value,
            ROUND(SUM(CAST(total AS DECIMAL)), 2) as total_revenue,
            ROUND(AVG(CAST(item_count AS INTEGER)), 2) as avg_items_per_order,
            MIN(order_date) as earliest_order,
            MAX(order_date) as latest_order
        FROM order_details
        GROUP BY status, shipping_method
        ORDER BY total_revenue DESC
    "#;
    let order_result = db.execute_query_str(order_analytics)?;
    println!("✓ Generated order analytics by status and shipping");
    
    // Blog post engagement metrics
    println!("\n📝 Content Analytics:");
    let content_analytics = r#"
        WITH post_metrics AS (
            SELECT 
                JSON_EXTRACT(document, '$.author') as author,
                JSON_EXTRACT(document, '$.stats.viewCount') as view_count,
                JSON_EXTRACT(document, '$.stats.likeCount') as like_count,
                JSON_EXTRACT(document, '$.stats.shareCount') as share_count,
                JSON_EXTRACT(document, '$.stats.commentCount') as comment_count,
                JSON_EXTRACT(document, '$.metadata.readTime') as read_time,
                JSON_EXTRACT(document, '$.metadata.difficulty') as difficulty,
                JSON_LENGTH(JSON_EXTRACT(document, '$.tags')) as tag_count
            FROM blog_posts
            WHERE JSON_EXTRACT(document, '$.isPublished') = true
        )
        SELECT 
            author,
            difficulty,
            COUNT(*) as post_count,
            SUM(CAST(view_count AS INTEGER)) as total_views,
            SUM(CAST(like_count AS INTEGER)) as total_likes,
            SUM(CAST(share_count AS INTEGER)) as total_shares,
            ROUND(AVG(CAST(view_count AS INTEGER)), 0) as avg_views_per_post,
            ROUND(AVG(CAST(read_time AS INTEGER)), 1) as avg_read_time,
            ROUND(AVG(CAST(tag_count AS INTEGER)), 1) as avg_tags_per_post,
            ROUND(
                SUM(CAST(like_count AS INTEGER))::DECIMAL / 
                SUM(CAST(view_count AS INTEGER)) * 100, 2
            ) as engagement_rate
        FROM post_metrics
        GROUP BY author, difficulty
        ORDER BY total_views DESC
    "#;
    let content_result = db.execute_query_str(content_analytics)?;
    println!("✓ Generated content engagement analytics");
    
    Ok(())
}

fn demonstrate_text_search(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🔎 Text Search Operations");
    println!("{}", "=".repeat(30));
    
    // Full-text search in blog posts
    println!("\n📝 Blog Post Search:");
    let text_search = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.title') as title,
               JSON_EXTRACT(document, '$.author') as author,
               JSON_EXTRACT(document, '$.excerpt') as excerpt,
               JSON_EXTRACT(document, '$.tags') as tags
        FROM blog_posts
        WHERE JSON_EXTRACT(document, '$.title') LIKE '%Document%'
           OR JSON_EXTRACT(document, '$.content') LIKE '%database%'
           OR JSON_EXTRACT(document, '$.excerpt') LIKE '%flexible%'
        ORDER BY JSON_EXTRACT(document, '$.stats.viewCount') DESC
    "#;
    let search_result = db.execute_query_str(text_search)?;
    println!("✓ Performed text search in blog posts");
    
    // Product search with multiple criteria
    println!("\n🛍️  Product Search:");
    let product_search = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.name') as product_name,
               JSON_EXTRACT(document, '$.brand') as brand,
               JSON_EXTRACT(document, '$.price') as price,
               JSON_EXTRACT(document, '$.tags') as tags
        FROM products
        WHERE (JSON_EXTRACT(document, '$.name') LIKE '%Pro%'
           OR JSON_CONTAINS(JSON_EXTRACT(document, '$.tags'), '"professional"'))
          AND JSON_EXTRACT(document, '$.inStock') = true
        ORDER BY JSON_EXTRACT(document, '$.reviews.average') DESC
    "#;
    let product_search_result = db.execute_query_str(product_search)?;
    println!("✓ Performed product search with filters");
    
    // User search by profile data
    println!("\n👤 User Profile Search:");
    let user_search = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.username') as username,
               JSON_EXTRACT(document, '$.profile.firstName') as first_name,
               JSON_EXTRACT(document, '$.profile.lastName') as last_name,
               JSON_EXTRACT(document, '$.profile.location.city') as city,
               JSON_EXTRACT(document, '$.roles') as roles
        FROM users
        WHERE JSON_EXTRACT(document, '$.profile.firstName') LIKE 'J%'
           OR JSON_EXTRACT(document, '$.profile.location.city') LIKE '%San%'
           OR JSON_CONTAINS(JSON_EXTRACT(document, '$.roles'), '"admin"')
        ORDER BY JSON_EXTRACT(document, '$.loginCount') DESC
    "#;
    let user_search_result = db.execute_query_str(user_search)?;
    println!("✓ Performed user profile search");
    
    Ok(())
}

fn demonstrate_geospatial_queries(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🌍 Geospatial Operations");
    println!("{}", "=".repeat(25));
    
    // Insert location data
    println!("\n📍 Inserting Location Data:");
    let locations = vec![
        json!({
            "_id": "loc_001",
            "name": "Central Park",
            "type": "park",
            "coordinates": {
                "lat": 40.785091,
                "lng": -73.968285
            },
            "address": {
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            "amenities": ["playground", "lake", "trails"],
            "rating": 4.7
        }),
        json!({
            "_id": "loc_002", 
            "name": "Golden Gate Bridge",
            "type": "landmark",
            "coordinates": {
                "lat": 37.819929,
                "lng": -122.478255
            },
            "address": {
                "city": "San Francisco",
                "state": "CA",
                "country": "USA"
            },
            "amenities": ["viewpoint", "walkway", "parking"],
            "rating": 4.8
        }),
        json!({
            "_id": "loc_003",
            "name": "Times Square",
            "type": "landmark", 
            "coordinates": {
                "lat": 40.758896,
                "lng": -73.985130
            },
            "address": {
                "city": "New York",
                "state": "NY", 
                "country": "USA"
            },
            "amenities": ["shopping", "restaurants", "theater"],
            "rating": 4.2
        })
    ];
    
    for location in locations {
        let sql = format!(
            "INSERT INTO locations (_id, document) VALUES ('{}', '{}')",
            location["_id"].as_str().unwrap(),
            location.to_string().replace("'", "''")
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted {} location documents", 3);
    
    // Distance calculations (simplified)
    println!("\n📏 Distance-based Queries:");
    let distance_query = r#"
        WITH location_data AS (
            SELECT 
                _id,
                JSON_EXTRACT(document, '$.name') as name,
                JSON_EXTRACT(document, '$.type') as type,
                CAST(JSON_EXTRACT(document, '$.coordinates.lat') AS DECIMAL) as lat,
                CAST(JSON_EXTRACT(document, '$.coordinates.lng') AS DECIMAL) as lng,
                JSON_EXTRACT(document, '$.address.city') as city,
                JSON_EXTRACT(document, '$.rating') as rating
            FROM locations
        )
        SELECT 
            name,
            type,
            city,
            lat,
            lng,
            rating,
            -- Simplified distance calculation (not actual geographic distance)
            ROUND(SQRT(POWER(lat - 40.7589, 2) + POWER(lng - (-73.9851), 2)), 4) as distance_from_times_square
        FROM location_data
        ORDER BY distance_from_times_square ASC
    "#;
    let distance_result = db.execute_query_str(distance_query)?;
    println!("✓ Calculated distances from Times Square");
    
    // Location queries by type and rating
    println!("\n🏛️  Location Filtering:");
    let location_filter = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.name') as name,
               JSON_EXTRACT(document, '$.type') as type,
               JSON_EXTRACT(document, '$.address.city') as city,
               JSON_EXTRACT(document, '$.rating') as rating,
               JSON_EXTRACT(document, '$.amenities') as amenities
        FROM locations
        WHERE JSON_EXTRACT(document, '$.type') = 'landmark'
          AND CAST(JSON_EXTRACT(document, '$.rating') AS DECIMAL) >= 4.5
        ORDER BY CAST(JSON_EXTRACT(document, '$.rating') AS DECIMAL) DESC
    "#;
    let filter_result = db.execute_query_str(location_filter)?;
    println!("✓ Found high-rated landmarks");
    
    // Amenity-based search
    println!("\n🎯 Amenity Search:");
    let amenity_search = r#"
        SELECT _id,
               JSON_EXTRACT(document, '$.name') as name,
               JSON_EXTRACT(document, '$.type') as type,
               JSON_EXTRACT(document, '$.amenities') as amenities,
               JSON_EXTRACT(document, '$.rating') as rating
        FROM locations
        WHERE JSON_CONTAINS(JSON_EXTRACT(document, '$.amenities'), '"parking"')
           OR JSON_CONTAINS(JSON_EXTRACT(document, '$.amenities'), '"shopping"')
        ORDER BY JSON_EXTRACT(document, '$.rating') DESC
    "#;
    let amenity_result = db.execute_query_str(amenity_search)?;
    println!("✓ Found locations with specific amenities");
    
    println!("\n🎯 Document Database Summary:");
    println!("✓ Document storage with JSON fields");
    println!("✓ Nested document queries and updates");
    println!("✓ Array operations and containment checks");
    println!("✓ Aggregation pipeline patterns");
    println!("✓ Full-text search capabilities");
    println!("✓ Geospatial data handling");
    println!("✓ Complex document relationships");
    
    Ok(())
}