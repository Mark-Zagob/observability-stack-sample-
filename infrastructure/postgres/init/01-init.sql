-- infrastructure/postgres/init/01-init.sql

-- ===========================================
-- Database cho Data Pipeline
-- ===========================================

-- Tạo schema cho e-commerce data
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Bảng orders - nơi lưu data từ Kafka consumer
CREATE TABLE ecommerce.orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    
    -- Indexes cho queries thường dùng
    CONSTRAINT chk_quantity CHECK (quantity > 0),
    CONSTRAINT chk_price CHECK (unit_price > 0)
);

CREATE INDEX idx_orders_customer ON ecommerce.orders(customer_id);
CREATE INDEX idx_orders_created_at ON ecommerce.orders(created_at);
CREATE INDEX idx_orders_status ON ecommerce.orders(order_status);
CREATE INDEX idx_orders_category ON ecommerce.orders(category);

-- Bảng để track processing metrics
CREATE TABLE ecommerce.processing_logs (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    records_processed INTEGER NOT NULL,
    records_failed INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- View cho monitoring
CREATE VIEW ecommerce.orders_summary AS
SELECT 
    DATE(created_at) as order_date,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM ecommerce.orders
GROUP BY DATE(created_at)
ORDER BY order_date DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA ecommerce TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ecommerce TO postgres;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully!';
END $$;
