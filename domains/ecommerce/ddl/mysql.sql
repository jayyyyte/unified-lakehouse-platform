-- =====================================================================
-- MOSAIC :: MySQL 8.0 source schema (E-Commerce TRANSACTIONAL data)
-- Domain: ecommerce.  Source of GR1 Appendix A.1 -- kept faithful to the report.
-- Role: high-frequency OLTP writes; captured via binlog (Debezium MySQL connector).
-- =====================================================================
CREATE DATABASE IF NOT EXISTS ecommerce_tx
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecommerce_tx;

-- ---- orders ----
CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGINT        NOT NULL AUTO_INCREMENT,
    user_id         BIGINT        NOT NULL,              -- FK -> postgres users.id
    status ENUM('pending','paid','shipped','delivered','cancelled')
                                  NOT NULL DEFAULT 'pending',
    total_amount    DECIMAL(12,2) NOT NULL,
    currency        CHAR(3)       NOT NULL DEFAULT 'VND',
    region          VARCHAR(32)   NOT NULL,              -- used for bucket partitioning
    buyer_email     VARCHAR(255)  NULL,                  -- PII: masked at SMT layer
    buyer_phone     VARCHAR(32)   NULL,                  -- PII: masked at SMT layer
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    KEY idx_orders_user (user_id),
    KEY idx_orders_created (created_at)
) ENGINE=InnoDB;

-- ---- order_items ----
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   BIGINT        NOT NULL AUTO_INCREMENT,
    order_id        BIGINT        NOT NULL,
    product_id      BIGINT        NOT NULL,              -- FK -> postgres products.id
    quantity        INT           NOT NULL,
    unit_price      DECIMAL(12,2) NOT NULL,
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_item_id),
    KEY idx_items_order (order_id),
    KEY idx_items_product (product_id)
) ENGINE=InnoDB;

-- ---- payments ----
CREATE TABLE IF NOT EXISTS payments (
    payment_id      BIGINT        NOT NULL AUTO_INCREMENT,
    order_id        BIGINT        NOT NULL,
    method          ENUM('card','ewallet','cod','bank_transfer') NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    raw_card_number VARCHAR(32)   NULL,                  -- PII: DROPPED at SMT layer
    status          ENUM('authorized','captured','failed','refunded') NOT NULL,
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_id),
    KEY idx_pay_order (order_id)
) ENGINE=InnoDB;

-- ---- CDC replication user ----
-- Grants required by Debezium: SELECT (snapshot) + REPLICATION privileges.
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED BY 'cdcpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
