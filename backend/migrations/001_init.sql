-- ============================================
-- Схема базы данных маркетплейса
-- ============================================

-- Включаем расширение UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- TODO: Создать таблицу order_statuses
-- Столбцы: status (PK), description
CREATE TABLE IF NOT EXISTS order_statuses (
    status VARCHAR(32) PRIMARY KEY,
    description TEXT NOT NULL
);


-- TODO: Вставить значения статусов
-- created, paid, cancelled, shipped, completed
INSERT INTO order_statuses (status, description)
VALUES
    ('created', 'Order was created'),
    ('paid', 'Order was paid'),
    ('cancelled', 'Order was cancelled'),
    ('shipped', 'Order was shipped'),
    ('completed', 'Order was completed')
ON CONFLICT (status) DO NOTHING;


-- TODO: Создать таблицу users
-- Столбцы: id (UUID PK), email, name, created_at
-- Ограничения:
--   - email UNIQUE
--   - email NOT NULL и не пустой
--   - email валидный (regex через CHECK)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_users_email_not_empty CHECK (length(trim(email)) > 0),
    CONSTRAINT chk_users_email_format CHECK (
        email ~ '^[A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-]{1,}$'
    )
);


-- TODO: Создать таблицу orders
-- Столбцы: id (UUID PK), user_id (FK), status (FK), total_amount, created_at
-- Ограничения:
--   - user_id -> users(id)
--   - status -> order_statuses(status)
--   - total_amount >= 0
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL REFERENCES order_statuses(status),
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_orders_total_amount_non_negative CHECK (total_amount >= 0)
);


-- TODO: Создать таблицу order_items
-- Столбцы: id (UUID PK), order_id (FK), product_name, price, quantity
-- Ограничения:
--   - order_id -> orders(id) CASCADE
--   - price >= 0
--   - quantity > 0
--   - product_name не пустой
CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_name VARCHAR(255) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT chk_order_items_price_non_negative CHECK (price >= 0),
    CONSTRAINT chk_order_items_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_order_items_product_name_not_empty CHECK (length(trim(product_name)) > 0)
);


-- TODO: Создать таблицу order_status_history
-- Столбцы: id (UUID PK), order_id (FK), status (FK), changed_at
-- Ограничения:
--   - order_id -> orders(id) CASCADE
--   - status -> order_statuses(status)
CREATE TABLE IF NOT EXISTS order_status_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL REFERENCES order_statuses(status),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- КРИТИЧЕСКИЙ ИНВАРИАНТ: Нельзя оплатить заказ дважды
-- ============================================
-- TODO: Создать функцию триггера check_order_not_already_paid()
-- При изменении статуса на 'paid' проверить что его нет в истории
-- Если есть - RAISE EXCEPTION
CREATE OR REPLACE FUNCTION check_order_not_already_paid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'paid' AND OLD.status <> 'paid' THEN
        IF EXISTS (
            SELECT 1
            FROM order_status_history osh
            WHERE osh.order_id = NEW.id
              AND osh.status = 'paid'
        ) THEN
            RAISE EXCEPTION 'Order % cannot be paid twice', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- TODO: Создать триггер trigger_check_order_not_already_paid
-- BEFORE UPDATE ON orders FOR EACH ROW
DROP TRIGGER IF EXISTS trigger_check_order_not_already_paid ON orders;
CREATE TRIGGER trigger_check_order_not_already_paid
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION check_order_not_already_paid();


-- ============================================
-- БОНУС (опционально)
-- ============================================
-- TODO: Триггер автоматического пересчета total_amount
-- TODO: Триггер автоматической записи в историю при изменении статуса
-- TODO: Триггер записи начального статуса при создании заказа
