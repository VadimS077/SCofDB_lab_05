-- ============================================
-- LAB 04: Идемпотентность платежных запросов
-- ============================================

-- TODO:
-- Создайте таблицу idempotency_keys, которая хранит:
-- 1) сам idempotency key
-- 2) идентификатор запроса (method + path)
-- 3) хэш/сигнатуру тела запроса (чтобы обнаруживать reuse ключа с другим payload)
-- 4) статус обработки (processing / completed / failed)
-- 5) кэш ответа (status code + body)
-- 6) timestamp'ы (created_at, updated_at, expires_at)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) NOT NULL,
    request_method VARCHAR(16) NOT NULL,
    request_path TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'processing',
    status_code INTEGER,
    response_body JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    CONSTRAINT idempotency_status_check CHECK (status IN ('processing', 'completed', 'failed'))
);

-- Рекомендуемый каркас (можно изменить при обосновании):
--
-- CREATE TABLE idempotency_keys (
--     id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
--     idempotency_key VARCHAR(255) NOT NULL,
--     request_method VARCHAR(16) NOT NULL,
--     request_path TEXT NOT NULL,
--     request_hash TEXT NOT NULL,
--     status VARCHAR(32) NOT NULL DEFAULT 'processing',
--     status_code INTEGER,
--     response_body JSONB,
--     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
--     expires_at TIMESTAMP NOT NULL,
--     CONSTRAINT idempotency_status_check CHECK (status IN ('processing', 'completed', 'failed'))
-- );

-- TODO:
-- Добавьте уникальность ключа в рамках endpoint:
--   UNIQUE (idempotency_key, request_method, request_path)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_idempotency_key_method_path'
    ) THEN
        ALTER TABLE idempotency_keys
            ADD CONSTRAINT uq_idempotency_key_method_path
            UNIQUE (idempotency_key, request_method, request_path);
    END IF;
END;
$$;

-- TODO:
-- Добавьте индексы:
-- 1) для очистки просроченных ключей (expires_at)
-- 2) для быстрых lookup по ключу/пути/методу
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at);

CREATE INDEX IF NOT EXISTS idx_idempotency_lookup
    ON idempotency_keys (idempotency_key, request_method, request_path);

-- TODO (опционально):
-- триггер автообновления updated_at
CREATE OR REPLACE FUNCTION set_idempotency_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_set_idempotency_updated_at ON idempotency_keys;
CREATE TRIGGER trigger_set_idempotency_updated_at
BEFORE UPDATE ON idempotency_keys
FOR EACH ROW
EXECUTE FUNCTION set_idempotency_updated_at();
