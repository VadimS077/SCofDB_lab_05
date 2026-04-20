"""
LAB 04: Проверка идемпотентного повтора запроса.

Цель:
При повторном запросе с тем же Idempotency-Key вернуть
кэшированный результат без повторного списания.
"""

import pytest
import pytest_asyncio
import os
import uuid
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

def _database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@db:5432/marketplace",
    )


@pytest_asyncio.fixture
async def db_factory():
    database_url = _database_url()
    if "sqlite" in database_url:
        pytest.skip("Для LAB 04 нужен PostgreSQL, а не sqlite.")

    engine = create_async_engine(database_url, echo=False, pool_size=10, max_overflow=5)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def api_client():
    async with AsyncClient(
        base_url="http://127.0.0.1:8080",
    ) as client:
        yield client


@pytest_asyncio.fixture
async def test_order(db_factory):
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()
    email = f"retry_idem_{user_id.hex[:8]}@example.com"

    async with db_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, NOW())
                    """
                ),
                {"id": user_id, "email": email, "name": "Retry Idempotent"},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:id, :user_id, 'created', 0, NOW())
                    """
                ),
                {"id": order_id, "user_id": user_id},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, 'created', NOW())
                    """
                ),
                {"id": uuid.uuid4(), "order_id": order_id},
            )

    yield order_id

    async with db_factory() as session:
        async with session.begin():
            await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})


@pytest.mark.asyncio
async def test_retry_with_same_key_returns_cached_response(api_client, db_factory, test_order):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Сделать первый POST /api/payments/retry-demo (mode='unsafe')
       с заголовком Idempotency-Key: fixed-key-123.
    3) Повторить тот же POST с тем же ключом и тем же payload.
    4) Проверить:
       - второй ответ пришёл из кэша (через признак, который вы добавите,
         например header X-Idempotency-Replayed=true),
       - в order_status_history только одно событие paid,
       - в idempotency_keys есть запись completed с response_body/status_code.
    """
    order_id = test_order
    idem_key = "fixed-key-123"
    payload = {"order_id": str(order_id), "mode": "unsafe"}
    headers = {"Idempotency-Key": idem_key}

    async with db_factory() as session:
        async with session.begin():
            await session.execute(
                text("DELETE FROM idempotency_keys WHERE idempotency_key = :idempotency_key"),
                {"idempotency_key": idem_key},
            )

    first_response = await api_client.post(
        "/api/payments/retry-demo",
        json=payload,
        headers=headers,
    )
    second_response = await api_client.post(
        "/api/payments/retry-demo",
        json=payload,
        headers=headers,
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert first_response.json() == second_response.json()
    assert second_response.headers.get("X-Idempotency-Replayed") == "true"

    async with db_factory() as session:
        history_result = await session.execute(
            text(
                """
                SELECT id
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                """
            ),
            {"order_id": order_id},
        )
        paid_history = history_result.mappings().all()
        assert len(paid_history) == 1, "С одним Idempotency-Key должно быть только одно списание"

        key_result = await session.execute(
            text(
                """
                SELECT status, status_code, response_body
                FROM idempotency_keys
                WHERE idempotency_key = :idempotency_key
                  AND request_method = 'POST'
                  AND request_path = '/api/payments/retry-demo'
                """
            ),
            {"idempotency_key": idem_key},
        )
        key_row = key_result.mappings().first()

    assert key_row is not None
    assert key_row["status"] == "completed"
    assert key_row["status_code"] == 200
    assert key_row["response_body"] == first_response.json()


@pytest.mark.asyncio
async def test_same_key_different_payload_returns_conflict(api_client, db_factory, test_order):
    """
    TODO: Реализовать негативный тест.

    Один и тот же Idempotency-Key нельзя использовать с другим payload.
    Ожидается 409 Conflict (или эквивалентная бизнес-ошибка).
    """
    order_id = test_order
    idem_key = "same-key-different-payload"
    headers = {"Idempotency-Key": idem_key}

    first_payload = {"order_id": str(order_id), "mode": "unsafe"}
    second_payload = {"order_id": str(order_id), "mode": "for_update"}

    async with db_factory() as session:
        async with session.begin():
            await session.execute(
                text("DELETE FROM idempotency_keys WHERE idempotency_key = :idempotency_key"),
                {"idempotency_key": idem_key},
            )

    first_response = await api_client.post(
        "/api/payments/retry-demo",
        json=first_payload,
        headers=headers,
    )
    conflict_response = await api_client.post(
        "/api/payments/retry-demo",
        json=second_payload,
        headers=headers,
    )

    assert first_response.status_code == 200
    assert conflict_response.status_code == 409
    assert "different payload" in conflict_response.json()["detail"]
