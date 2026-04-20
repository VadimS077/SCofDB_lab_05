"""
LAB 04: Демонстрация проблемы retry без идемпотентности.

Сценарий:
1) Клиент отправил запрос на оплату.
2) До получения ответа \"сеть оборвалась\" (моделируем повтором запроса).
3) Клиент повторил запрос БЕЗ Idempotency-Key.
4) В unsafe-режиме возможна двойная оплата.
"""

import asyncio
import os
import pytest
import pytest_asyncio
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
    email = f"retry_unsafe_{user_id.hex[:8]}@example.com"

    async with db_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, NOW())
                    """
                ),
                {"id": user_id, "email": email, "name": "Retry Unsafe"},
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
async def test_retry_without_idempotency_can_double_pay(api_client, db_factory, test_order):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Выполнить две параллельные попытки POST /api/payments/retry-demo
       с mode='unsafe' и БЕЗ заголовка Idempotency-Key.
    3) Проверить историю order_status_history:
       - paid-событий больше 1 (или иная метрика двойного списания).
    4) Вывести понятный отчёт в stdout:
       - сколько попыток
       - сколько paid в истории
       - почему это проблема.
    """
    order_id = test_order
    payload = {"order_id": str(order_id), "mode": "unsafe"}

    first_call = api_client.post("/api/payments/retry-demo", json=payload)
    second_call = api_client.post("/api/payments/retry-demo", json=payload)
    responses = await asyncio.gather(first_call, second_call)

    for response in responses:
        assert response.status_code == 200

    async with db_factory() as session:
        history_result = await session.execute(
            text(
                """
                SELECT id, changed_at
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                ORDER BY changed_at
                """
            ),
            {"order_id": order_id},
        )
        paid_history = history_result.mappings().all()

    paid_count = len(paid_history)
    assert paid_count > 1, "Без Idempotency-Key в unsafe-режиме должна проявиться двойная оплата"

    print("Retry without idempotency report:")
    print(f"  total_attempts={len(responses)}")
    print(f"  paid_events_in_history={paid_count}")
    print("  problem=one client intent was processed multiple times")
