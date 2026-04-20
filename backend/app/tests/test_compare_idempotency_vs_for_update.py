"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)
"""

import pytest
import os
import uuid
import pytest_asyncio
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
async def test_orders(db_factory):
    user_id = uuid.uuid4()
    order_for_update = uuid.uuid4()
    order_idempotent = uuid.uuid4()
    email = f"compare_{user_id.hex[:8]}@example.com"

    async with db_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, NOW())
                    """
                ),
                {"id": user_id, "email": email, "name": "Compare Approaches"},
            )
            for order_id in (order_for_update, order_idempotent):
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

    yield {"for_update": order_for_update, "idempotent": order_idempotent, "user_id": user_id}

    async with db_factory() as session:
        async with session.begin():
            await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})


@pytest.mark.asyncio
async def test_compare_for_update_and_idempotency_behaviour(api_client, db_factory, test_orders):
    """
    TODO: Реализовать сравнительный тест/сценарий.

    Минимум сравнения:
    1) Повтор запроса с mode='for_update':
       - защита от гонки на уровне БД,
       - повтор может вернуть бизнес-ошибку \"already paid\".
    2) Повтор запроса с mode='unsafe' + Idempotency-Key:
       - второй вызов возвращает тот же кэшированный успешный ответ,
         без повторного списания.

    В конце добавьте вывод:
    - чем отличаются цели и UX двух подходов,
    - почему они не взаимоисключающие и могут использоваться вместе.
    """
    for_update_order_id = test_orders["for_update"]
    idempotent_order_id = test_orders["idempotent"]

    for_update_payload = {"order_id": str(for_update_order_id), "mode": "for_update"}
    for_update_first = await api_client.post("/api/payments/retry-demo", json=for_update_payload)
    for_update_second = await api_client.post("/api/payments/retry-demo", json=for_update_payload)

    assert for_update_first.status_code == 200
    assert for_update_second.status_code == 200
    assert for_update_first.json()["success"] is True
    assert for_update_second.json()["success"] is False
    assert "already paid" in for_update_second.json()["message"].lower()

    idem_key = f"compare-idem-{uuid.uuid4()}"
    idem_headers = {"Idempotency-Key": idem_key}
    idem_payload = {"order_id": str(idempotent_order_id), "mode": "unsafe"}

    idem_first = await api_client.post(
        "/api/payments/retry-demo",
        json=idem_payload,
        headers=idem_headers,
    )
    idem_second = await api_client.post(
        "/api/payments/retry-demo",
        json=idem_payload,
        headers=idem_headers,
    )

    assert idem_first.status_code == 200
    assert idem_second.status_code == 200
    assert idem_first.json() == idem_second.json()
    assert idem_second.headers.get("X-Idempotency-Replayed") == "true"

    async with db_factory() as session:
        for_update_paid = await session.execute(
            text(
                """
                SELECT COUNT(*) AS cnt
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                """
            ),
            {"order_id": for_update_order_id},
        )
        idem_paid = await session.execute(
            text(
                """
                SELECT COUNT(*) AS cnt
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                """
            ),
            {"order_id": idempotent_order_id},
        )
        for_update_count = for_update_paid.scalar_one()
        idempotent_count = idem_paid.scalar_one()

    assert for_update_count == 1
    assert idempotent_count == 1

    print("for update vs idempotency-key:")
    print("  for udate protects db race conditions, retry may return business error.")
    print("  idempotency-key protects api retry UX, retry returns cached success.")
    print("  these mechanisms are complementary and should be used together.")
