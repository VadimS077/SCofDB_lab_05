"""
LAB 05: Демонстрация неконсистентности кэша.
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.infrastructure.db import get_db
from app.main import app


class InMemoryRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        _ = ex
        self.store[key] = value

    async def delete(self, key: str):
        self.store.pop(key, None)


@pytest.mark.asyncio
async def test_stale_order_card_when_db_updated_without_invalidation(db_session, monkeypatch):
    """
    TODO: Реализовать сценарий:
    1) Прогреть кэш карточки заказа (GET /api/cache-demo/orders/{id}/card?use_cache=true).
    2) Изменить заказ в БД через endpoint mutate-without-invalidation.
    3) Повторно запросить карточку с use_cache=true.
    4) Проверить, что клиент получает stale данные из кэша.
    """
    fake_redis = InMemoryRedis()
    monkeypatch.setattr("app.application.cache_service.get_redis", lambda: fake_redis)

    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    try:
        user_id = str(uuid.uuid4())
        order_id = str(uuid.uuid4())
        item_id = str(uuid.uuid4())
        await db_session.execute(
            text(
                """
                INSERT INTO users (id, email, name, created_at)
                VALUES (:id, :email, :name, CURRENT_TIMESTAMP)
                """
            ),
            {"id": user_id, "email": "cache-stale@example.com", "name": "Cache User"},
        )
        await db_session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, 'created', 100, CURRENT_TIMESTAMP)
                """
            ),
            {"id": order_id, "user_id": user_id},
        )
        await db_session.execute(
            text(
                """
                INSERT INTO order_items (id, order_id, product_name, price, quantity, subtotal)
                VALUES (:id, :order_id, 'Phone', 100, 1, 100)
                """
            ),
            {"id": item_id, "order_id": order_id},
        )
        await db_session.commit()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            warm_response = await client.get(
                f"/api/cache-demo/orders/{order_id}/card?use_cache=true"
            )
            assert warm_response.status_code == 200
            assert warm_response.json()["total_amount"] == 100.0

            mutate_response = await client.post(
                f"/api/cache-demo/orders/{order_id}/mutate-without-invalidation",
                json={"new_total_amount": 777},
            )
            assert mutate_response.status_code == 200
            assert mutate_response.json()["cache_invalidated"] is False

            stale_response = await client.get(
                f"/api/cache-demo/orders/{order_id}/card?use_cache=true"
            )
            assert stale_response.status_code == 200
            assert stale_response.json()["total_amount"] == 100.0
    finally:
        app.dependency_overrides.clear()
