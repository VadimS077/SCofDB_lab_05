"""Cache service template for LAB 05."""

import json
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.cache_keys import catalog_key, order_card_key
from app.infrastructure.redis_client import get_redis


class CacheService:
    """
    Сервис кэширования каталога и карточки заказа.

    TODO:
    - реализовать методы через Redis client + БД;
    - добавить TTL и версионирование ключей.
    """

    def __init__(
        self,
        db: AsyncSession,
        *,
        redis_client: Redis | None = None,
        catalog_ttl_seconds: int = 60,
        order_card_ttl_seconds: int = 60,
    ) -> None:
        self.db = db
        self.redis = redis_client or get_redis()
        self.catalog_ttl_seconds = catalog_ttl_seconds
        self.order_card_ttl_seconds = order_card_ttl_seconds

    async def get_catalog(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть catalog из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        key = catalog_key()
        if use_cache:
            cached = await self.redis.get(key)
            if cached:
                return json.loads(cached)

        result = await self.db.execute(
            text(
                """
                SELECT
                    oi.product_name,
                    count(*) AS order_lines,
                    sum(oi.quantity) AS sold_qty,
                    round(avg(oi.price)::numeric, 2) AS avg_price
                FROM order_items oi
                GROUP BY oi.product_name
                ORDER BY sold_qty DESC
                LIMIT 100
                """
            )
        )
        rows = result.mappings().all()
        catalog = [dict(row) for row in rows]

        if use_cache:
            await self.redis.set(
                key,
                json.dumps(catalog, default=str),
                ex=self.catalog_ttl_seconds,
            )
        return catalog

    async def get_order_card(self, order_id: str, *, use_cache: bool = True) -> dict[str, Any]:
        """
        TODO:
        1) Попытаться вернуть карточку заказа из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        key = order_card_key(order_id)
        if use_cache:
            cached = await self.redis.get(key)
            if cached:
                return json.loads(cached)

        order_result = await self.db.execute(
            text(
                """
                SELECT id, user_id, status, total_amount, created_at
                FROM orders
                WHERE id = :order_id
                """
            ),
            {"order_id": order_id},
        )
        order = order_result.mappings().first()
        if not order:
            return {}

        items_result = await self.db.execute(
            text(
                """
                SELECT product_name, price, quantity
                FROM order_items
                WHERE order_id = :order_id
                ORDER BY product_name
                """
            ),
            {"order_id": order_id},
        )
        items = [dict(row) for row in items_result.mappings().all()]

        order_card = {
            "id": str(order["id"]),
            "user_id": str(order["user_id"]),
            "status": order["status"],
            "total_amount": float(order["total_amount"]),
            "created_at": str(order["created_at"]),
            "items": items,
        }
        if use_cache:
            await self.redis.set(
                key,
                json.dumps(order_card, default=str),
                ex=self.order_card_ttl_seconds,
            )
        return order_card

    async def invalidate_order_card(self, order_id: str) -> None:
        """TODO: Удалить ключ карточки заказа из Redis."""
        await self.redis.delete(order_card_key(order_id))

    async def invalidate_catalog(self) -> None:
        """TODO: Удалить ключ каталога из Redis."""
        await self.redis.delete(catalog_key())
