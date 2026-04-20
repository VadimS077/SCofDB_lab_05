"""
LAB 05: Rate limiting endpoint оплаты через Redis.
"""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.middleware.rate_limit_middleware import RateLimitMiddleware


@pytest.mark.asyncio
async def test_payment_endpoint_rate_limit():
    """
    TODO: Реализовать тест.

    Рекомендуемая проверка:
    1) Сделать N запросов оплаты в пределах одного окна.
    2) Проверить, что первые <= limit проходят.
    3) Следующие запросы получают 429 Too Many Requests.
    4) Проверить заголовки X-RateLimit-Limit / X-RateLimit-Remaining.
    """
    class InMemoryRedis:
        def __init__(self) -> None:
            self.counters: dict[str, int] = {}
            self.ttls: dict[str, int] = {}

        async def incr(self, key: str) -> int:
            value = self.counters.get(key, 0) + 1
            self.counters[key] = value
            return value

        async def expire(self, key: str, seconds: int):
            self.ttls[key] = seconds

        async def ttl(self, key: str) -> int:
            return self.ttls.get(key, -1)

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limit_per_window=2, window_seconds=10)

    @app.post("/api/payments/retry-demo")
    async def retry_demo():
        return {"ok": True}

    middleware_instance = app.user_middleware[0]
    middleware_instance.kwargs["limit_per_window"] = 2
    middleware_instance.kwargs["window_seconds"] = 10

    fake_redis = InMemoryRedis()
    original_init = RateLimitMiddleware.__init__

    def patched_init(self, app, limit_per_window: int = 5, window_seconds: int = 10):
        original_init(self, app, limit_per_window=limit_per_window, window_seconds=window_seconds)
        self.redis = fake_redis

    RateLimitMiddleware.__init__ = patched_init
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r1 = await client.post("/api/payments/retry-demo")
            r2 = await client.post("/api/payments/retry-demo")
            r3 = await client.post("/api/payments/retry-demo")

            assert r1.status_code == 200
            assert r2.status_code == 200
            assert r3.status_code == 429

            assert r1.headers["X-RateLimit-Limit"] == "2"
            assert r1.headers["X-RateLimit-Remaining"] == "1"
            assert r2.headers["X-RateLimit-Remaining"] == "0"
            assert r3.headers["X-RateLimit-Remaining"] == "0"
    finally:
        RateLimitMiddleware.__init__ = original_init
