"""Rate limiting middleware template for LAB 05."""

from typing import Callable

from fastapi import Request, Response
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.cache_keys import payment_rate_limit_key
from app.infrastructure.redis_client import get_redis


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Redis-based rate limiting для endpoint оплаты.

    Цель:
    - защита от DDoS/шторма запросов;
    - защита от случайных повторных кликов пользователя.
    """

    def __init__(self, app, limit_per_window: int = 5, window_seconds: int = 10):
        super().__init__(app)
        self.limit_per_window = limit_per_window
        self.window_seconds = window_seconds
        self.redis = get_redis()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать Redis rate limiting.

        Рекомендуемая логика:
        1) Применять только к endpoint оплаты:
           - /api/orders/{order_id}/pay
           - /api/payments/retry-demo
        2) Сформировать subject:
           - user_id (если есть), иначе client IP.
        3) Использовать Redis INCR + EXPIRE:
           - key = rate_limit:pay:{subject}
           - если counter > limit_per_window -> 429 Too Many Requests.
        4) Для прохождения запроса добавить в ответ headers:
           - X-RateLimit-Limit
           - X-RateLimit-Remaining
        """

        if not self._is_payment_endpoint(request):
            return await call_next(request)

        subject = self._build_subject(request)
        key = payment_rate_limit_key(subject)

        current = await self.redis.incr(key)
        if current == 1:
            await self.redis.expire(key, self.window_seconds)

        remaining = max(self.limit_per_window - current, 0)
        ttl = await self.redis.ttl(key)

        if current > self.limit_per_window:
            response = JSONResponse(
                status_code=429,
                content={"detail": "Too Many Requests"},
            )
            response.headers["X-RateLimit-Limit"] = str(self.limit_per_window)
            response.headers["X-RateLimit-Remaining"] = "0"
            response.headers["X-RateLimit-Reset"] = str(max(ttl, 0))
            return response

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.limit_per_window)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(max(ttl, 0))
        return response

    @staticmethod
    def _is_payment_endpoint(request: Request) -> bool:
        if request.method != "POST":
            return False
        path = request.url.path
        return path.endswith("/pay") or path == "/api/payments/retry-demo"

    @staticmethod
    def _build_subject(request: Request) -> str:
        user_id = request.headers.get("X-User-Id")
        if user_id:
            return f"user:{user_id}"
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"
