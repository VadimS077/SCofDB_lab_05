"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Callable

from fastapi import Request, Response
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.db import DATABASE_URL


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware для идемпотентности POST-запросов оплаты.

    Идея:
    - Клиент отправляет `Idempotency-Key` в header.
    - Если запрос с таким ключом уже выполнялся для того же endpoint и payload,
      middleware возвращает кэшированный ответ (без повторного списания).
    """

    def __init__(self, app, ttl_seconds: int = 24 * 60 * 60):
        super().__init__(app)
        self.ttl_seconds = ttl_seconds
        self.target_paths = {"/api/payments/retry-demo"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать алгоритм.

        Рекомендуемая логика:
        1) Пропускать только целевые запросы:
           - method == POST
           - path в whitelist для платежей
        2) Читать Idempotency-Key из headers.
           Если ключа нет -> обычный call_next(request)
        3) Считать request_hash (например sha256 от body).
        4) В транзакции:
           - проверить запись в idempotency_keys
           - если completed и hash совпадает -> вернуть кэш (status_code + body)
           - если key есть, но hash другой -> вернуть 409 Conflict
           - если ключа нет -> создать запись processing
        5) Выполнить downstream request через call_next.
        6) Сохранить response в idempotency_keys со статусом completed.
        7) Вернуть response клиенту.

        Дополнительно:
        - обработайте кейс конкурентных одинаковых ключей
          (уникальный индекс + retry/select existing).
        """

        if not self._is_target_request(request):
            return await call_next(request)

        idempotency_key = request.headers.get("Idempotency-Key")
        if not idempotency_key:
            return await call_next(request)

        raw_body = await request.body()
        request_hash = self.build_request_hash(raw_body)
        request_method = request.method
        request_path = request.url.path
        expires_at = datetime.utcnow() + timedelta(seconds=self.ttl_seconds)

        existing = await self._fetch_existing(
            idempotency_key=idempotency_key,
            request_method=request_method,
            request_path=request_path,
        )
        existing_response = self._build_existing_response(existing, request_hash)
        if existing_response is not None:
            return existing_response

        try:
            await self._insert_processing_record(
                idempotency_key=idempotency_key,
                request_method=request_method,
                request_path=request_path,
                request_hash=request_hash,
                expires_at=expires_at,
            )
        except IntegrityError:
            existing = await self._fetch_existing(
                idempotency_key=idempotency_key,
                request_method=request_method,
                request_path=request_path,
            )
            existing_response = self._build_existing_response(existing, request_hash)
            if existing_response is not None:
                return existing_response
            return JSONResponse(
                status_code=409,
                content={"detail": "Idempotency key is already being processed"},
            )

        try:
            downstream_response = await call_next(request)
        except Exception:
            await self._mark_failed(
                idempotency_key=idempotency_key,
                request_method=request_method,
                request_path=request_path,
            )
            raise

        response_body_bytes = b""
        async for chunk in downstream_response.body_iterator:
            response_body_bytes += chunk

        response_body_obj = self._decode_response_body(
            response_body_bytes, downstream_response.headers.get("content-type", "")
        )
        await self._mark_completed(
            idempotency_key=idempotency_key,
            request_method=request_method,
            request_path=request_path,
            status_code=downstream_response.status_code,
            response_body=self.encode_response_payload(response_body_obj),
        )

        final_response = Response(
            content=response_body_bytes,
            status_code=downstream_response.status_code,
            media_type=downstream_response.media_type,
        )
        for header, value in downstream_response.headers.items():
            if header.lower() != "content-length":
                final_response.headers[header] = value
        return final_response

    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса для проверки reuse ключа с другим payload."""
        return hashlib.sha256(raw_body).hexdigest()

    @staticmethod
    def encode_response_payload(body_obj) -> str:
        """Сериализация response body для сохранения в idempotency_keys."""
        return json.dumps(body_obj, ensure_ascii=False)

    def _is_target_request(self, request: Request) -> bool:
        return request.method == "POST" and request.url.path in self.target_paths

    @staticmethod
    def _decode_response_body(body: bytes, content_type: str):
        if not body:
            return {}
        if "application/json" in content_type:
            try:
                return json.loads(body.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                pass
        try:
            return {"raw_text": body.decode("utf-8")}
        except UnicodeDecodeError:
            return {"raw_text": body.decode("utf-8", errors="replace")}

    async def _fetch_existing(
        self,
        *,
        idempotency_key: str,
        request_method: str,
        request_path: str,
    ):
        async with self._session() as session:
            result = await session.execute(
                text(
                    """
                    SELECT request_hash, status, status_code, response_body
                    FROM idempotency_keys
                    WHERE idempotency_key = :idempotency_key
                      AND request_method = :request_method
                      AND request_path = :request_path
                      AND expires_at > NOW()
                    """
                ),
                {
                    "idempotency_key": idempotency_key,
                    "request_method": request_method,
                    "request_path": request_path,
                },
            )
            return result.mappings().first()

    def _build_existing_response(self, existing, request_hash: str) -> Response | None:
        if not existing:
            return None

        if existing["request_hash"] != request_hash:
            return JSONResponse(
                status_code=409,
                content={"detail": "Idempotency key reuse with different payload"},
            )

        if existing["status"] == "completed":
            response = JSONResponse(
                status_code=existing["status_code"] or 200,
                content=existing["response_body"] or {},
            )
            response.headers["X-Idempotency-Replayed"] = "true"
            return response

        return JSONResponse(
            status_code=409,
            content={"detail": "Idempotency key is already being processed"},
        )

    async def _insert_processing_record(
        self,
        *,
        idempotency_key: str,
        request_method: str,
        request_path: str,
        request_hash: str,
        expires_at: datetime,
    ) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        INSERT INTO idempotency_keys (
                            idempotency_key,
                            request_method,
                            request_path,
                            request_hash,
                            status,
                            expires_at
                        )
                        VALUES (
                            :idempotency_key,
                            :request_method,
                            :request_path,
                            :request_hash,
                            'processing',
                            :expires_at
                        )
                        """
                    ),
                    {
                        "idempotency_key": idempotency_key,
                        "request_method": request_method,
                        "request_path": request_path,
                        "request_hash": request_hash,
                        "expires_at": expires_at,
                    },
                )

    async def _mark_failed(
        self,
        *,
        idempotency_key: str,
        request_method: str,
        request_path: str,
    ) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE idempotency_keys
                        SET status = 'failed'
                        WHERE idempotency_key = :idempotency_key
                          AND request_method = :request_method
                          AND request_path = :request_path
                        """
                    ),
                    {
                        "idempotency_key": idempotency_key,
                        "request_method": request_method,
                        "request_path": request_path,
                    },
                )

    async def _mark_completed(
        self,
        *,
        idempotency_key: str,
        request_method: str,
        request_path: str,
        status_code: int,
        response_body,
    ) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE idempotency_keys
                        SET status = 'completed',
                            status_code = :status_code,
                            response_body = CAST(:response_body AS JSONB)
                        WHERE idempotency_key = :idempotency_key
                          AND request_method = :request_method
                          AND request_path = :request_path
                        """
                    ),
                    {
                        "status_code": status_code,
                        "response_body": response_body,
                        "idempotency_key": idempotency_key,
                        "request_method": request_method,
                        "request_path": request_path,
                    },
                )

    @asynccontextmanager
    async def _session(self):
        engine = create_async_engine(DATABASE_URL, echo=False)
        session_factory = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )
        try:
            async with session_factory() as session:
                yield session
        finally:
            await engine.dispose()
