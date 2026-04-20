"""Сервис для демонстрации конкурентных оплат.

Этот модуль содержит два метода оплаты:
1. pay_order_unsafe() - небезопасная реализация (READ COMMITTED без блокировок)
2. pay_order_safe() - безопасная реализация (REPEATABLE READ + FOR UPDATE)
"""

import asyncio
import uuid
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.exceptions import OrderAlreadyPaidError, OrderNotFoundError


class PaymentService:
    """Сервис для обработки платежей с разными уровнями изоляции."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def pay_order_unsafe(self, order_id: uuid.UUID) -> dict:
        """
        НЕБЕЗОПАСНАЯ реализация оплаты заказа.

        Использует READ COMMITTED (по умолчанию) без блокировок.
        ЛОМАЕТСЯ при конкурентных запросах - может привести к двойной оплате!

        TODO: Реализовать метод следующим образом:

        1. Прочитать текущий статус заказа:
           SELECT status FROM orders WHERE id = :order_id

        2. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError

        3. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid'
           WHERE id = :order_id AND status = 'created'

        4. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())

        5. Сделать commit

        ВАЖНО: НЕ используйте FOR UPDATE!
        ВАЖНО: НЕ меняйте уровень изоляции (оставьте READ COMMITTED по умолчанию)!

        Args:
            order_id: ID заказа для оплаты

        Returns:
            dict с информацией о заказе после оплаты

        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        async with self.session.begin():
            row = await self.session.execute(
                text("SELECT status FROM orders WHERE id = :order_id"),
                {"order_id": order_id},
            )
            status = row.scalar_one_or_none()
            if status is None:
                raise OrderNotFoundError(order_id)
            if status != "created":
                raise OrderAlreadyPaidError(order_id)

            await asyncio.sleep(0.1)

            await self.session.execute(
                text(
                    """
                    UPDATE orders
                    SET status = 'paid'
                    WHERE id = :order_id AND status = 'created'
                    """
                ),
                {"order_id": order_id},
            )

            hid = uuid.uuid4()
            await self.session.execute(
                text(
                    """
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, 'paid', NOW())
                    """
                ),
                {"id": hid, "order_id": order_id},
            )

        return {"order_id": str(order_id), "status": "paid"}

    async def pay_order_safe(
        self,
        order_id: uuid.UUID,
        *,
        _delay_after_lock_sec: float = 0.0,
    ) -> dict:
        """
        БЕЗОПАСНАЯ реализация оплаты заказа.

        Использует REPEATABLE READ + FOR UPDATE для предотвращения race condition.
        Корректно работает при конкурентных запросах.

        TODO: Реализовать метод следующим образом:

        1. Установить уровень изоляции REPEATABLE READ:
           await self.session.execute(
               text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
           )

        2. Заблокировать строку заказа для обновления:
           SELECT status FROM orders WHERE id = :order_id FOR UPDATE

           ВАЖНО: FOR UPDATE гарантирует, что другие транзакции будут ЖДАТЬ
           освобождения блокировки. Это предотвращает race condition.

        3. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError

        4. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid'
           WHERE id = :order_id AND status = 'created'

        5. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())

        6. Сделать commit

        ВАЖНО: Обязательно используйте FOR UPDATE!
        ВАЖНО: Обязательно установите REPEATABLE READ!

        Args:
            order_id: ID заказа для оплаты

        Returns:
            dict с информацией о заказе после оплаты

        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        async with self.session.begin():
            await self.session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )
            row = await self.session.execute(
                text(
                    """
                    SELECT status FROM orders
                    WHERE id = :order_id
                    FOR UPDATE
                    """
                ),
                {"order_id": order_id},
            )
            status = row.scalar_one_or_none()
            if status is None:
                raise OrderNotFoundError(order_id)
            if status != "created":
                raise OrderAlreadyPaidError(order_id)

            if _delay_after_lock_sec > 0:
                await asyncio.sleep(_delay_after_lock_sec)

            upd = await self.session.execute(
                text(
                    """
                    UPDATE orders
                    SET status = 'paid'
                    WHERE id = :order_id AND status = 'created'
                    """
                ),
                {"order_id": order_id},
            )
            if upd.rowcount != 1:
                raise OrderAlreadyPaidError(order_id)

            hid = uuid.uuid4()
            await self.session.execute(
                text(
                    """
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, 'paid', NOW())
                    """
                ),
                {"id": hid, "order_id": order_id},
            )

        return {"order_id": str(order_id), "status": "paid"}

    async def get_payment_history(self, order_id: uuid.UUID) -> list[dict[str, Any]]:
        """
        Получить историю оплат для заказа.

        Используется для проверки, сколько раз был оплачен заказ.

        TODO: Реализовать метод:

        SELECT id, order_id, status, changed_at
        FROM order_status_history
        WHERE order_id = :order_id AND status = 'paid'
        ORDER BY changed_at

        Args:
            order_id: ID заказа

        Returns:
            Список словарей с записями об оплате
        """
        result = await self.session.execute(
            text(
                """
                SELECT id, order_id, status, changed_at
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                ORDER BY changed_at
                """
            ),
            {"order_id": order_id},
        )
        rows = result.mappings().all()
        return [dict(r) for r in rows]
