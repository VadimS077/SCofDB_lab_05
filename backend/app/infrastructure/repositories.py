"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    """Репозиторий для User."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(user: User) -> None
    # Используйте INSERT ... ON CONFLICT DO UPDATE
    async def save(self, user: User) -> None:
        await self.session.execute(
            text(
                """
                INSERT INTO users (id, email, name, created_at)
                VALUES (:id, :email, :name, :created_at)
                ON CONFLICT (id) DO UPDATE
                SET email = EXCLUDED.email,
                    name = EXCLUDED.name
                """
            ),
            {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "created_at": user.created_at,
            },
        )

    # TODO: Реализовать find_by_id(user_id: UUID) -> Optional[User]
    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        result = await self.session.execute(
            text(
                """
                SELECT id, email, name, created_at
                FROM users
                WHERE id = :id
                """
            ),
            {"id": user_id},
        )
        row = result.mappings().first()
        if not row:
            return None
        return User(
            id=row["id"],
            email=row["email"],
            name=row["name"],
            created_at=row["created_at"],
        )

    # TODO: Реализовать find_by_email(email: str) -> Optional[User]
    async def find_by_email(self, email: str) -> Optional[User]:
        result = await self.session.execute(
            text(
                """
                SELECT id, email, name, created_at
                FROM users
                WHERE email = :email
                """
            ),
            {"email": email},
        )
        row = result.mappings().first()
        if not row:
            return None
        return User(
            id=row["id"],
            email=row["email"],
            name=row["name"],
            created_at=row["created_at"],
        )

    # TODO: Реализовать find_all() -> List[User]
    async def find_all(self) -> List[User]:
        result = await self.session.execute(
            text(
                """
                SELECT id, email, name, created_at
                FROM users
                ORDER BY created_at ASC
                """
            )
        )
        rows = result.mappings().all()
        return [
            User(
                id=row["id"],
                email=row["email"],
                name=row["name"],
                created_at=row["created_at"],
            )
            for row in rows
        ]


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(order: Order) -> None
    # Сохранить заказ, товары и историю статусов
    async def save(self, order: Order) -> None:
        await self.session.execute(
            text(
                """
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, :created_at)
                ON CONFLICT (id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    status = EXCLUDED.status,
                    total_amount = EXCLUDED.total_amount
                """
            ),
            {
                "id": order.id,
                "user_id": order.user_id,
                "status": order.status.value,
                "total_amount": order.total_amount,
                "created_at": order.created_at,
            },
        )

        await self.session.execute(
            text("DELETE FROM order_items WHERE order_id = :order_id"),
            {"order_id": order.id},
        )
        for item in order.items:
            await self.session.execute(
                text(
                    """
                    INSERT INTO order_items (id, order_id, product_name, price, quantity)
                    VALUES (:id, :order_id, :product_name, :price, :quantity)
                    """
                ),
                {
                    "id": item.id,
                    "order_id": order.id,
                    "product_name": item.product_name,
                    "price": item.price,
                    "quantity": item.quantity,
                },
            )

        await self.session.execute(
            text("DELETE FROM order_status_history WHERE order_id = :order_id"),
            {"order_id": order.id},
        )
        for change in order.status_history:
            await self.session.execute(
                text(
                    """
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, :status, :changed_at)
                    """
                ),
                {
                    "id": change.id,
                    "order_id": order.id,
                    "status": change.status.value,
                    "changed_at": change.changed_at,
                },
            )

    # TODO: Реализовать find_by_id(order_id: UUID) -> Optional[Order]
    # Загрузить заказ со всеми товарами и историей
    # Используйте object.__new__(Order) чтобы избежать __post_init__
    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        result = await self.session.execute(
            text(
                """
                SELECT id, user_id, status, total_amount, created_at
                FROM orders
                WHERE id = :id
                """
            ),
            {"id": order_id},
        )
        row = result.mappings().first()
        if not row:
            return None
        return await self._load_full_order(row)

    # TODO: Реализовать find_by_user(user_id: UUID) -> List[Order]
    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        result = await self.session.execute(
            text(
                """
                SELECT id, user_id, status, total_amount, created_at
                FROM orders
                WHERE user_id = :user_id
                ORDER BY created_at DESC
                """
            ),
            {"user_id": user_id},
        )
        rows = result.mappings().all()
        return [await self._load_full_order(row) for row in rows]

    # TODO: Реализовать find_all() -> List[Order]
    async def find_all(self) -> List[Order]:
        result = await self.session.execute(
            text(
                """
                SELECT id, user_id, status, total_amount, created_at
                FROM orders
                ORDER BY created_at DESC
                """
            )
        )
        rows = result.mappings().all()
        return [await self._load_full_order(row) for row in rows]

    async def _load_full_order(self, row) -> Order:
        order = object.__new__(Order)
        order.id = row["id"]
        order.user_id = row["user_id"]
        order.status = OrderStatus(row["status"])
        order.total_amount = Decimal(row["total_amount"])
        order.created_at = row["created_at"]

        item_result = await self.session.execute(
            text(
                """
                SELECT id, order_id, product_name, price, quantity
                FROM order_items
                WHERE order_id = :order_id
                ORDER BY id
                """
            ),
            {"order_id": order.id},
        )
        item_rows = item_result.mappings().all()
        order.items = [
            OrderItem(
                id=item["id"],
                order_id=item["order_id"],
                product_name=item["product_name"],
                price=Decimal(item["price"]),
                quantity=item["quantity"],
            )
            for item in item_rows
        ]

        history_result = await self.session.execute(
            text(
                """
                SELECT id, order_id, status, changed_at
                FROM order_status_history
                WHERE order_id = :order_id
                ORDER BY changed_at ASC
                """
            ),
            {"order_id": order.id},
        )
        history_rows = history_result.mappings().all()
        order.status_history = [
            OrderStatusChange(
                id=entry["id"],
                order_id=entry["order_id"],
                status=OrderStatus(entry["status"]),
                changed_at=entry["changed_at"],
            )
            for entry in history_rows
        ]
        return order
