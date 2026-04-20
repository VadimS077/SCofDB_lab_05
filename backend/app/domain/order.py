"""Доменные сущности заказа."""

import uuid
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
from typing import List

from .exceptions import (
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError,
)


# TODO: Реализовать OrderStatus (str, Enum)
# Значения: CREATED, PAID, CANCELLED, SHIPPED, COMPLETED
class OrderStatus(str, Enum):
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"
    SHIPPED = "shipped"
    COMPLETED = "completed"


# TODO: Реализовать OrderItem (dataclass)
# Поля: product_name, price, quantity, id, order_id
# Свойство: subtotal (price * quantity)
# Валидация: quantity > 0, price >= 0
@dataclass
class OrderItem:
    product_name: str
    price: Decimal
    quantity: int
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    order_id: uuid.UUID | None = None

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise InvalidQuantityError(self.quantity)
        if self.price < Decimal("0"):
            raise InvalidPriceError(self.price)

    @property
    def subtotal(self) -> Decimal:
        return self.price * self.quantity


# TODO: Реализовать OrderStatusChange (dataclass)
# Поля: order_id, status, changed_at, id
@dataclass
class OrderStatusChange:
    order_id: uuid.UUID
    status: OrderStatus
    changed_at: datetime = field(default_factory=datetime.utcnow)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


# TODO: Реализовать Order (dataclass)
# Поля: user_id, id, status, total_amount, created_at, items, status_history
# Методы:
#   - add_item(product_name, price, quantity) -> OrderItem
#   - pay() -> None  [КРИТИЧНО: нельзя оплатить дважды!]
#   - cancel() -> None
#   - ship() -> None
#   - complete() -> None
@dataclass
class Order:
    user_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: OrderStatus = OrderStatus.CREATED
    total_amount: Decimal = Decimal("0")
    created_at: datetime = field(default_factory=datetime.utcnow)
    items: List[OrderItem] = field(default_factory=list)
    status_history: List[OrderStatusChange] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.total_amount < Decimal("0"):
            raise InvalidAmountError(self.total_amount)
        if not self.status_history:
            self.status_history.append(
                OrderStatusChange(order_id=self.id, status=self.status)
            )

    def add_item(self, product_name, price, quantity) -> OrderItem:
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        item = OrderItem(
            product_name=product_name,
            price=Decimal(price),
            quantity=quantity,
            order_id=self.id,
        )
        self.items.append(item)
        self.total_amount = sum((i.subtotal for i in self.items), Decimal("0"))
        return item

    def pay(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        self.status = OrderStatus.PAID
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=OrderStatus.PAID)
        )

    def cancel(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        if self.status == OrderStatus.CANCELLED:
            return
        self.status = OrderStatus.CANCELLED
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=OrderStatus.CANCELLED)
        )

    def ship(self) -> None:
        if self.status != OrderStatus.PAID:
            raise ValueError("Order must be paid before shipping")
        self.status = OrderStatus.SHIPPED
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=OrderStatus.SHIPPED)
        )

    def complete(self) -> None:
        if self.status != OrderStatus.SHIPPED:
            raise ValueError("Order must be shipped before completion")
        self.status = OrderStatus.COMPLETED
        self.status_history.append(
            OrderStatusChange(order_id=self.id, status=OrderStatus.COMPLETED)
        )
