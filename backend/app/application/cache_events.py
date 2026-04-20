"""Event-driven cache invalidation template for LAB 05."""

from dataclasses import dataclass

from app.application.cache_service import CacheService


@dataclass
class OrderUpdatedEvent:
    """Событие изменения заказа."""

    order_id: str


class CacheInvalidationEventBus:
    """
    Минимальный event bus для LAB 05.

    TODO:
    - реализовать publish/subscribe;
    - на OrderUpdatedEvent инвалидировать:
      - order_card:v1:{order_id}
      - catalog:v1 (если изменение затрагивает агрегаты каталога).
    """
    def __init__(self, cache_service: CacheService):
        self.cache_service = cache_service

    async def publish_order_updated(self, event: OrderUpdatedEvent) -> None:
        await self.cache_service.invalidate_order_card(event.order_id)
        await self.cache_service.invalidate_catalog()
