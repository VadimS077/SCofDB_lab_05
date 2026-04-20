"""
Тест для демонстрации РЕШЕНИЯ проблемы race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_safe() заказ оплачивается только один раз.
"""

import asyncio
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.application.payment_service import PaymentService
from app.domain.exceptions import OrderAlreadyPaidError


def _database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@db:5432/marketplace",
    )


@pytest_asyncio.fixture
async def db_session():
    """Создать сессию БД для тестов."""
    url = _database_url()
    if "sqlite" in url:
        pytest.skip(
            "Нужен PostgreSQL (FOR UPDATE / изоляция). Пример: "
            "export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"
        )
    engine = create_async_engine(
        url,
        echo=False,
        pool_size=10,
        max_overflow=5,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def test_order(db_session):
    """Создать тестовый заказ со статусом 'created'."""
    Session = db_session
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()
    email = f"concurrent_safe_{user_id.hex[:8]}@example.com"

    async with Session() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, NOW())
                    """
                ),
                {"id": user_id, "email": email, "name": "Concurrent Safe"},
            )
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

    yield order_id

    async with Session() as session:
        async with session.begin():
            await session.execute(
                text("DELETE FROM users WHERE id = :id"),
                {"id": user_id},
            )


@pytest.mark.asyncio
async def test_concurrent_payment_safe_prevents_race_condition(db_session, test_order):
    """
    Тест демонстрирует решение проблемы race condition с помощью pay_order_safe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен только один раз.
    Это показывает, что метод pay_order_safe() защищен от конкурентных запросов.
    
    TODO: Реализовать тест следующим образом:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_safe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_safe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_safe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить результаты:
       - Одна попытка должна УСПЕШНО завершиться
       - Вторая попытка должна выбросить OrderAlreadyPaidError ИЛИ вернуть ошибку
       
       success_count = sum(1 for r in results if not isinstance(r, Exception))
       error_count = sum(1 for r in results if isinstance(r, Exception))
       
       assert success_count == 1, "Ожидалась одна успешная оплата"
       assert error_count == 1, "Ожидалась одна неудачная попытка"
       
    4. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
       assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
       
    5. Вывести информацию об успешном решении:
       
       print(f"✅ RACE CONDITION PREVENTED!")
       print(f"Order {order_id} was paid only ONCE:")
       print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
       print(f"Second attempt was rejected: {results[1]}")
    """
    Session = db_session
    order_id = test_order

    async def payment_attempt_1():
        async with Session() as session1:
            service1 = PaymentService(session1)
            return await service1.pay_order_safe(order_id)

    async def payment_attempt_2():
        async with Session() as session2:
            service2 = PaymentService(session2)
            return await service2.pay_order_safe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True,
    )

    success_count = sum(1 for r in results if not isinstance(r, Exception))
    error_count = sum(1 for r in results if isinstance(r, Exception))
    assert success_count == 1, "Ожидалась одна успешная оплата"
    assert error_count == 1, "Ожидалась одна неудачная попытка"
    assert isinstance(
        next(r for r in results if isinstance(r, Exception)),
        OrderAlreadyPaidError,
    )

    async with Session() as session:
        service = PaymentService(session)
        history = await service.get_payment_history(order_id)

    assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"

    print("✅ RACE CONDITION PREVENTED!")
    print(f"Order {order_id} was paid only ONCE:")
    print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
    err = next(r for r in results if isinstance(r, Exception))
    print(f"Second attempt was rejected: {err!r}")


@pytest.mark.asyncio
async def test_concurrent_payment_safe_with_explicit_timing(db_session, test_order):
    """Дополнительный тест: проверить работу блокировок с явной задержкой.
    
    TODO: Реализовать тест с добавлением задержки в первой транзакции:
    
    1. Первая транзакция:
       - Начать транзакцию
       - Заблокировать заказ (FOR UPDATE)
       - Добавить задержку (asyncio.sleep(1))
       - Оплатить
       - Commit
       
    2. Вторая транзакция (запустить через 0.1 секунды после первой):
       - Начать транзакцию
       - Попытаться заблокировать заказ (FOR UPDATE)
       - ДОЛЖНА ЖДАТЬ освобождения блокировки от первой транзакции
       - После освобождения - увидеть обновленный статус 'paid'
       - Выбросить OrderAlreadyPaidError
       
    3. Проверить временные метки:
       - Вторая транзакция должна завершиться ПОЗЖЕ первой
       - Разница должна быть >= 1 секунды (время задержки)
       
    Это подтверждает, что FOR UPDATE действительно блокирует строку."""
    Session = db_session
    order_id = test_order

    async def slow_first():
        async with Session() as s:
            return await PaymentService(s).pay_order_safe(
                order_id, _delay_after_lock_sec=1.0
            )

    async def second_after_tick():
        await asyncio.sleep(0.05)
        async with Session() as s:
            return await PaymentService(s).pay_order_safe(order_id)

    results = await asyncio.gather(
        slow_first(),
        second_after_tick(),
        return_exceptions=True,
    )
    assert sum(1 for r in results if not isinstance(r, Exception)) == 1
    assert sum(1 for r in results if isinstance(r, Exception)) == 1

    async with Session() as session:
        history = await PaymentService(session).get_payment_history(order_id)
    assert len(history) == 1



@pytest.mark.asyncio
async def test_concurrent_payment_safe_multiple_orders():
    """
    Дополнительный тест: проверить, что блокировки не мешают разным заказам.
    
    TODO: Реализовать тест:
    1. Создать ДВА разных заказа
    2. Оплатить их ПАРАЛЛЕЛЬНО с помощью pay_order_safe()
    3. Проверить, что ОБА успешно оплачены
    
    Это показывает, что FOR UPDATE блокирует только конкретную строку,
    а не всю таблицу, что важно для производительности.
    """
    # TODO: Реализовать тест с несколькими заказами
    raise NotImplementedError("TODO: Реализовать test_concurrent_payment_safe_multiple_orders")

if __name__ == "__main__":
    """
    Запуск теста:

    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_safe.py -v -s

    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_safe_prevents_race_condition PASSED

    Вывод должен показывать:
    ✅ RACE CONDITION PREVENTED!
    Order XXX was paid only ONCE:
      - 2024-XX-XX: status = paid
    Second attempt was rejected: OrderAlreadyPaidError(...)
    """
    pytest.main([__file__, "-v", "-s"])
