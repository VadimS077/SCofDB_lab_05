"""
Тест для демонстрации ПРОБЛЕМЫ race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_unsafe() возникает двойная оплата.
"""

import asyncio
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.application.payment_service import PaymentService


def _database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@db:5432/marketplace",
    )


@pytest_asyncio.fixture
async def db_session():
    """
    Сессия БД для тестов.
    TODO: Реализовать фикстуру:
    1. Создать engine
    2. Создать session maker
    3. Открыть сессию
    4. Yield сессию
    5. Закрыть сессию после теста
    """
    engine = create_async_engine(
        _database_url(),
        echo=False,
        pool_size=10,
        max_overflow=5,
    )
    async_session = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session() as session:
        yield session
    
    await engine.dispose()


@pytest_asyncio.fixture
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    
    TODO: Реализовать фикстуру:
    1. Создать тестового пользователя
    2. Создать тестовый заказ со статусом 'created'
    3. Записать начальный статус в историю
    4. Вернуть order_id
    5. После теста - очистить данные
    """
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()
    email = f"unsafe_{user_id.hex[:10]}@example.com"

    await db_session.execute(
        text(
            """
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, NOW())
            """
        ),
        {"id": user_id, "email": email, "name": "Concurrent Unsafe"},
    )
    await db_session.execute(
        text(
            """
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, 'created', 0, NOW())
            """
        ),
        {"id": order_id, "user_id": user_id},
    )
    await db_session.execute(
        text(
            """
            INSERT INTO order_status_history (id, order_id, status, changed_at)
            VALUES (:id, :order_id, 'created', NOW())
            """
        ),
        {"id": uuid.uuid4(), "order_id": order_id},
    )
    await db_session.commit()

    yield order_id

    await db_session.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})
    await db_session.commit()


@pytest.mark.asyncio
async def test_concurrent_payment_unsafe_demonstrates_race_condition(db_session, test_order):
    """
    Тест демонстрирует проблему race condition при использовании pay_order_unsafe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен дважды.
    Это показывает, что метод pay_order_unsafe() НЕ защищен от конкурентных запросов.
    
    TODO: Реализовать тест следующим образом:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_unsafe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_unsafe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_unsafe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ДВЕ ЗАПИСИ 'paid' - это и есть проблема!
       assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"
       
    4. Вывести информацию о проблеме:
       
       print(f"⚠️ RACE CONDITION DETECTED!")
       print(f"Order {order_id} was paid TWICE:")
       for record in history:
           print(f"  - {record['changed_at']}: status = {record['status']}")
    """
    # TODO: Реализовать тест, демонстрирующий race condition
    order_id = test_order

    engine = create_async_engine(
        _database_url(),
        echo=False,
        pool_size=10,
        max_overflow=5,
    )
    async_session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session_maker() as session1, async_session_maker() as session2:

        async def payment_attempt_1():
            service1 = PaymentService(session1)
            return await service1.pay_order_unsafe(order_id)

        async def payment_attempt_2():
            service2 = PaymentService(session2)
            return await service2.pay_order_unsafe(order_id)

        await asyncio.gather(
            payment_attempt_1(),
            payment_attempt_2(),
            return_exceptions=True,
        )

    await engine.dispose()

    service = PaymentService(db_session)
    history = await service.get_payment_history(order_id)

    assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"

    print(f"Order {order_id} was paid TWICE:")
    for record in history:
        print(f"  - {record['changed_at']}: status = {record['status']}")


@pytest.mark.asyncio
async def test_concurrent_payment_unsafe_both_succeed():
    """
    Дополнительный тест: проверить, что ОБЕ транзакции успешно завершились.
    
    TODO: Реализовать проверку, что:
    1. Обе попытки оплаты вернули успешный результат
    2. Ни одна не выбросила исключение
    3. Обе записали в историю
    
    Это подтверждает, что проблема не в ошибках, а в race condition.
    """
    # TODO: Реализовать проверку успешности обеих транзакций
    pass


if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_unsafe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_unsafe_demonstrates_race_condition PASSED
    
    Вывод должен показывать:
    ⚠️ RACE CONDITION DETECTED!
    Order XXX was paid TWICE:
      - 2024-XX-XX: status = paid
      - 2024-XX-XX: status = paid
    """
    pytest.main([__file__, "-v", "-s"])