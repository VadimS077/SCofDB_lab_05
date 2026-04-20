"""Сервис для работы с пользователями."""

import uuid
from typing import Optional, List

from app.domain.user import User
from app.domain.exceptions import EmailAlreadyExistsError, UserNotFoundError


class UserService:
    """Сервис для операций с пользователями."""

    def __init__(self, repo):
        self.repo = repo

    # TODO: Реализовать register(email, name) -> User
    # 1. Проверить что email не занят
    # 2. Создать User
    # 3. Сохранить через repo.save()
    async def register(self, email: str, name: str = "") -> User:
        existing = await self.repo.find_by_email(email)
        if existing is not None:
            raise EmailAlreadyExistsError(email)
        user = User(email=email, name=name)
        await self.repo.save(user)
        return user

    # TODO: Реализовать get_by_id(user_id) -> User
    async def get_by_id(self, user_id: uuid.UUID) -> User:
        user = await self.repo.find_by_id(user_id)
        if user is None:
            raise UserNotFoundError(user_id)
        return user

    # TODO: Реализовать get_by_email(email) -> Optional[User]
    async def get_by_email(self, email: str) -> Optional[User]:
        return await self.repo.find_by_email(email)

    # TODO: Реализовать list_users() -> List[User]
    async def list_users(self) -> List[User]:
        return await self.repo.find_all()
