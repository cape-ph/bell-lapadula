from typing import Callable, Iterable, Optional

from classification.classified import Classification


class Select:
    def __init__(
        self,
        table: str,
        columns: Optional[Iterable[str]] = None,
        *,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        self._table = table
        self._columns = ("*",) if columns is None else tuple(columns)
        self._where = where
        self._group_by = group_by
        self._order_by = order_by
        self._limit = limit

    def __str__(self):
        return self.sql

    def _build(
        self,
        table: Optional[str] = None,
        columns: Optional[Iterable[str]] = None,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        if table is None:
            table = self._table
        if columns is None:
            columns = self._columns
        if where is None:
            where = self._where
        if group_by is None:
            group_by = self._group_by
        if order_by is None:
            order_by = self._order_by
        if limit is None:
            limit = self._limit
        return Select(
            table=table,
            columns=columns,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
        )

    def where(self, search_condition: str):
        return self._build(where=search_condition)

    def group_by(self, expression: str):
        return self._build(group_by=expression)

    def order_by(self, expression: str):
        return self._build(order_by=expression)

    def limit(self, number: int):
        return self._build(limit=number)

    @property
    def sql(self) -> str:
        columns = ", ".join(self._columns)
        query = f"SELECT ({columns}) FROM {self._table}"  # noqa: S608

        if self._where:
            query += f" WHERE {self._where}"
        if self._group_by:
            query += f" GROUP BY {self._group_by}"
        if self._order_by:
            query += f" ORDER BY {self._order_by}"
        if self._limit:
            query += f" LIMIT {self._limit}"

        return f"{query};"


class Query:
    @staticmethod
    def select(
        from_table: str, columns: Optional[Iterable[str]] = None
    ) -> Select:
        return Select(from_table, columns)
