from typing import Any, Iterable, Mapping, Optional


def select(
    table: str,
    columns: Optional[Iterable[str]] = None,
) -> "SelectStatement":
    """Build a SELECT statement

    Args:
        table (str): the SQL table
        columns (Optional[Iterable[str]]): the columns to select.  If not
            provided defaults to the wildcard "*".

    Returns:
        SelectStatement: a SelectStatement
    """
    return SelectStatement(table, columns)


def insert(
    table: str,
    values: Mapping[str, Any],
) -> "InsertStatement":
    """Build an INSERT statement

    Args:
        table (str): the SQL table
        values (Mapping[str, Any]): mapping of data to insert.  The keys must
            be the column names and the values are the values to insert.

    Returns:
        InsertStatement: an InsertStatement
    """
    return InsertStatement(table, values)


class SelectStatement:
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

    @property
    def table(self) -> str:
        return self._table

    @property
    def columns(self) -> tuple[str, ...]:
        return self._columns

    def _build(
        self,
        table: Optional[str] = None,
        columns: Optional[Iterable[str]] = None,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "SelectStatement":
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
        return SelectStatement(
            table=table,
            columns=columns,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
        )

    def where(self, search_condition: str) -> "SelectStatement":
        """Add a where clause

        Args:
            search_condition (str): the search condition

        Returns:
            SelectStatement: a new statement with the added clause
        """
        return self._build(where=search_condition)

    def group_by(self, expression: str) -> "SelectStatement":
        """Add a group by clause

        Args:
            expression (str): the group by expression

        Returns:
            SelectStatement: a new statement with the added clause
        """
        return self._build(group_by=expression)

    def order_by(self, expression: str) -> "SelectStatement":
        """Add an order by clause

        Args:
            expression (str): the order by expression

        Returns:
            SelectStatement: a new statement with the added clause
        """
        return self._build(order_by=expression)

    def limit(self, number: int) -> "SelectStatement":
        """Add a limit clause

        Args:
            number (int): the number of rows to read

        Returns:
            SelectStatement: a new statement with the added clause
        """
        return self._build(limit=number)

    @property
    def sql(self) -> str:
        """Get the SQL representation of the statement

        Returns:
            str: SQL code
        """
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


class InsertStatement:
    def __init__(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        or_: Optional[str] = None,
    ):
        self._table = table
        self._values = values
        self._or = or_

    def __str__(self):
        return self.sql

    @property
    def table(self) -> str:
        return self._table

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self._values.keys())

    def _build(
        self,
        table: Optional[str] = None,
        values: Optional[Mapping[str, Any]] = None,
        or_: Optional[str] = None,
    ) -> "InsertStatement":
        if table is None:
            table = self._table
        if values is None:
            values = self._values
        if or_ is None:
            or_ = self._or
        return InsertStatement(
            table=table,
            values=values,
            or_=or_,
        )

    def or_abort(self) -> "InsertStatement":
        return self._build(or_="ABORT")

    def or_fail(self) -> "InsertStatement":
        return self._build(or_="FAIL")

    def or_ignore(self) -> "InsertStatement":
        return self._build(or_="IGNORE")

    def or_replace(self) -> "InsertStatement":
        return self._build(or_="REPLACE")

    def or_rollback(self) -> "InsertStatement":
        return self._build(or_="ROLLBACK")

    @property
    def sql(self) -> str:
        columns = ", ".join(self.columns)
        placeholders = ", ".join(["?" for _ in range(len(self._values))])
        if self._or is not None:
            query = f"INSERT {self._or}"
        else:
            query = "INSERT"
        query += f" INTO {self._table} ({columns})"
        query += f" VALUES ({placeholders});"
        return query
