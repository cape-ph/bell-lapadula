from typing import Callable, Generic, Optional, TypeVar, cast

T = TypeVar("T")
U = TypeVar("U")


class ExpectationError(ValueError):
    """Raised when an expectation is violated."""

    pass


class Maybe(Generic[T]):
    """The Maybe monad, used to represent values that may be undefined.

    Note:
        The Maybe monad is an alternative way to implement error handling for
        sequences of operations that may fail.  Javascript, Rust, Haskell, and
        other functional languages all have a Maybe type which serves this
        purpose.

    Example:
        >>> def divide(num: float, denom: float) -> Maybe[float]:
        >>>     if denom == 0:
        >>>         return Maybe.nothing(ZeroDivisionError())
        >>>     return Maybe.just(num / denom)
        >>>
        >>> # use the function
        >>> assert divide(4, 2).expect() == 2.0
        >>>
        >>> # chain computations
        >>> assert divide(4, 2).map(lambda x: 3 * x).expect() == 6.0
        >>>
        >>> # add some error handling
        >>> result = (
        >>>     divide(2, 0)
        >>>     .if_error(lambda err: math.inf)
        >>>     .expect("Division failed")
        >>> )
    """

    def __init__(self, value: T, error: Optional[Exception] = None) -> None:
        self._value = value
        self._error = error

    def __repr__(self) -> str:
        if self._value is None:
            return "Maybe.nothing"
        return f"Maybe.just({self._value})"

    @staticmethod
    def just(value: T) -> "Maybe[T]":
        """Create a Maybe that stores a valid value.

        Args:
            value (T): the value to store

        Returns:
            Maybe[T]: the wrapped value
        """
        return Maybe(value)

    @staticmethod
    def nothing(error: Optional[str | Exception] = None) -> "Maybe[T]":
        """Create a Maybe that stores nothing (invalid value).

        Args:
            error (Optional[str  |  Exception]): the error to set. Defaults to
                an ExpectationError().

        Returns:
            Maybe[T]: the nothing value
        """
        if isinstance(error, str):
            error = ExpectationError(error)
        elif error is None:
            error = ExpectationError()
        return cast(Maybe[T], Maybe(None, error))

    def is_valid(self) -> bool:
        return self._error is None

    def map(self, func: Callable[[T], U]) -> "Maybe[U]":
        """Map the value using a function

        Args:
            func (Callable[[T], U]): the function to map the value

        Returns:
            Maybe[U]: the maybe storing the mapped value or error
        """
        if not self.is_valid():
            res = Maybe.nothing(self._error)
        else:
            try:
                res = Maybe.just(func(self._value))
            except Exception as e:
                res = Maybe.nothing(e)
        return cast(Maybe[U], res)

    def apply(self, func: Callable[[T], "Maybe[U]"]) -> "Maybe[U]":
        """Map the value using a monadic function

        Args:
            func (Callable[[T], Maybe[U]]): the monadic function used to map
                the value.  A monadic function returns another Maybe.

        Returns:
            Maybe[U]: the maybe storing the mapped value or error
        """
        if not self.is_valid():
            return cast(Maybe[U], Maybe.nothing(self._error))
        return func(self._value)

    def if_error(self, func: Callable[[Exception], "Maybe[T]"]) -> "Maybe[T]":
        """If an error has occurred, manage it with a callback function

        Args:
            func (Callable[[Exception], Maybe[T]]): the callback function
                used to manage the error.  If the callback itself raises
                an exception, this exception immediately is passed to
                the Python exception system.

        Returns:
            Maybe[T]: the value after recovering from the error
        """
        if self.is_valid():
            return self
        return func(self._error)

    def expect(self, msg: Optional[str | Exception] = None) -> T:
        """Get the stored value or raise an exception if an error has occurred

        Args:
            msg (Optional[str | Exception]): exception to raise if the value
                is None.  The argument may be either a string (which is
                coerced into a ExpectationError) or an Exception.  If provided
                the exception replaces any exception already stored on the
                Maybe object.  If not provided, this method uses the exception
                stored on the Maybe object if it exists or an ExpectationError.

        Raises:
            ExpectationError: raised if the expectation is violated
            Exception: raised if a prior step set the exception

        Returns:
            T: the resolved value
        """
        if not self.is_valid():
            if isinstance(msg, Exception):
                raise msg
            elif isinstance(msg, str):
                raise ExpectationError(msg)
            elif self._error is not None:
                raise self._error
            else:
                raise ExpectationError()
        return self._value
