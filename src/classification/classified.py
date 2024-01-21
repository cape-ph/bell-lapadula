from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterable, Optional, Union, cast

from classification.maybe import MISSING, ExpectationError, Maybe, Missing, T, U


class Classification:
    """A type for a classification marking."""

    def __init__(self, *categories: str):
        """Create a classification marking

        Args:
            categories (str): the classification categories
        """
        self.categories = frozenset(categories)

    @staticmethod
    def coerce(
        value: "Classification" | Iterable[str] | None,
    ) -> "Classification":
        """Cast the value into a classification

        Args:
            value (Classification | Iterable[str] | None): the classification
                labels.  If None, default to the empty (e.g. unclassified)
                label set.

        Returns:
            Classification: the classification
        """
        if isinstance(value, Classification):
            return value
        elif value is None:
            return Classification()
        else:
            return Classification(*value)

    def __repr__(self) -> str:
        return "(" + "//".join(sorted(self.categories)) + ")"

    def __eq__(self, other: object) -> bool:
        if not hasattr(other, "categories"):
            return False
        return self.categories == other.categories

    def __lt__(self, other: "Classification") -> bool:
        return self.categories < other.categories

    def __le__(self, other: "Classification") -> bool:
        return self.categories <= other.categories

    def __gt__(self, other: "Classification") -> bool:
        return self.categories > other.categories

    def __ge__(self, other: "Classification") -> bool:
        return self.categories >= other.categories


class ClassificationViolationError(ValueError):
    """Raised when a classification rule is violated"""

    pass


def union(classifications: Iterable[Classification]) -> Classification:
    """Get the union of a sequence of classifications

    Args:
        classifications (Iterable[Classification]): the sequence of
            classifications

    Returns:
        Classification: the union classification.  This is guaranteed to be an
            equal or higher classification level than all of the input
            classifications.
    """

    def iter_categories():
        for c in classifications:
            yield from c.categories

    categories = tuple(iter_categories())
    return Classification(*categories)


# Note:
#
# We are intentionally re-implementing the same interface as the Maybe class,
# but not using class inheritance.  The added "classification" arguments to
# some of these methods would violate the Liskov substitution principle if we
# used direct inheritance.
#
# We are also not using a Protocol because currently the Self type does not
# support generics (required to type .map correctly).
#


class Classified(Generic[T]):
    """A monad type to store values with a classification marking"""

    def __init__(
        self,
        value: T,
        classification: Classification | Iterable[str],
        error: Optional[Exception] = None,
    ):
        """Create a new classified value

        Args:
            value (T): the value that is classified
            classification (Classification | Iterable[str]): the classification
                of the value
        """
        self._value = value
        self._error = error
        self._classification = Classification.coerce(classification)

    @property
    def value(self) -> T | Missing:
        return self._value

    @property
    def error(self) -> Exception | None:
        return self._error

    def __repr__(self) -> str:
        part = (
            "Classified.nothing"
            if self.value is MISSING
            else f"Classified.just({self.value})"
        )
        return f"{self.classification} {part}"

    @staticmethod
    def just(
        value: T,
        classification: Optional[Classification | Iterable[str]] = None,
    ) -> "Classified[T]":
        """Create a Maybe that stores a valid value.

        Args:
            value (T): the value to store
            classification (Optional[Classification | Iterable[str]]): the
                classification of the value.

        Returns:
            Maybe[T]: the wrapped value
        """
        clss = Classification.coerce(classification)
        return Classified(value, clss)

    @staticmethod
    def nothing(
        error: Optional[str | Exception] = None,
        classification: Optional[Classification | Iterable[str]] = None,
    ) -> "Classified[T]":
        """Create a Maybe that stores nothing (invalid value).

        Args:
            error (Optional[str | Exception]): the error to set. Defaults to
                an ExpectationError().
            classification (Optional[Classification | Iterable[str]]): the
                classification of the value.

        Returns:
            Maybe[T]: the nothing value
        """
        if isinstance(error, str):
            err: Exception = ExpectationError(error)
        elif error is None:
            err = ExpectationError()
        else:
            err = error
        clss = Classification.coerce(classification)
        return cast(Classified[T], Classified(MISSING, clss, err))

    @property
    def classification(self) -> Classification:
        """Get the classification assigned to the value

        Returns:
            Classification: the classification
        """
        return self._classification

    def is_valid(self) -> bool:
        return self.error is None

    @staticmethod
    def coerce(
        value: "Classified[T]" | Maybe[T] | T,
        classification: Optional[Classification | Iterable[str]] = None,
    ) -> "Classified[T]":
        """Cast the value into a classified type

        Note:
            If the argument is not already a classified type, this method
            returns a new Classified monad.

        Args:
            value (Classified[T] | T): the value to cast
            classification (Optional[Classification | Iterable[str]]): the
                classification to apply. If the value is already classified,
                it's classification level is not modified.  Defaults to the
                empty classification level (i.e. unclassified).

        Returns:
            Classified[T]: the value cast into a Classified monad
        """
        clss = Classification.coerce(classification)
        if isinstance(value, Classified):
            return value
        if isinstance(value, Maybe):
            if value.is_valid():
                if isinstance(value.value, Missing):
                    return Classified.nothing(None, clss)
                return Classified.just(value.value, clss)
            else:
                return Classified.nothing(value.error, clss)
        return Classified.just(value, clss)

    def map(self, func: Callable[[T], U]) -> "Classified[U]":
        if not self.is_valid():
            res: Classified[T] = Classified.nothing(
                self.error, self.classification
            )
        else:
            try:
                res = Classified.just(func(self.value), self.classification)  # type: ignore
            except Exception as e:
                res = Classified.nothing(e, self.classification)
        return cast(Classified[U], res)

    @staticmethod
    def map_many(
        func: Callable[[Any], U],
        *args: Union["Classified[Any]", Any],
        **kwargs: Union["Classified[Any]", Any],
    ) -> "Classified[U]":
        """Evaluate `func` on arguments while propagating classification

        Args:
            func (Callable[[Any], U]): the function to evaluate
            *args (Classified[Any] | Any): the arguments
            **kwargs (Classified[Any] | Any): the keyword arguments

        Returns:
            Classified[U]: the new Classified value storing the result of the
                function application.  The classification of the value is the
                union of all of the classifications of the inputs.
        """
        arguments = ClassifiedArguments(
            args=tuple(Classified.coerce(a) for a in args),
            kwargs={k: Classified.coerce(v) for k, v in kwargs.items()},
        )
        classification = union(
            (
                union(a.classification for a in arguments.args),
                union(a.classification for a in arguments.kwargs.values()),
            )
        )

        def method(args: ClassifiedArguments) -> U:
            uargs = tuple(a.expect() for a in args.args)
            ukwargs = {k: a.expect() for k, a in args.kwargs.items()}
            return func(*uargs, **ukwargs)

        return Classified.just(arguments, classification).map(method)

    def apply(self, func: Callable[[T], "Maybe[U]"]) -> "Classified[U]":
        """Map the value using a monadic function

        Args:
            func (Callable[[T], Maybe[U]]): the monadic function used to
                map the value.  A monadic function returns another Maybe.

        Returns:
            Classified[U]: the classified storing the mapped value or error
        """

        def wrap(m: Maybe[U]) -> Classified[U]:
            if m.is_valid():
                return Classified.just(m.expect(), self.classification)
            else:
                return Classified.nothing(m.error, self.classification)

        if not self.is_valid():
            return cast(
                Classified[U],
                Classified.nothing(self._error, self.classification),
            )
        else:
            return wrap(func(self.expect()))

    def if_error(
        self, func: Callable[[Exception], "Maybe[T]"]
    ) -> "Classified[T]":
        """If an error has occurred, manage it with a callback function

        Args:
            func (Callable[[Exception], Maybe[T]]): the callback function
                used to manage the error.  If the callback itself raises
                an exception, this exception immediately is passed to
                the Python exception system.

        Returns:
            Classified[T]: the value after recovering from the error
        """
        if self.is_valid():
            return self
        if self.error is None:
            maybe = func(ExpectationError())
        else:
            maybe = func(self.error)
        return Classified.coerce(maybe, self.classification)

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
            elif self.error is not None:
                raise self.error
            else:
                raise ExpectationError()
        if isinstance(self.value, Missing):
            raise ExpectationError()
        return self.value


@dataclass
class ClassifiedArguments:
    args: tuple[Classified[Any], ...]
    kwargs: dict[str, Classified[Any]]
