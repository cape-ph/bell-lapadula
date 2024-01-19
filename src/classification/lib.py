from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterable, TypeVar, Union


class Classification:
    """A type for a classification marking."""

    def __init__(self, *categories: str):
        """Create a classification marking

        Args:
            categories (str): the classification categories
        """
        self.categories = frozenset(categories)

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


T = TypeVar("T")
U = TypeVar("U")


class Classified(Generic[T]):
    """A monadic type to store values with a classification marking"""

    def __init__(
        self, value: T, classification: Classification | Iterable[str]
    ):
        """Create a new classified value

        Args:
            value (T): the value that is classified
            classification (Classification | Iterable[str]): the classification
                of the value
        """
        self._value = value
        self._classification = (
            classification
            if isinstance(classification, Classification)
            else Classification(*classification)
        )

    def __repr__(self) -> str:
        return f"{self.classification} {self.value}"

    @property
    def value(self) -> T:
        """Get the value stored in the monad

        Returns:
            T: the stored value
        """
        return self._value

    @property
    def classification(self) -> Classification:
        """Get the classification assigned to the value

        Returns:
            Classification: the classification
        """
        return self._classification

    def bind(self, func: Callable[[T], U]) -> "Classified[U]":
        """Apply `func` to the value, returning a new classified value

        Note:
            The bind method implements the monad pattern by applying
            `func` to the enclosed value and returning a new monad storing
            the result of the function application.  In addition, the new
            monad has the same classification marking as the current object.
            This ensures that classification markings propagate correctly
            through the computation.

        Example:
            >>> v = Classified(1, Classification("PHI"))
            >>> w = v.bind(lambda x: x + 1)
            >>> assert w.value == 2
            >>> assert w.classification == Classification("PHI")

        Args:
            func (Callable[[T], U]): function to apply

        Returns:
            Classified[U]: the new classified value storing the result of the
                function application.
        """
        value = func(self.value)
        return Classified(value, self.classification)

    @staticmethod
    def cast(value: "Classified[T]" | T) -> "Classified[T]":
        """Cast the value into a classified type

        Note:
            If the argument is not already a classified type, this method
            returns a new Classified monad with an empty (unclassified)
            classification tag.

        Args:
            value (Classified[T] | T): the value to cast

        Returns:
            Classified[T]: the value cast into a Classified monad
        """
        if isinstance(value, Classified):
            return value
        return Classified(value, Classification())

    @staticmethod
    def eval(  # noqa: A003
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
            args=tuple(Classified.cast(a) for a in args),
            kwargs={k: Classified.cast(v) for k, v in kwargs.items()},
        )
        classification = union(
            (
                union(a.classification for a in arguments.args),
                union(a.classification for a in arguments.kwargs.values()),
            )
        )

        def method(args: ClassifiedArguments) -> U:
            uargs = tuple(a.value for a in args.args)
            ukwargs = {k: a.value for k, a in args.kwargs.items()}
            return func(*uargs, **ukwargs)

        return Classified(arguments, classification).bind(method)


@dataclass
class ClassifiedArguments:
    args: tuple[Classified[Any], ...]
    kwargs: dict[str, Classified[Any]]
