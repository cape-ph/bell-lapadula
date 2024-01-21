import pytest
from classification.classified import (
    Classification,
    Classified,
    union,
)
from classification.maybe import ExpectationError, Maybe


def test_classification():
    A = Classification("A")
    B = Classification("B")
    AB = Classification("A", "B")

    assert A == Classification("A")
    assert A != B

    # Set comparisons
    assert not (A < Classification("A"))
    assert A <= Classification("A")
    assert not (A > Classification("A"))
    assert A >= Classification("A")

    assert A < AB
    assert B < AB
    assert AB > A
    assert AB > B
    assert A <= AB
    assert B <= AB
    assert AB >= A
    assert AB >= B


def test_classification_union():
    classification = union(Classification(c) for c in "ABCDE")
    assert classification == Classification("A", "B", "C", "D", "E")


def test_classified():
    classification = Classification("PHI")
    value = Classified.just(1, classification)

    assert value.expect() == 1
    assert value.classification == classification

    new_value = value.map(lambda x: x + 1)
    assert new_value.expect() == 2
    assert new_value.classification == classification


def test_classified_coerce():
    value = Classified.coerce(1)
    assert value.expect() == 1
    assert value.classification == Classification()

    new_value = Classified.coerce(value)
    assert new_value == value


def test_classified_map_many():
    def func(m: float, x: float, b: int = 0) -> float:
        return m * x + b

    m = 1.5
    x = 2.0
    b = 1

    value = Classified.map_many(
        func,
        Classified(m, Classification("A")),
        x,
        b=Classified(b, Classification("B")),
    )

    assert value.expect() == 4.0
    assert value.classification == Classification("A", "B")


def test_classified_map_expect():
    res = (
        Classified.just(1, ("A", "B")).map(lambda x: x + 1).map(lambda x: 2 * x)
    )

    assert res.expect() == 4
    assert res.classification == Classification("A", "B")

    with pytest.raises(ZeroDivisionError):
        Classified(1, Classification("A", "B")).map(lambda x: x / 0).expect()
    with pytest.raises(ValueError):
        Classified(1, Classification("A", "B")).map(lambda x: x / 0).expect(
            ValueError()
        )
    with pytest.raises(ExpectationError):
        Classified(1, Classification("A", "B")).map(lambda x: x / 0).expect(
            "cant do that"
        )
