from classification.lib import Classification, Classified, union


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
    value = Classified(1, classification)

    assert value.value == 1
    assert value.classification == classification

    new_value = value.bind(lambda x: x + 1)
    assert new_value.value == 2
    assert new_value.classification == classification


def test_classified_cast():
    value = Classified.cast(1)
    assert value.value == 1
    assert value.classification == Classification()

    new_value = Classified.cast(value)
    assert new_value == value


def test_classified_eval():
    def func(m: float, x: float, b: int = 0) -> float:
        return m * x + b

    m = 1.5
    x = 2.0
    b = 1

    value = Classified.eval(
        func,
        Classified(m, Classification("A")),
        x,
        b=Classified(b, Classification("B")),
    )

    assert value.value == 4.0
    assert value.classification == Classification("A", "B")
