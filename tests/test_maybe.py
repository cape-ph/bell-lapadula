import math
from functools import partial

import pytest
from classification.maybe import ExpectationError, Maybe


def divide(a: float, b: float) -> Maybe[float]:
    if b == 0:
        return Maybe.nothing(ZeroDivisionError())
    return Maybe.just(a / b)


def test_maybe():
    m = divide(1, 2).expect("Failed to divide.")
    assert m == 1 / 2


def test_maybe_expect():
    with pytest.raises(ExpectationError):
        divide(1, 0).expect("Failed to divide.")
    with pytest.raises(ZeroDivisionError):
        divide(1, 0).expect()
    with pytest.raises(ValueError):
        divide(1, 0).expect(ValueError())


def test_maybe_map():
    assert Maybe.just(1).map(lambda x: x * 2).expect() == 2


def test_maybe_apply():
    assert Maybe.just(1).apply(partial(divide, b=2)).expect() == 1 / 2


def test_maybe_if_error():
    def handle_zero_division(err: Exception) -> Maybe[float]:
        if isinstance(err, ZeroDivisionError):
            return Maybe.just(math.inf)
        return Maybe.nothing(err)

    result = divide(1, 0).if_error(handle_zero_division).expect()
    assert result == math.inf

    result = divide(1, 1).if_error(handle_zero_division).expect()
    assert result == 1.0
