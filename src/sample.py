from collections.abc import Mapping


def test() -> Mapping[str, type] | int: ...


x = test()
