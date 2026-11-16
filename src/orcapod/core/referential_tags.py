"""The ``tag_only_columns`` option of a source (ITL-1159).

Referential tags arrive in Orcapod v0.4: a declared source tag will then be referential by
default (a handle to the data column of the same name), and ``tag_only_columns`` names the
tags kept plain. In v0.3 every tag is plain. Every source that declares tag columns already
accepts the option, with exactly one value, ``True`` (all tags plain), which is also the
default, so code and tests written for v0.3 can state the plain behaviour explicitly and
keep it when the default changes. Any other value would make a tag referential and is
refused.
"""

from __future__ import annotations

from collections.abc import Iterable

TagOnlyColumns = str | Iterable[str] | bool
"""The ``tag_only_columns`` option of a source: names, or ``True`` for every tag."""


def check_tag_only_columns(tag_only_columns: TagOnlyColumns) -> None:
    """Refuse a ``tag_only_columns`` value that would make a tag referential.

    ``tag_only_columns=True`` keeps every declared tag plain (tag only, not data), which is
    what every source does in v0.3; it is also the default. Any other value (``False``,
    names, an empty collection, even a collection listing every tag) is refused:
    referential tags arrive in v0.4.

    Args:
        tag_only_columns: The tags kept plain; only ``True`` (all of them) is accepted.

    Raises:
        ValueError: If ``tag_only_columns`` is anything but ``True``.
    """
    if tag_only_columns is not True:
        raise ValueError(
            f"tag_only_columns={tag_only_columns!r}: every tag is plain in Orcapod v0.3, "
            "so only True (the default) is accepted. Referential tags (tag_only_columns "
            "naming some of the tags, an empty collection, or False) arrive in v0.4."
        )
