from typing import Protocol, runtime_checkable


@runtime_checkable
class Labelable(Protocol):
    """
    Protocol for objects that can have a human-readable label.

    Labels provide meaningful names for objects in the computational graph,
    aiding in debugging, visualization, and monitoring. They serve as
    human-friendly identifiers that complement the technical identifiers
    used internally.

    Labels are optional but highly recommended for:
    - Debugging complex computational graphs
    - Visualization and monitoring tools
    - Error messages and logging
    - User interfaces and dashboards

    """

    @property
    def label(self) -> str:
        """
        Return the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Returns:
            str: Human-readable label for this object
            None: No label is set (will use default naming)
        """
        ...

    @label.setter
    def label(self, label: str | None) -> None:
        """
        Set the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Args:
            value (str): Human-readable label for this object
        """
        ...
