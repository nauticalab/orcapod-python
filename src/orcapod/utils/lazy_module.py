import importlib
from types import ModuleType
from typing import Any, Optional


class LazyModule:
    """
    A wrapper that lazily loads a module only when its attributes are first accessed.

    Example:
        # Instead of: import expensive_module
        expensive_module = LazyModule('expensive_module')

        # Module is only loaded when you access something:
        result = expensive_module.some_function()  # Now it imports
    """

    def __init__(self, module_name: str, package: str | None = None):
        """
        Initialize lazy module loader.

        Args:
            module_name: Name of the module to import
            package: Package for relative imports (same as importlib.import_module)
        """
        self._module_name = module_name
        self._package = package
        self._module: ModuleType | None = None
        self._loaded = False

    def _load_module(self) -> ModuleType:
        """Load the module if not already loaded."""
        if not self._loaded:
            self._module = importlib.import_module(self._module_name, self._package)
            self._loaded = True
        assert self._module is not None, (
            f"Module '{self._module_name}' could not be loaded. "
            "This should not happen if the module exists."
        )
        return self._module

    def __getattr__(self, name: str) -> Any:
        """Get attribute from the wrapped module, loading it if necessary."""
        if name.startswith("_"):
            #!? Side effect: this also blocks lazily accessing legitimate module dunders like
            #!? `pa.__version__` / `mod.__all__` (they raise AttributeError instead of loading).
            #!? Fine for the current pyarrow/polars/git usage, but a lurking gotcha. Consider
            #!? special-casing known module dunders, or documenting the limitation.
            # Avoid infinite recursion for internal attributes
            #! Yes this should be checked/revisited for an improved logic
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

        module = self._load_module()
        return getattr(module, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute on the wrapped module or on this instance."""
        if name.startswith("_") or not self._loaded:
            # Set on this instance for internal attributes or before loading
            super().__setattr__(name, value)
        else:
            # Set on the wrapped module
            setattr(self._load_module(), name, value)

    def __delattr__(self, name: str) -> None:
        """Delete attribute from the wrapped module."""
        if name.startswith("_"):
            super().__delattr__(name)
        else:
            delattr(self._load_module(), name)

    def __dir__(self) -> list[str]:
        """Return directory of the wrapped module."""
        if self._loaded:
            return dir(self._module)
        else:
            # Return empty list or basic attributes before loading
            return []

    def __repr__(self) -> str:
        """String representation."""
        if self._loaded:
            return f"<LazyModule '{self._module_name}' (loaded): {repr(self._module)}>"
        else:
            return f"<LazyModule '{self._module_name}' (not loaded)>"

    def __str__(self) -> str:
        """String representation."""
        return self.__repr__()

    # Support for callable modules (modules with __call__)
    def __call__(self, *args, **kwargs):
        """Call the module if it's callable."""
        module = self._load_module()
        return module(*args, **kwargs)  # type: ignore

    # Support for iteration if the module is iterable
    def __iter__(self):
        """Iterate over the module if it's iterable."""
        module = self._load_module()
        return iter(module)  # type: ignore

    def __len__(self):
        """Get length of the module if it supports len()."""
        module = self._load_module()
        return len(module)  # type: ignore

    def __getitem__(self, key):
        """Get item from the module if it supports indexing."""
        module = self._load_module()
        return module[key]  # type: ignore

    def __setitem__(self, key, value):
        """Set item on the module if it supports item assignment."""
        module = self._load_module()
        module[key] = value  # type: ignore

    def __contains__(self, item):
        """Check if item is in the module if it supports 'in' operator."""
        module = self._load_module()
        return item in module

    @property
    def is_loaded(self) -> bool:
        """Check if the module has been loaded."""
        return self._loaded

    @property
    def module_name(self) -> str:
        """Get the module name."""
        return self._module_name

    def force_load(self) -> ModuleType:
        """Force load the module and return it."""
        return self._load_module()


# Convenience function for creating lazy modules
def lazy_import(module_name: str, package: Optional[str] = None) -> LazyModule:
    """
    Create a lazy module loader.

    Args:
        module_name: Name of the module to import
        package: Package for relative imports

    Returns:
        LazyModule instance that will load the module on first access

    Example:
        np = lazy_import('numpy')
        pd = lazy_import('pandas')

        # Modules are only imported when you use them:
        array = np.array([1, 2, 3])  # numpy imported here
        df = pd.DataFrame({'a': [1, 2]})  # pandas imported here
    """
    return LazyModule(module_name, package)
