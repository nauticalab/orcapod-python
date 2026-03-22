from orcapod.semantic_types.semantic_struct_converters import (
    SemanticStructConverterBase,
)


class DummyConverter(SemanticStructConverterBase):
    def __init__(self):
        super().__init__("dummy")
        self._python_type = dict
        self._arrow_struct_type = "dummy_struct"

    @property
    def python_type(self):
        return self._python_type

    @property
    def arrow_struct_type(self):
        return self._arrow_struct_type

    def python_to_struct_dict(self, value):
        return value

    def struct_dict_to_python(self, struct_dict):
        return struct_dict

    def can_handle_python_type(self, python_type):
        return python_type is dict

    def can_handle_struct_type(self, struct_type):
        return struct_type == "dummy_struct"

    def is_semantic_struct(self, struct_dict):
        return isinstance(struct_dict, dict)

    def hash_struct_dict(self, struct_dict, add_prefix=False):
        return "dummyhash"


# --- SemanticStructConverterBase tests ---
def test_semantic_struct_converter_base_properties():
    converter = DummyConverter()
    assert converter.semantic_type_name == "dummy"
    assert converter.hasher_id == "dummy_content_sha256"


def test_format_hash_string():
    converter = DummyConverter()
    hash_bytes = b"\x01\x02"
    assert converter._format_hash_string(hash_bytes, add_prefix=False) == "0102"
    assert (
        converter._format_hash_string(hash_bytes, add_prefix=True)
        == "dummy:sha256:0102"
    )


def test_compute_content_hash():
    converter = DummyConverter()
    data = b"abc"
    result = converter._compute_content_hash(data)
    import hashlib

    assert result.digest == hashlib.sha256(data).digest()


# --- PythonPathStructConverter tests ---


def test_extensibility_with_new_converter():
    class NewConverter(SemanticStructConverterBase):
        def __init__(self):
            super().__init__("newtype")
            self._python_type = list
            self._arrow_struct_type = "new_struct"

        @property
        def python_type(self):
            return self._python_type

        @property
        def arrow_struct_type(self):
            return self._arrow_struct_type

        def python_to_struct_dict(self, value):
            return {"data": value}

        def struct_dict_to_python(self, struct_dict):
            return struct_dict["data"]

        def can_handle_python_type(self, python_type):
            return python_type is list

        def can_handle_struct_type(self, struct_type):
            return struct_type == "new_struct"

        def is_semantic_struct(self, struct_dict):
            return "data" in struct_dict

        def hash_struct_dict(self, struct_dict, add_prefix=False):
            return "newhash"

    converter = NewConverter()
    assert converter.semantic_type_name == "newtype"
    assert converter.python_to_struct_dict([1, 2, 3]) == {"data": [1, 2, 3]}
    assert converter.struct_dict_to_python({"data": [1, 2, 3]}) == [1, 2, 3]
    assert converter.can_handle_python_type(list)
    assert converter.can_handle_struct_type("new_struct")
    assert converter.is_semantic_struct({"data": [1, 2, 3]})
    assert converter.hash_struct_dict({"data": [1, 2, 3]}) == "newhash"


# --- Edge cases ---
def test_dummy_converter_edge_cases():
    converter = DummyConverter()
    assert converter.is_semantic_struct({})
    assert not converter.is_semantic_struct(None)
    assert converter.hash_struct_dict({}) == "dummyhash"
