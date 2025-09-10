def test_import_constants_executes_lines():
    from infrastructure.config import constants as c

    # Assert that we can import the module and it has public constants
    public_attrs = [a for a in dir(c) if a.isupper()]
    assert public_attrs, "No se detectaron constantes públicas"

    # Validate that each public constant is not None
    for name in public_attrs:
        val = getattr(c, name)
        assert val is not None
