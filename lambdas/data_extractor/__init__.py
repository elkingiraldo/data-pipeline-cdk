# Re-export the handler module so tests can `from lambdas.data_extractor import handler`.
from . import handler

__all__ = ["handler"]
