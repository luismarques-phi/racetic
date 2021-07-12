from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    id: str
    root_location: str
    extension: str
