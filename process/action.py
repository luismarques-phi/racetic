from dataclasses import dataclass
from typing import Optional

from process.product import Product


# @dataclass(frozen=True)
class Action:
    def execute(self, file: Product) -> (bool, Optional[str]):
        raise NotImplemented


