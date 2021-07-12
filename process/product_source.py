import os
from dataclasses import dataclass, field
from typing import Optional

from process.product import Product


@dataclass(frozen=True)
class ProductSource:
    ...


@dataclass(frozen=True)
class FileSystemSource(ProductSource):
    root_path: str
    filter_extension: str
    filter_contains: Optional[str] = field(default=None)

    def get_files(self) -> list[Product]:
        result = []
        for root, dirs, files in os.walk(self.root_path):
            for file in files:
                filename, file_extension = os.path.splitext(file)
                if file_extension == self.filter_extension:
                    if self.filter_contains and self.filter_contains not in filename:
                        continue
                    result.append(Product(id=file, root_location=root, extension=file_extension))
        return result
