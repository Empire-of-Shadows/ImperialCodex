from dataclasses import dataclass
from typing import List

from pymongo import IndexModel


@dataclass
class CollectionConfig:
    """Configuration for a collection including indexes and settings."""
    name: str
    database: str
    connection: str = 'primary'
    indexes: List[IndexModel] = None
    capped: bool = False
    max_size: int = None
    max_documents: int = None
