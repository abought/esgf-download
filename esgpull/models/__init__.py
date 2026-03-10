from typing import TypeVar

from esgpull.models.base import Base, _BaseModel
from esgpull.models.dataset import Dataset, DatasetRecord
from esgpull.models.facet import Facet
from esgpull.models.file import FastFile, FileStatus
from esgpull.models.globus_storage import GlobusStorage
from esgpull.models.globus_transfer import GlobusTransfer, GlobusTransferStatus
from esgpull.models.options import Option, Options
from esgpull.models.query import File, LegacyQuery, Query, QueryDict
from esgpull.models.selection import Selection
from esgpull.models.synda_file import SyndaFile
from esgpull.models.tag import Tag

Table = TypeVar("Table", bound=_BaseModel)

__all__ = [
    "Base",
    "Dataset",
    "DatasetRecord",
    "Facet",
    "FastFile",
    "File",
    "FileStatus",
    "GlobusStorage",
    "GlobusTransfer",
    "GlobusTransferStatus",
    "LegacyQuery",
    "Option",
    "Options",
    "Query",
    "QueryDict",
    "Selection",
    "SyndaFile",
    "Table",
    "Tag",
]
