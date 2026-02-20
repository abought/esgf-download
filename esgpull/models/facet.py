import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class Facet(Base):
    """A facet represents one search term/value filter pair. A query may contain more than one facet."""
    __tablename__ = "facet"

    name: Mapped[str] = mapped_column(sa.String(64))
    value: Mapped[str] = mapped_column(sa.String(255))

    def _as_bytes(self) -> bytes:
        self_tuple = (self.name, self.value)
        return str(self_tuple).encode()

    def __hash__(self) -> int:
        return hash(self._as_bytes())
