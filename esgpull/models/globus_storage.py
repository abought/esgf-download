from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class GlobusStorage(Base):
    """
    A globus-accessible storage location. Each row corresponds to the folder for a given dataset
        (or alternate asset replica): both an origin and a path
    """
    __tablename__ = "globus_storage"
    __table_args__ = (
        sa.UniqueConstraint("origin_id", "origin_path"),
    )

    origin_id: Mapped[str] = mapped_column(sa.String(36))
    origin_path: Mapped[str] = mapped_column(sa.Text)

    def _as_bytes(self) -> bytes:
        return str((self.origin_id, self.origin_path)).encode()
