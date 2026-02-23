from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from esgpull.models.base import BaseNoSHA

if TYPE_CHECKING:
    from esgpull.models.query import File


class GlobusTransferStatus(Enum):
    """
    These reflect allowed values for Globus transfer service task status:
        https://docs.globus.org/api/transfer/task/#filter_and_order_by_options
    """
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class GlobusTransfer(BaseNoSHA):
    """
    Track the status of Globus transfers in progress (batch download of many files from a single collection)
    """
    __tablename__ = "globus_transfer"

    task_id: Mapped[str] = mapped_column(sa.String(36), primary_key=True)
    files: Mapped[list[File]] = relationship(
        back_populates="globus_transfer",
        init=False,
        default_factory=list,
        repr=False,
    )

    # Last time status polled by esgpull
    last_updated: Mapped[datetime] = mapped_column(
        server_default=sa.func.now(),
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )
    # Values returned via the globus transfer API
    status: Mapped[GlobusTransferStatus] = mapped_column(sa.Enum(GlobusTransferStatus))
    completion_time: Mapped[datetime | None] = mapped_column(sa.DateTime, default=None)
