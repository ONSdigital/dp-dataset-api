from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from .dataset import IsBasedOn, LinkObject
from .version import Alert, Distribution, QualityDesignation, UsageNote


class EditionUpdateLinks(BaseModel):
    dataset: LinkObject | None = None
    latest_version: LinkObject | None = None
    self: LinkObject | None = None
    versions: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class Edition(BaseModel):
    edition: str | None = None
    edition_title: str | None = None
    id: str | None = None
    dataset_id: str | None = None
    version: int | None = None
    last_updated: datetime | None = None
    release_date: str | None = None
    links: EditionUpdateLinks | None = None
    state: str | None = None
    alerts: list[Alert] | None = None
    usage_notes: list[UsageNote] | None = None
    distributions: list[Distribution] | None = None
    is_migration: bool | None = None
    is_based_on: IsBasedOn | None = None
    type: str | None = None
    quality_designation: QualityDesignation | None = None

    model_config = ConfigDict(validate_by_name=True)


class DatasetEdition(BaseModel):
    dataset_id: str
    title: str
    description: str
    edition: str
    edition_title: str
    latest_version: LinkObject
    release_date: str
    state: str

    model_config = ConfigDict(validate_by_name=True)


class DatasetEditionsList(BaseModel):
    """Response wrapper for dataset editions list."""

    items: list[DatasetEdition] | None = None
    count: int | None = None
    offset: int | None = None
    limit: int | None = None
    total_count: int | None = None

    model_config = ConfigDict(validate_by_name=True)


class EditionsList(BaseModel):
    """Response wrapper for editions list."""

    items: list[Edition] | None = None
    count: int = 0
    offset: int = 0
    limit: int = 0
    total_count: int = 0

    model_config = ConfigDict(validate_by_name=True)
