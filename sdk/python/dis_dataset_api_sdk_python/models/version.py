from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from .dataset import (
    ContactDetails,
    DatasetLinks,
    GeneralDetails,
    IsBasedOn,
    LinkObject,
    Publisher,
)

MAX_IDS = 200


class Alert(BaseModel):
    date: str | None = None
    description: str | None = None
    type: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class UsageNote(BaseModel):
    note: str | None = None
    title: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class Distribution(BaseModel):
    title: str | None = None
    format: DistributionFormat | None = None
    media_type: DistributionMediaType | None = None
    download_url: str | None = None
    byte_size: int | None = None

    model_config = ConfigDict(validate_by_name=True)


class DistributionFormat(str, Enum):
    csv = "csv"
    sdmx = "sdmx"
    xls = "xls"
    xlsx = "xlsx"
    csdb = "csdb"
    csvw_metadata = "csvw-metadata"
    zip = "zip"
    json = "json"
    xml = "xml"


class DistributionMediaType(str, Enum):
    csv = "text/csv"
    sdmx = "application/vnd.sdmx.structurespecificdata+xml"
    xls = "application/vnd.ms-excel"
    xlsx = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    csdb = "text/plain"
    csvw_metadata = "application/ld+json"
    zip = "application/zip"


class DownloadObject(BaseModel):
    href: str | None = None
    private: str | None = None
    public: str | None = None
    size: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class DownloadList(BaseModel):
    xls: DownloadObject | None = None
    xlsx: DownloadObject | None = None
    csv: DownloadObject | None = None
    txt: DownloadObject | None = None
    csvw: DownloadObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class QualityDesignation(str, Enum):
    accredited_official = "accredited-official"
    official_in_development = "official-in-development"
    official = "official"
    no_accreditation = "no-accreditation"


class QueryParams(BaseModel):
    ids: list[str] | None = None
    is_based_on: str | None = None
    state: str | None = None
    limit: int | None = None
    offset: int | None = None

    model_config = ConfigDict(validate_by_name=True)

    def validate_params(self) -> None:
        """Validate query parameters."""
        if (self.limit is not None and self.limit < 0) or (
            self.offset is not None and self.offset < 0
        ):
            raise ValueError("negative offsets or limits are not allowed")

        if self.ids is not None and len(self.ids) > MAX_IDS:
            raise ValueError(
                f"too many query parameters have been provided. Maximum allowed: {MAX_IDS}"
            )

    def to_params(self) -> dict[str, list[str] | str | int]:
        """Convert to request parameters dictionary, excluding None values."""
        params: dict[str, list[str] | str | int] = {}
        if self.ids is not None:
            params["ids"] = self.ids
        if self.is_based_on is not None:
            params["is_based_on"] = self.is_based_on
        if self.state is not None:
            params["state"] = self.state
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
        return params


class VersionLinks(BaseModel):
    dataset: LinkObject | None = None
    dimensions: LinkObject | None = None
    edition: LinkObject | None = None
    self: LinkObject | None = None
    spatial: LinkObject | None = None
    version: LinkObject | None = None
    web_page: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class Version(BaseModel):
    alerts: list[Alert] | None = None
    collection_id: str | None = None
    dataset_id: str | None = None
    dimensions: list[dict] | None = None
    distributions: list[Distribution] | None = None
    downloads: DownloadList | None = None
    edition: str | None = None
    edition_title: str | None = None
    headers: list[str] | None = None
    id: str | None = None
    last_updated: datetime | None = None
    latest_changes: list[dict] | None = None
    links: VersionLinks | None = None
    release_date: str | None = None
    state: str | None = None
    temporal: list[dict] | None = None
    usage_notes: list[UsageNote] | None = None
    is_based_on: IsBasedOn | None = None
    version: int | None = None
    type: str | None = None
    lowest_geography: str | None = None
    quality_designation: QualityDesignation | None = None
    is_migration: bool | None = None
    previous_edition_id: list[str] | None = None
    related_content: list[GeneralDetails] | None = None
    etag: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class DimensionLink(BaseModel):
    code_list: LinkObject | None = None
    options: LinkObject | None = None
    version: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class Dimension(BaseModel):
    description: str | None = None
    label: str | None = None
    last_updated: datetime | None = None
    links: DimensionLink | None = None
    href: str | None = None
    id: str | None = None
    name: str | None = None
    variable: str | None = None
    number_of_options: int | None = None
    is_area_type: bool | None = None
    quality_statement_text: str | None = None
    quality_statement_url: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class PublicDimensionOptionLinks(BaseModel):
    code: LinkObject | None = None
    code_list: LinkObject | None = None
    version: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class PublicDimensionOption(BaseModel):
    label: str | None = None
    links: PublicDimensionOptionLinks | None = None
    name: str | None = None
    option: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class MetadataLinks(BaseModel):
    access_rights: LinkObject | None = None
    self: LinkObject | None = None
    spatial: LinkObject | None = None
    version: LinkObject | None = None
    website_version: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class Metadata(BaseModel):
    area_type: str | None = None
    alerts: list[Alert] | None = None
    canonical_topic: str | None = None
    classifications: str | None = None
    contacts: list[ContactDetails] | None = None
    coverage: str | None = None
    csv_header: list[str] | None = None
    dataset_links: DatasetLinks | None = None
    description: str | None = None
    dimensions: list[Dimension] | None = None
    distribution: list[str] | None = None
    distributions: list[Distribution] | None = None
    downloads: DownloadList | None = None
    edition: str | None = None
    edition_title: str | None = None
    id: str | None = None
    is_based_on: IsBasedOn | None = None
    is_migration: bool | None = None
    keywords: list[str] | None = None
    last_updated: datetime | None = None
    latest_changes: list[dict] | None = None
    links: MetadataLinks | None = None
    license: str | None = None
    methodologies: list[GeneralDetails] | None = None
    national_statistic: bool | None = None
    next_release: str | None = None
    publications: list[GeneralDetails] | None = None
    publisher: Publisher | None = None
    qmi: GeneralDetails | None = None
    quality_designation: QualityDesignation | None = None
    related_datasets: list[GeneralDetails] | None = None
    related_content: list[GeneralDetails] | None = None
    release_date: str | None = None
    release_frequency: str | None = None
    source: str | None = None
    state: str | None = None
    survey: str | None = None
    subtopics: list[str] | None = None
    table_id: str | None = None
    table_population: str | None = None
    temporal: list[dict] | None = None
    theme: str | None = None
    title: str | None = None
    topics: list[str] | None = None
    type: str | None = None
    unit_of_measure: str | None = None
    uri: str | None = None
    usage_notes: list[UsageNote] | None = None
    version: int | None = None

    model_config = ConfigDict(validate_by_name=True)


class VersionsList(BaseModel):
    items: list[Version] = Field(default_factory=list)
    count: int = 0
    offset: int = 0
    limit: int = 0
    total_count: int = 0

    model_config = ConfigDict(validate_by_name=True)


class VersionDimensionsList(BaseModel):
    items: list[Dimension] = Field(default_factory=list)

    model_config = ConfigDict(validate_by_name=True)


class VersionDimensionOptionsList(BaseModel):
    items: list[PublicDimensionOption] = Field(default_factory=list)

    model_config = ConfigDict(validate_by_name=True)
