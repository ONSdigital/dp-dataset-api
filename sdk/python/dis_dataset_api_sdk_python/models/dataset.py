from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class LinkObject(BaseModel):
    href: str | None = None
    id: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class GeneralDetails(BaseModel):
    description: str | None = None
    href: str | None = None
    title: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class ContactDetails(BaseModel):
    email: str | None = None
    name: str | None = None
    telephone: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class Publisher(BaseModel):
    href: str | None = None
    name: str | None = None
    type: str | None = None

    model_config = ConfigDict(validate_by_name=True)


class IsBasedOn(BaseModel):
    type: str = Field(alias="@type")
    id: str = Field(alias="@id")

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)


class DatasetLinks(BaseModel):
    access_rights: LinkObject | None = None
    editions: LinkObject | None = None
    latest_version: LinkObject | None = None
    self: LinkObject | None = None
    taxonomy: LinkObject | None = None

    model_config = ConfigDict(validate_by_name=True)


class Dataset(BaseModel):
    id: str | None = None
    uri: str | None = None
    title: str | None = None
    description: str | None = None
    type: str | None = None
    state: str | None = None
    release_frequency: str | None = None
    next_release: str | None = None
    license: str | None = None
    theme: str | None = None
    unit_of_measure: str | None = None
    last_updated: datetime | None = None
    links: DatasetLinks | None = None
    contacts: list[ContactDetails] | None = None
    publisher: Publisher | None = None
    qmi: GeneralDetails | None = None
    methodologies: list[GeneralDetails] | None = None
    publications: list[GeneralDetails] | None = None
    related_datasets: list[GeneralDetails] | None = None
    related_content: list[GeneralDetails] | None = None
    national_statistic: bool | None = None
    canonical_topic: str | None = None
    subtopics: list[str] | None = None
    topics: list[str] | None = None
    survey: str | None = None
    previous_series_id: list[str] | None = None
    is_migration: bool | None = None
    is_based_on: IsBasedOn | None = None

    model_config = ConfigDict(validate_by_name=True)
