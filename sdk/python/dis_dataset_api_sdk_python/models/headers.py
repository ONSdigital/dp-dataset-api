from __future__ import annotations

from pydantic import BaseModel


class HttpHeaders(BaseModel):
    CollectionID: str | None = None
    DownloadServiceToken: str | None = None
    IfMatch: str | None = None
    Authorization: str | None = None

    def to_http_headers(self) -> dict[str, str]:
        raw = self.model_dump(exclude_none=True)
        return {key: value for key, value in raw.items() if isinstance(value, str)}
