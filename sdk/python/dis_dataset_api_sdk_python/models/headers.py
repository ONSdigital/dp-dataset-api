from __future__ import annotations

from pydantic import BaseModel


class HttpHeaders(BaseModel):
    collection_id: str | None = None
    download_service_token: str | None = None
    if_match: str | None = None
    authorization: str | None = None

    def to_http_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}

        if self.collection_id is not None:
            headers["Collection-ID"] = self.collection_id
        if self.download_service_token is not None:
            headers["X-Download-Service-Token"] = self.download_service_token
        if self.if_match is not None:
            headers["If-Match"] = self.if_match
        if self.authorization is not None:
            headers["Authorization"] = self.authorization

        return headers
