from __future__ import annotations

from typing import Optional

from pagegraph.graph.node.storage_area import StorageAreaNode


class CookieJarNode(StorageAreaNode):

    def as_cookie_jar_node(self) -> Optional[CookieJarNode]:
        return self


    def to_report(self, include_source: bool = False) -> CookieJarReport:
        return self
