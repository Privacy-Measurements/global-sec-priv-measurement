from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from pagegraph.graph.node.abc.dom_element import DOMElementNode
from pagegraph.serialize import Reportable, DOMElementReport

if TYPE_CHECKING:
    from pagegraph.serialize import JSONAble


class TextNode(DOMElementNode, Reportable):

    def as_text_node(self) -> Optional[TextNode]:
        return self

#    def to_report(self) -> DOMElementReport:
#        attrs: dict[str, JSONAble] = {"text": self.text()}
#        return DOMElementReport(self.pg_id(), self.tag_name(), attrs)

    #PATCH
    def to_report(self) -> DOMElementReport:
        parent = self.parent_at_serialization()
        parent_id = parent.pg_id() if parent else None
        parent_tag = parent.tag_name() if parent else None

        attrs: dict[str, JSONAble] = {"text": self.text()}

        return DOMElementReport(
            self.pg_id(),
            self.tag_name(),
            attrs,
            parent_id,
            parent_tag,
        )

    def tag_name(self) -> str:
        return "[text]"

    def text(self) -> str:
        return self.data()[self.__class__.RawAttrs.TEXT.value]
