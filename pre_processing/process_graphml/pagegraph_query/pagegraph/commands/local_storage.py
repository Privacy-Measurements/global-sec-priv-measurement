from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
import networkx
import json

import pagegraph.commands
import pagegraph.graph
from pagegraph.serialize import ReportBase

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Optional, Union

    from pagegraph.serialize import ScriptReport, BasicReport, FrameReport
    from pagegraph.types import PageGraphId, PageGraphNodeId


@dataclass
class Result(ReportBase):
    caller: Union[ScriptReport, BasicReport]
    call: JSCallResultReport


class Command(pagegraph.commands.Base):
    frame_nid: Optional[PageGraphNodeId]
    cross_frame: bool
    include_source: bool
    depth: int




    def __init__(self, input_path: Path, frame_nid: Optional[PageGraphId],
                 cross_frame: bool,
                 pg_id: Optional[PageGraphId], debug: bool = False) -> None:

        self.frame_nid = frame_nid
        self.cross_frame = cross_frame
        self.pg_id = pg_id
        super().__init__(input_path, debug)


    def validate(self) -> bool:
        if self.frame_nid:
            pagegraph.commands.validate_node_id(self.frame_nid)
        if self.pg_id:
            pagegraph.commands.validate_pg_id(self.pg_id)
        return super().validate()


    def get_key_of_caller(self, edge_report):

        if edge_report['incoming node']['type'] == 'local storage':
            return 'outgoing node'

        elif edge_report['outgoing node']['type'] == 'local storage':
            return "incoming node"

        return None

    def execute(self) -> pagegraph.commands.Result:

        reports: list[Result] = []

        pg = pagegraph.graph.from_path(self.input_path, self.debug)
        local_storage_nodes = pg.local_storage_nodes()

        if len(local_storage_nodes) > 1:
            throw('BECAREFUL MORE THAN ONE localStorageNode:', len(local_storage_nodes), self.input_path)
            return

        if len(local_storage_nodes) == 0:
            return {}

        local_storage_node = local_storage_nodes[0]
        outgoing_edges = local_storage_node.outgoing_edges()
        incoming_edges = local_storage_node.incoming_edges()

        for outgoing_edge in outgoing_edges + incoming_edges:
            report = {}

            edge_report = outgoing_edge.to_edge_report()
            edge_report = json.loads(pagegraph.commands.Result(pg, edge_report).to_json())['report']
            edge_data = outgoing_edge.data()
            report['edge_id'] = edge_report['id']
            report['event_type'] = edge_report['type']

            if report['event_type'] == 'storage bucket':
                continue


            report['storage_key'] = edge_data['key']
            report['storage_value'] = edge_data.get('value', 0)
            report['caller'] = {}
            caller_key = self.get_key_of_caller(edge_report)

            report['caller']['id'] = edge_report[caller_key]['id']
            report['caller']['type'] = edge_report[caller_key]['type']
            report['caller']['hash'] = edge_report[caller_key]['details']['hash']
            report['caller']['type_script'] = edge_report[caller_key]['details']['script type']
            report['caller']['url'] = edge_report[caller_key]['details']['url']

            if 'details' in report and 'frame id' in report['details']:
                report['frame_id'] =  report['details']['frame id']


            reports.append(report)

        return pagegraph.commands.Result(pg, reports)
    