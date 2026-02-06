from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import sys, os
import random
import pagegraph.commands
import pagegraph.graph
from pagegraph.serialize import ReportBase

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Optional, Union
    from pagegraph.graph.node.dom_root import DOMRootNode
    from pagegraph.serialize import ScriptReport, BasicReport, JSCallResultReport
    from pagegraph.types import PageGraphId


JS_CALLS_TO_IGNORE = ["DateConstructor", "ConsoleLog", "DateNow", "JsonParse", "JsonStringify", "DateTimeFormatSupportedLocalesOf", "DatePrototypeSetHours", "DatePrototypeSetMinutes", "DatePrototypeSetSeconds", "DateTimeFormatConstructor", "DateTimeFormatPrototypeResolvedOptions", "DatePrototypeToUTCString", "ConsoleWarn", "DateUTC", "DatePrototypeToISOString", "DatePrototypeToString", "DatePrototypeToLocaleString", "DatePrototypeToLocaleDateString", "DatePrototypeSetUTCFullYear", "DatePrototypeSetFullYear", "DateTimeFormatPrototypeFormat", "DateTimeFormatInternalFormat"]


THRESHOLD_FOR_JS_CALLS_TO_IGNORE = 500
THRESHOLD_FOR_JS_CALLS_TO_COSIDER = 5000



@dataclass
class Result(ReportBase):
    caller: Union[ScriptReport, BasicReport]
    call: JSCallResultReport


class Command(pagegraph.commands.Base):
    frame_nid: Optional[PageGraphId]
    cross_frame: bool
    method: Optional[str]
    pg_id: Optional[PageGraphId]

    def __init__(self, input_path: Path, frame_nid: Optional[PageGraphId],
                 cross_frame: bool, method: Optional[str],
                 pg_id: Optional[PageGraphId], debug: bool = False) -> None:
        
        self.frame_nid = frame_nid
        self.cross_frame = cross_frame
        self.method = method
        self.pg_id = pg_id
        super().__init__(input_path, debug)

    def validate(self) -> None:
        if self.frame_nid:
            pagegraph.commands.validate_node_id(self.frame_nid)
        if self.pg_id:
            pagegraph.commands.validate_pg_id(self.pg_id)
        return super().validate()

    def execute(self) -> pagegraph.commands.Result:

        pg = pagegraph.graph.from_path(self.input_path, self.debug)
        
        reports: list[Result] = []        
        domroot_node: Optional[DOMRootNode] = None

        if self.frame_nid:
            domroot_node = pg.node(self.frame_nid).as_domroot_node()
            if not domroot_node:
                raise ValueError("The PageGraph id provided is not " +
                    f"a DOMRootNode, nid={self.frame_nid}")

        can_do_fast_path = (domroot_node is not None and
            not self.cross_frame and not self.pg_id and not self.method)

        if can_do_fast_path:
            for call_edge in pg.js_call_edges():
                if call_edge.frame_id() != domroot_node.frame_id():
                    continue
                js_result = call_edge.call_result()
                result_report = js_result.to_report()
                script_node = js_result.call.incoming_node()
                script_report = script_node.to_report()
                reports.append(Result(script_report, result_report))
            return pagegraph.commands.Result(pg, reports)


        lits_of_nodes = list(pg.js_structure_nodes())

        js_structure_nodes = pg.js_structure_nodes()

        for js_node in js_structure_nodes:

            if self.method and self.method not in js_node.name():
                continue

            call_results = js_node.call_results()

            if js_node.name() in JS_CALLS_TO_IGNORE and len(call_results) > THRESHOLD_FOR_JS_CALLS_TO_IGNORE: 
                call_results = random.sample(call_results, k=THRESHOLD_FOR_JS_CALLS_TO_IGNORE)

            elif len(call_results) > THRESHOLD_FOR_JS_CALLS_TO_COSIDER: 
                call_results = random.sample(call_results, k=THRESHOLD_FOR_JS_CALLS_TO_COSIDER)

            nb_call_results = len(call_results)

            cpt = 0
            for call_result in call_results:
            

                cpt +=1 
                
                if (self.frame_nid and call_result.call_context().pg_id() != self.frame_nid):
                    continue


                if (self.cross_frame and not call_result.is_cross_frame_call()):
                    continue


                script_node = call_result.call.incoming_node()
                if self.pg_id and script_node.pg_id() != self.pg_id:
                    continue

                call_report = call_result.to_report()
                script_report = script_node.to_report()
                reports.append(Result(script_report, call_report))

        return pagegraph.commands.Result(pg, reports)