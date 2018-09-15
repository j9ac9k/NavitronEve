from pathlib import Path
from ast import literal_eval
from itertools import count
from typing import Dict, Tuple, List
from timeit import default_timer as timer
import csv
import logging

from networkx import DiGraph


class System:
    def __init__(
        self,
        name: str,
        id: int,
        security: float,
        connections: List[int],
        region_name: str,
        constellation_name: str,
        index: int,
        risk: float = float("inf"),
    ):

        self.name = name
        self.id = id
        self.security = security
        self.region_name = region_name
        self.connections = connections
        self.constellation_name = constellation_name
        self.index = index
        self.risk = risk

    def __hash__(self):
        # returns a unique value starting from 0, and at max
        # n_systems; having it return this value helps with
        # algorithm runtime
        return self.index


def parse_universe() -> Tuple[Dict[int, System], Dict[str, int]]:

    # FIXME: parsing exepcts CSV, needs to change to connect to db
    fpath = Path(__file__).parent / "Data" / "sde-universe_2018-07-16.csv"
    system_mapping: Dict[int, System] = {}
    name_to_id: Dict[str, int] = {}

    # jovian systems that are not connected to the eve universe
    excluded_regions = set(["J7HZ-F", "A821-A", "UUA-F4"])

    with open(fpath) as csv_file:
        counter = count()
        reader = csv.DictReader(csv_file)
        for row in reader:
            system_id = int(row["system_id"])
            region_name = row["region_name"]
            # check for systems not part of the standard universe
            if (
                (system_id >= 31_000_000)
                or (region_name in excluded_regions)
                or not row["stargates"]
            ):
                continue
            system_name = row["solarsystem_name"]
            new_system = System(
                system_name,
                system_id,
                max(0.0, float(row["security_status"])),
                list(literal_eval(row["stargates"])),
                region_name=region_name,
                constellation_name=row["constellation_name"],
                index=next(counter),
            )
            system_mapping[system_id] = new_system
            name_to_id[system_name] = system_id
    return system_mapping, name_to_id


def construct_graph(system_mapping: Dict[int, System]) -> DiGraph:
    di_graph = DiGraph()
    di_graph.add_nodes_from(system_mapping.values())
    di_graph.add_weighted_edges_from(
        [
            (u, system_mapping[connection], system_mapping[connection].risk)
            for u in system_mapping.values()
            for connection in u.connections
        ]
    )
    return di_graph
