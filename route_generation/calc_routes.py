from typing import Dict, List
from functools import partial

from build_graph import System  # for type hinting
from networkx import single_source_dijkstra_path, all_pairs_dijkstra_path, DiGraph


def all_pairs_optimum_routes(
    G: DiGraph, cutoff=None, weight="weight"
) -> Dict[Dict[System, List[System]]]:
    return dict(all_pairs_dijkstra_path(G, weight=weight))


def single_source_optimal_path(
    G: DiGraph, source: System, cutoff=None, weight="weight"
) -> Dict[Dict[System, List[System]]]:
    return dict(single_source_dijkstra_path(G, source, cutoff=cutoff, weight=weight))


def path_between(
    start: str,
    dest: str,
    paths: Dict[System, Dict[System, List[System]]],
    name_mapping: Dict[str, int],
    system_mapping: Dict[int, System]
):
    """
    Method takes a start and destination system by name, as well as references to the paths, and dictionaries to access the system objects,
    """
    assert start in name_mapping.keys()
    assert dest in name_mapping.keys()
    start_system = system_mapping[name_mapping[start]]
    dest_system = system_mapping[name_mapping[dest]]
    return [system.name for system in paths[start_system][dest_system]]
