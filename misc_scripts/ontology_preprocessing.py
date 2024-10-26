"""Preprocesses the ontology into a hierarchical JSON format.
"""

from rdflib.term import BNode
from rdflib import Graph, RDFS, RDF, OWL, URIRef
import argparse
import sys
import os
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import write_json


def process_owl_to_tree(path: str) -> list:

    g = Graph()
    g.parse(path)

    child_parent = defaultdict(list)

    # Dictionary to store node metadata
    node_metadata = {}

    # Get all subclass relationships
    for s, p, o in g.triples((None, RDFS.subClassOf, None)):
        if isinstance(s, BNode) or isinstance(o, BNode):
            continue
        child = str(s)
        parent = str(o)
        child_parent[parent].append(child)

    # Get metadata for each class
    for s in g.subjects(RDF.type, OWL.Class):
        if isinstance(s, BNode):
            continue

        class_uri = str(s)

        # Get label
        label = None
        for o in g.objects(s, RDFS.label):
            label = str(o)
            break

        # Get definition
        definition = None
        for o in g.objects(s, URIRef("http://purl.obolibrary.org/obo/IAO_0000115")):
            definition = str(o)
            break

        # Get synonyms
        synonyms = []
        for p, o in g.predicate_objects(s):
            if "synonym" in str(p):
                synonyms.append(str(o))

        node_metadata[class_uri] = {
            "id": class_uri.split("/")[-1],
            "label": label,
            "definition": definition,
            "synonyms": synonyms,
        }

    # Create tree structure
    def build_tree(node):
        children = child_parent.get(node, [])
        node_data = node_metadata.get(node, {})

        return {
            "id": node_data.get("id"),
            "label": node_data.get("label"),
            "metadata": {
                "definition": node_data.get("definition"),
                "synonyms": node_data.get("synonyms"),
            },
            "children": [build_tree(child) for child in children],
        }

    # Find root nodes (nodes without parents)
    all_children = set(
        [child for children in child_parent.values() for child in children]
    )
    root_nodes = [node for node in child_parent.keys() if node not in all_children]

    # Build tree starting from root nodes
    tree = [build_tree(root) for root in root_nodes]

    return tree


def main() -> None:

    parser = argparse.ArgumentParser(prog="ontology_preprocessing.py")
    parser.add_argument("owl_path", help="path to the source ontology file (.owl file)")
    options = parser.parse_args()

    ontology_fp = options.owl_path
    tree = process_owl_to_tree(path=ontology_fp)
    write_json("./obci.json", tree)


if __name__ == "__main__":
    main()
