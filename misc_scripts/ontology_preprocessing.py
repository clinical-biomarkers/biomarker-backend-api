"""Preprocesses the ontology into a hierarchical JSON format."""

from rdflib.term import BNode
from rdflib import Graph, RDFS, RDF, OWL, URIRef
import argparse
import sys
import os
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import write_json


def get_label(g, node):
    """Helper function to get label for a node."""
    # First try to get the label directly
    for o in g.objects(URIRef(node), RDFS.label):
        return str(o)

    # If no label found, try to get the label for string URIs
    if isinstance(node, str):
        for o in g.objects(URIRef(node), RDFS.label):
            return str(o)

    # Fallback to URI fragment if no label found
    node_str = str(node)
    return node_str.split("/")[-1]

def get_property_info(g, prop_uri):
    """Helper function to get both ID and label for a property."""
    # prop_uri is already a URIRef when it comes from the graph
    prop_id = str(prop_uri).split("/")[-1]
    prop_label = get_label(g, prop_uri)  # Pass the URIRef directly
    return {"id": prop_id, "label": prop_label}

def axiom_to_string(axiom):
    """Convert an axiom structure to a Protégé-like string representation."""
    if not axiom:
        return ""

    if axiom["type"] == "class":
        return axiom["label"]

    elif axiom["type"] == "intersection":
        return " and ".join(axiom_to_string(comp) for comp in axiom["components"])

    elif axiom["type"] == "union":
        return (
            "("
            + " or ".join(axiom_to_string(comp) for comp in axiom["components"])
            + ")"
        )

    elif axiom["type"] == "restriction":
        if axiom["restriction_type"] == "some":
            return (
                f"{axiom['property']['label']} some {axiom_to_string(axiom['target'])}"
            )
        elif axiom["restriction_type"] == "value":
            return f"{axiom['property']['label']} value '{axiom['target']}'"

    return ""


def process_restriction(g, restriction_node):
    """Process a restriction node (e.g., 'some' or 'value' restrictions)."""
    property_uri = None
    target = None
    restriction_type = None

    # Get the property
    for _, _, prop in g.triples((restriction_node, OWL.onProperty, None)):
        property_uri = prop  # prop is already a URIRef, no need to convert

    # Check for 'some' restriction
    for _, _, target_node in g.triples((restriction_node, OWL.someValuesFrom, None)):
        restriction_type = "some"
        target = process_class_expression(g, target_node)

    # Check for 'value' restriction
    for _, _, target_node in g.triples((restriction_node, OWL.hasValue, None)):
        restriction_type = "value"
        target = get_label(g, target_node)  # Pass the URIRef directly

    property_info = get_property_info(g, property_uri) if property_uri else None

    return {
        "type": "restriction",
        "restriction_type": restriction_type,
        "property": property_info,
        "target": target,
    }


def process_class_expression(g, node):
    """Process a class expression (class, union, intersection, or restriction)."""
    if isinstance(node, BNode):
        # Check for union
        for _, _, union_list in g.triples((node, OWL.unionOf, None)):
            return {
                "type": "union",
                "components": [
                    process_class_expression(g, item) for item in g.items(union_list)
                ],
            }

        # Check for intersection
        for _, _, intersection_list in g.triples((node, OWL.intersectionOf, None)):
            return {
                "type": "intersection",
                "components": [
                    process_class_expression(g, item)
                    for item in g.items(intersection_list)
                ],
            }

        # Must be a restriction
        return process_restriction(g, node)
    else:
        # Direct class reference - node is already a URIRef
        return {"type": "class", "label": get_label(g, node)}


def process_equivalence_axiom(g, class_node):
    """Process equivalence axioms for a given class."""
    equivalences = []

    for s, p, o in g.triples((class_node, OWL.equivalentClass, None)):
        equivalences.append(process_class_expression(g, o))

    return equivalences


def process_owl_to_tree(path: str) -> list:
    g = Graph()
    g.parse(path)
    child_parent = defaultdict(list)
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

        # Get equivalence axioms
        equivalences = process_equivalence_axiom(g, s)

        node_metadata[class_uri] = {
            "id": class_uri.split("/")[-1],
            "label": label,
            "definition": definition,
            "synonyms": synonyms,
            "equivalent_to": [axiom_to_string(axiom) for axiom in equivalences],
        }

    def build_tree(node):
        children = child_parent.get(node, [])
        node_data = node_metadata.get(node, {})
        return {
            "id": node_data.get("id"),
            "label": node_data.get("label"),
            "metadata": {
                "definition": node_data.get("definition"),
                "synonyms": node_data.get("synonyms"),
                "equivalent_to": node_data.get("equivalent_to"),
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
