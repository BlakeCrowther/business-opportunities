from biz_opps.neo4j.constraints import delete_all_constraints
from biz_opps.neo4j.construction import delete_nodes_by_label
from biz_opps.neo4j.spatial import remove_spatial_layer


def cleanup_neo4j(
    driver,
    constraints,
    nodes=None,
    spatial_layers=None,
):
    """
    Cleans up the Neo4j database by deleting all constraints, nodes, relationships, and spatial layers.

    Args:
        driver (neo4j.Driver): Neo4j driver object.
        constraints (dict): Constraints for data validation.
        nodes (list): Optional list of node labels to delete.
        spatial_layers (list): Optional list of spatial layer labels to delete.
    """
    print("Cleaning up Neo4j...")
    node_labels = nodes or list(constraints["nodes"].keys())
    spatial_layer_labels = spatial_layers or list(constraints["spatial_layers"].keys())
    with driver.session() as session:
        # Delete constraints
        delete_all_constraints(session, constraints)

        # Delete nodes
        for label in node_labels:
            delete_nodes_by_label(session, label)

        # Delete spatial layers
        for layer in spatial_layer_labels:
            # ensure nodes in layer are deleted
            layer_nodes = constraints["spatial_layers"][layer]["nodes"]
            for node in layer_nodes:
                delete_nodes_by_label(session, node)
            # delete layer
            remove_spatial_layer(session, layer)

    print("Neo4j cleanup complete.")
