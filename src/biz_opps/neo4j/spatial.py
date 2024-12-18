def init_wkt_layer(session, layer_name, geometry_property_name="wkt"):
    """
    Initialize a WKT layer if it doesn't exist.

    Args:
        session: Neo4j session
        layer_name: Name of the layer
        geometry_property_name: Name of the geometry property
    """
    try:
        # First check if layer exists
        check_query = """
        MATCH (layer:SpatialLayer {layer: $layer_name}) 
        RETURN count(layer) as count
        """
        result = session.run(check_query, {"layer_name": layer_name})
        exists = result.single()["count"] > 0

        if not exists:
            # Create layer only if it doesn't exist
            query = """
            CALL spatial.addWKTLayer($layer_name, $geometry_property)
            YIELD node
            RETURN node
            """
            session.run(
                query,
                {"layer_name": layer_name, "geometry_property": geometry_property_name},
            )
            print(f"Created WKT layer: {layer_name}")
        else:
            print(f"Using existing WKT layer: {layer_name}")
        return True
    except Exception as e:
        print(f"Failed to initialize WKT layer {layer_name}: {e}")
        return False


def init_point_layer(session, layer_name):
    """
    Initialize a point layer if it doesn't exist.
    """
    # Check if layer exists
    check_query = """
    MATCH (layer:SpatialLayer {layer: $layer_name}) 
    RETURN count(layer) as count
    """
    try:
        result = session.run(check_query, {"layer_name": layer_name})
        exists = result.single()["count"] > 0

        if not exists:
            # Create layer only if it doesn't exist
            query = """
            CALL spatial.addPointLayer($layer_name)
            YIELD node
            RETURN node
            """
            session.run(query, {"layer_name": layer_name})
            print(f"Created point layer: {layer_name}")
        else:
            print(f"Using existing point layer: {layer_name}")
        return True
    except Exception as e:
        print(f"Failed to initialize point layer {layer_name}: {e}")
        return False


def add_node_to_spatial_layer(session, node, layer_name):
    """
    Add a node to a specified spatial layer.

    Args:
        session: Neo4j session
        node: Node to add to the layer
        layer_name: Name of the layer

    Note: This will reference the geometry property specified during spatial layer creation.
    """
    try:
        query = """
        MATCH (n) WHERE elementId(n) = $node_id
        CALL spatial.addNode($layer_name, n)
        YIELD node
        RETURN node
        """

        result = session.run(
            query, {"layer_name": layer_name, "node_id": node.element_id}
        )
        return bool(result.single())
    except Exception as e:
        print(f"Failed to add node to spatial layer {layer_name}: {e}")
        return False


def remove_spatial_layer(session, layer_name):
    query = """
    CALL spatial.removeLayer($layer_name)
    """
    try:
        session.run(query, layer_name=layer_name)
        print(f"Removed spatial layer: {layer_name}")
    except Exception as e:
        print(f"Failed to remove spatial layer {layer_name}: {e}")


def find_within_distance(session, point, distance, layer_name):
    """Find all geometries within a certain distance of a point."""
    query = """
    CALL spatial.withinDistance($layer_name, $point, $distance) 
    YIELD node, distance
    RETURN node, distance
    """
    return session.run(query, layer_name=layer_name, point=point, distance=distance)


def find_containing_geometry(session, point, layer_name):
    """Find geometry containing the given point."""
    query = """
    CALL spatial.intersects($layer_name, $point) 
    YIELD node
    RETURN node
    """
    return session.run(query, layer_name=layer_name, point=point)
