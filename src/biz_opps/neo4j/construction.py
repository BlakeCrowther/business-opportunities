# Module containing functions to create and manage nodes and relationships in a Neo4j graph database.

# ------------------------------ NODE MANAGEMENT FUNCTIONS ----------------------------


def create_node(session, label, properties, match_keys=None, verbose=False):
    """
    Creates a node in Neo4j using MERGE to ensure uniqueness.

    Args:
        session (neo4j.Session): The Neo4j session to use
        label (str): The label of the node
        properties (dict): Node properties
        match_keys (list): List of property names to use for matching. If None, uses all properties
        verbose (bool): Whether to print verbose output

    Returns:

    """
    try:
        # Build merge properties dictionary
        merge_props = {key: properties[key] for key in match_keys}

        # Build the MERGE pattern with literal properties
        merge_pattern = ", ".join(f"{k}: ${k}" for k in match_keys)
        query = f"""
        MERGE (n:{label} {{{merge_pattern}}})
        SET n += $remaining_props
        RETURN n
        """

        # Separate remaining properties
        remaining_props = {k: v for k, v in properties.items() if k not in match_keys}
        result = session.run(query, {**merge_props, "remaining_props": remaining_props})

        node = result.single()["n"]
        return node
    except Exception as e:
        print(f"Failed to merge node for {label}: {e}")
        return False


def delete_node(session, label, properties, match_keys=None):
    """
    Deletes a node in Neo4j using merge keys for identification.

    Args:
        session (neo4j.Session): The Neo4j session to use
        label (str): The label of the node
        properties (dict): Node properties to identify the node
        match_keys (list): List of property names to use for matching. If None, uses all properties
    """
    try:
        # Determine which properties to use for matching
        match_props = (
            {k: properties[k] for k in match_keys} if match_keys else properties
        )

        # Build the match condition string
        match_props_str = ", ".join(f"{k}: ${k}" for k in match_props.keys())

        # First check if the node exists
        check_query = (
            f"MATCH (n:{label} {{{match_props_str}}}) RETURN COUNT(n) as count"
        )
        result = session.run(check_query, match_props)
        count = result.single()["count"]

        if count == 0:
            print(f"No node with matching properties found: {match_props}")
            return False

        # If node exists, delete it and its relationships
        delete_query = f"MATCH (n:{label} {{{match_props_str}}}) DETACH DELETE n"
        session.run(delete_query, match_props)
        print(f"Node with properties {match_props} deleted.")
        return True

    except Exception as e:
        print(f"Failed to delete node {label} with properties {match_props}: {e}")
        return False


def delete_nodes_by_label(session, label):
    """
    Deletes all nodes with the specified label and all relationships connected to them.

    Args:
        session (neo4j.Session): The Neo4j session to use for running the query.
        label (str): The label of the nodes to delete.

    Returns:
        None: This function does not return anything.

    Note:
        This operation will delete all nodes with the given label, along with their connected relationships.
    """
    try:
        query = f"MATCH (n:{label}) DETACH DELETE n"
        session.run(query)
        print(f"All nodes with label '{label}' deleted.")
    except Exception as e:
        print(f"Failed to delete nodes with label '{label}': {e}")


# ------------------------------ RELATIONSHIP MANAGEMENT FUNCTIONS ---------------------


def create_relationship(
    session,
    start_label,
    start_props,
    start_match_keys,
    end_label,
    end_props,
    end_match_keys,
    rel_type,
    rel_properties=None,
    verbose=False,
):
    """Creates a relationship between nodes using MERGE."""
    try:
        # Build match strings for query
        start_match_str = ", ".join(f"{k}: ${k}" for k in start_match_keys)
        end_match_str = ", ".join(f"{k}: ${k}" for k in end_match_keys)

        params = {
            **{k: start_props[k] for k in start_match_keys},
            **{k: end_props[k] for k in end_match_keys},
        }

        # Create relationship
        create_query = f"""
        MATCH (a:{start_label} {{{start_match_str}}})
        MATCH (b:{end_label} {{{end_match_str}}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r = $rel_properties
        WITH a, b, r
        RETURN r
        """

        result = session.run(
            create_query, {**params, "rel_properties": rel_properties or {}}
        )

        if verbose:
            print(
                f"Created {rel_type} relationship between {start_label} and {end_label}"
            )
        return bool(result.single()["r"])

    except Exception as e:
        print(f"Error creating {rel_type} relationship:")
        print(f"Start ({start_label}): {start_props}")
        print(f"End ({end_label}): {end_props}")
        print(f"Error: {str(e)}")
        return False


def update_relationship_properties(
    session,
    start_label,
    start_props,
    start_match_keys,
    end_label,
    end_props,
    end_match_keys,
    rel_type,
    new_properties,
):
    """
    Updates properties of an existing relationship between two nodes.

    Args:
        session (neo4j.Session): The Neo4j session to use
        start_label (str): The label of the starting node
        start_props (dict): Properties of the starting node
        start_match_keys (list): Properties to use for matching start node
        end_label (str): The label of the ending node
        end_props (dict): Properties of the ending node
        end_match_keys (list): Properties to use for matching end node
        rel_type (str): The type of the relationship
        new_properties (dict): Properties to update on the relationship
    """
    try:
        # Build match conditions
        start_match_str = ", ".join(f"{k}: ${k}" for k in start_match_keys)
        end_match_str = ", ".join(f"{k}: ${k}" for k in end_match_keys)

        query = f"""
        MATCH (a:{start_label} {{{start_match_str}}})-[r:{rel_type}]->(b:{end_label} {{{end_match_str}}})
        SET r += $new_properties
        """

        # Get match properties
        start_match_props = {k: start_props[k] for k in start_match_keys}
        end_match_props = {k: end_props[k] for k in end_match_keys}

        session.run(
            query,
            {**start_match_props, **end_match_props, "new_properties": new_properties},
        )
        print(
            f"Relationship '{rel_type}' updated between {start_label} and {end_label}"
        )
        return True
    except Exception as e:
        print(f"Failed to update relationship {rel_type}: {e}")
        return False


def delete_relationship(
    session,
    start_label,
    start_props,
    start_match_keys,
    end_label,
    end_props,
    end_match_keys,
    rel_type,
):
    """
    Deletes a relationship between nodes based on matching keys.

    Args:
        session (neo4j.Session): The Neo4j session to use
        start_label (str): The label of the starting node
        start_props (dict): Properties of the starting node
        start_match_keys (list): Properties to use for matching start node
        end_label (str): The label of the ending node
        end_props (dict): Properties of the ending node
        end_match_keys (list): Properties to use for matching end node
        rel_type (str): The type of the relationship to delete
    """
    try:
        # Build match conditions
        start_match_str = ", ".join(f"{k}: ${k}" for k in start_match_keys)
        end_match_str = ", ".join(f"{k}: ${k}" for k in end_match_keys)

        query = f"""
        MATCH (a:{start_label} {{{start_match_str}}})-[r:{rel_type}]->(b:{end_label} {{{end_match_str}}})
        DELETE r
        """

        # Get match properties
        start_match_props = {k: start_props[k] for k in start_match_keys}
        end_match_props = {k: end_props[k] for k in end_match_keys}

        session.run(query, {**start_match_props, **end_match_props})
        print(
            f"Relationship '{rel_type}' deleted between {start_label} and {end_label}"
        )
        return True
    except Exception as e:
        print(f"Failed to delete relationship {rel_type}: {e}")
        return False


def delete_relationships_by_type(session, rel_type):
    """
    Deletes all relationships of a specific type in the graph.

    Args:
        session (neo4j.Session): The Neo4j session to use for running the query.
        rel_type (str): The type of the relationships to delete.

    Returns:
        bool: True if the relationships were deleted successfully, False otherwise.

    Note:
        This function will delete all relationships of the specified type, leaving the nodes intact.
    """
    try:
        query = f"MATCH ()-[r:{rel_type}]->() DELETE r"
        session.run(query)
        print(f"All relationships of type '{rel_type}' deleted.")
        return True
    except Exception as e:
        print(f"Failed to delete relationships of type '{rel_type}': {e}")
        return False


# ------------------------------ INDEX MANAGEMENT FUNCTIONS ----------------------------


def create_node_index(session, node_type, property_names, verbose=False):
    """Creates node index for a node type on a property or composite of properties."""
    try:
        if len(property_names) > 1:
            props_str = ", ".join(f"n.{prop}" for prop in property_names)
            query = f"""
            CREATE INDEX {node_type.lower()}_composite IF NOT EXISTS
            FOR (n:{node_type})
            ON ({props_str})
            """
        else:
            query = f"""
            CREATE INDEX {node_type.lower()}_{property_names[0]} IF NOT EXISTS
            FOR (n:{node_type})
            ON (n.{property_names[0]})
            """
        session.run(query)
    except Exception as e:
        print(f"Failed to create node indexes for {node_type}: {e}")
        return False
