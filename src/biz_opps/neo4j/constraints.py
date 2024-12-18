from biz_opps.neo4j.helpers import get_neo4j_driver
from biz_opps.utils.file import load_json, get_root_dir

CONSTRAINTS_SCHEMA_PATH = f"{get_root_dir()}/configs/constraints_schema.json"


def load_constraints():
    """
    Loads constraints from a JSON file.

    Args:
        json_path (str): Path to the JSON file containing constraints.

    Returns:
        dict: The parsed JSON containing node definitions and constraints.
    """
    constraints = load_json(CONSTRAINTS_SCHEMA_PATH)
    return constraints


# ------------------------------ CONSTRAINTS CREATION ----------------------------------


def create_uniqueness_constraint(
    session, label_or_rel_type, constraint_name, property_name, is_relationship=False
):
    """
    Creates a uniqueness constraint on a property of a node or relationship in the Neo4j database.

    Args:
        session (neo4j.Session): Active Neo4j session for running the constraint query.
        label_or_rel_type (str): Node label or relationship type to apply the constraint.
        constraint_name (str): The name of the constraint to create.
        property_name (str): The name of the property that must be unique.
        is_relationship (bool): Whether the constraint is for a relationship (default is False for a node).

    Returns:
        None: Prints the success or error message for the operation.
    """
    try:
        if is_relationship:
            query = f"CREATE CONSTRAINT {constraint_name} FOR ()-[r:{label_or_rel_type}]-() REQUIRE r.{property_name} IS UNIQUE"
        else:
            query = f"CREATE CONSTRAINT {constraint_name} FOR (n:{label_or_rel_type}) REQUIRE n.{property_name} IS UNIQUE"

        session.run(query)
        print(f"Uniqueness constraint created for {label_or_rel_type}: {query}")
    except Exception as e:
        print(
            f"Error creating uniqueness constraint for {label_or_rel_type} and property {property_name}: {e}"
        )


def create_constraints(session, constraints):
    """
    Creates node and relationship uniqueness constraints defined in the constraints schema.

    Args:
        session (neo4j.Session): Active Neo4j session for running the constraint queries.
        constraints (dict): Dictionary with "nodes" and "relationships" keys, each containing definitions
                            and property-level constraints (e.g., uniqueness, existence, type).

    Returns:
        None: Iterates through the constraints and applies them to the database.
    """
    nodes = constraints.get("nodes", {})
    relationships = constraints.get("relationships", {})

    for label, node_schema in nodes.items():
        node_properties = node_schema.get("properties", {})
        for property_name, property_constraints in node_properties.items():
            if "unique" in property_constraints:
                constraint_name = property_constraints["unique"]["constraint_name"]
                create_uniqueness_constraint(
                    session, label, constraint_name, property_name
                )

    for rel_type, rel_schema in relationships.items():
        rel_properties = rel_schema.get("properties", {})
        for property_name, property_constraints in rel_properties.items():
            if "unique" in property_constraints:
                constraint_name = property_constraints["unique"]["constraint_name"]
                create_uniqueness_constraint(
                    session,
                    rel_type,
                    constraint_name,
                    property_name,
                    is_relationship=True,
                )

    print("Constraints creation complete.")


# ------------------------------ CONSTRAINTS DELETION ----------------------------------


def delete_constraint(session, constraint_name):
    """
    Deletes a constraint by its name in the Neo4j database.

    Args:
        session (neo4j.Session): Active Neo4j session for running the constraint query.
        constraint_name (str): The name of the constraint to delete.
    """
    try:
        query = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        session.run(query)
        print(f"Constraint {constraint_name} deleted successfully.")
    except Exception as e:
        print(f"Error deleting constraint {constraint_name}: {e}")


def delete_all_constraints(session, constraints):
    """
    Deletes all node and relationship uniqueness constraints.

    Args:
        session (neo4j.Session): Active Neo4j session for running the constraint queries.
        constraints (dict): Dictionary with "nodes" and "relationships" keys, each containing definitions
                            and property-level constraints (e.g., uniqueness, existence, type).
    """
    nodes = constraints.get("nodes", {})
    relationships = constraints.get("relationships", {})

    for _, node_schema in nodes.items():
        node_properties = node_schema.get("properties", {})
        for _, property_constraints in node_properties.items():
            if "unique" in property_constraints:
                constraint_name = property_constraints["unique"]["constraint_name"]
                delete_constraint(session, constraint_name)

    for _, rel_schema in relationships.items():
        rel_properties = rel_schema.get("properties", {})
        for _, property_constraints in rel_properties.items():
            if "unique" in property_constraints:
                constraint_name = property_constraints["unique"]["constraint_name"]
                delete_constraint(session, constraint_name)

    print("Constraints deletion complete.")
