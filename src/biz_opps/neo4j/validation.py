# Module containing functions to validate data before inserting into Neo4j.
from biz_opps.neo4j.constraints import load_constraints

VALIDATORS = {
    "STRING": lambda x: isinstance(x, str),
    "INTEGER": lambda x: isinstance(x, int),
    "FLOAT": lambda x: isinstance(x, (float, int)),
    "BOOLEAN": lambda x: isinstance(x, bool),
    "POINT": lambda x: (
        isinstance(x, dict)
        and "latitude" in x
        and isinstance(x["latitude"], (int, float))
        and "longitude" in x
        and isinstance(x["longitude"], (int, float))
    ),
    "LIST": lambda x: isinstance(x, list),
    "MAP": lambda x: isinstance(x, dict),
    "ENUM": lambda x, allowed_values: x in allowed_values,
}

# ------------------------------ DATA VALIDATION ---------------------------------


def validate_property(node_or_rel_data, property_name, property_constraints):
    """
    Validates a property of a node or relationship against the defined constraints.

    Args:
        node_or_rel_data (dict): The node or relationship data.
        property_name (str): The property that should exist.
        property_constraints (dict): The constraints defined for the property (e.g., type, allowed values).

    Raises:
        ValueError: If the property is missing or does not match allowed values.
        TypeError: If the property is of the wrong type.

    Returns:
        None: Raises an exception if validation fails, otherwise returns successfully
    """
    property_value = node_or_rel_data["properties"].get(property_name)

    exists, property_type, enum, numeric_range = (
        property_constraints.get("exists"),
        property_constraints.get("type"),
        property_constraints.get("enum"),
        property_constraints.get("range"),
    )
    # Check if the property must exist
    if exists and property_value is None:
        raise ValueError(f"Property '{property_name}' must exist but is missing.")

    # Skip validation
    if not exists and property_value is None:
        return

    # Validate type if specified
    if property_type and not VALIDATORS.get(property_type.upper(), lambda x: False)(
        property_value
    ):
        raise TypeError(
            f"Property '{property_name}' must be of type {property_type}, but got {type(property_value)}."
        )

    # Validate allowed values if specified (for ENUM type)
    if enum and property_value not in enum:
        raise ValueError(
            f"Property '{property_name}' must be one of {enum}, but got {property_value}."
        )

    # Validate numeric ranges (if applicable)
    if numeric_range:
        min_value, max_value = numeric_range.get("min"), numeric_range.get("max")
        if min_value is not None and property_value < min_value:
            raise ValueError(
                f"Property '{property_name}' must be greater than or equal to {min_value}."
            )
        if max_value is not None and property_value > max_value:
            raise ValueError(
                f"Property '{property_name}' must be less than or equal to {max_value}."
            )


def validate_data(node_or_rel_data, constraints, is_relationship=False):
    """
    Validates the properties of a node or relationship before inserting them into Neo4j during the ETL process.

    Args:
        node_or_rel (dict): The node or relationship data.
        constraints (dict): The constraints from the JSON configuration.
        is_relationship (bool): Whether the data is a relationship (default is False for a node).

    Returns:
        None: Raises an exception if validation fails, otherwise returns successfully.
    """
    label = node_or_rel_data["label"]
    node_or_rel_constraints = constraints.get(
        "nodes" if not is_relationship else "relationships"
    ).get(label, None)

    if not node_or_rel_constraints:
        raise ValueError(f"No constraints found for label '{label}'.")

    # Check for extra properties in the data that are not in the schema
    node_or_rel_property_keys = set(node_or_rel_data["properties"].keys())
    schema_property_keys = set(node_or_rel_constraints["properties"].keys())
    extra_properties = node_or_rel_property_keys - schema_property_keys
    if extra_properties:
        raise ValueError(f"Extra properties found in data: {extra_properties}")

    # Check for missing properties that are required by the schema
    missing_properties = {
        key
        for key, value in node_or_rel_constraints.items()
        if value.get("exists") and key not in node_or_rel_data["properties"]
    }
    if missing_properties:
        raise ValueError(f"Missing required properties: {missing_properties}")

    # Validate each property against its constraints
    for property_name, property_constraints in node_or_rel_constraints.items():
        validate_property(node_or_rel_data, property_name, property_constraints)
