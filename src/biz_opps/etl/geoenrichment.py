import pandas as pd
from itertools import product
import numpy as np

from biz_opps.neo4j.cleanup import cleanup_neo4j
from biz_opps.neo4j.construction import (
    create_node,
    create_node_index,
    create_relationship,
)
from biz_opps.utils.file import get_root_dir

# from biz_opps.neo4j.validation import validate_data  TODO: Add validation


def create_enrichment_indexes(session, verbose=False):
    """Creates enrichment indexes."""
    # Enrichment node indexes
    enrichment_indexes = {
        "TotalPopulation": ["level"],
        "PopulationGrowth": ["growth_rate"],
        "AgeAverage": ["group"],
        "AgeGroup": ["group", "representation"],
        "WealthIndex": ["category"],
        "EducationLevel": ["level", "representation"],
        "CrimeIndex": ["category"],
        "FastFoodSpendingIndex": ["category"],
    }

    for node_type, properties in enrichment_indexes.items():
        # Single property indexes
        for prop in properties:
            session.run(
                f"""
            CREATE INDEX {node_type.lower()}_{prop} IF NOT EXISTS
            FOR (n:{node_type})
            ON (n.{prop})
            """
            )

        # Composite index for nodes with multiple properties
        if len(properties) > 1:
            props_str = ", ".join(f"n.{prop}" for prop in properties)
            session.run(
                f"""
                CREATE INDEX {node_type.lower()}_composite IF NOT EXISTS
                FOR (n:{node_type})
                ON ({props_str})
                """
            )


def prepare_data(df):
    # Define age midpoints (one for each age group)
    age_midpoints = [
        2.5,  # 0-4
        7.5,  # 5-9
        12.5,  # 10-14
        17.5,  # 15-19
        22.5,  # 20-24
        27.5,  # 25-29
        32.5,  # 30-34
        37.5,  # 35-39
        42.5,  # 40-44
        47.5,  # 45-49
        52.5,  # 50-54
        57.5,  # 55-59
        62.5,  # 60-64
        67.5,  # 65-69
        72.5,  # 70-74
        77.5,  # 75-79
        82.5,  # 80-84
        87.5,  # 85+
    ]

    # Identify male and female columns
    male_columns = [col for col in df.columns if col.startswith("male")]
    female_columns = [col for col in df.columns if col.startswith("fem")]

    # Combine male and female population for each age group
    combined_population = [
        df[male_columns[i]] + df[female_columns[i]] for i in range(len(male_columns))
    ]

    # Calculate weighted age sum and total population for all genders
    df["weighted_age"] = sum(
        combined_population[i] * age_midpoints[i]
        for i in range(len(combined_population))
    )

    df["avg_age"] = df["weighted_age"] / df["totpop_cy"]

    # Normalize wealth index
    df["normalized_wlthindxcy"] = (df["wlthindxcy"] - df["wlthindxcy"].min()) / (
        df["wlthindxcy"].max() - df["wlthindxcy"].min()
    )

    # Fast food spending
    df["fastfoodspending"] = df["x1133_a"] + df["x1138_a"] + df["x1148_a"]
    df["normalized_fastfoodspending"] = (
        df["fastfoodspending"] - df["fastfoodspending"].min()
    ) / (df["fastfoodspending"].max() - df["fastfoodspending"].min())

    # Education levels
    df["BASIC"] = df[["nohs_cy", "somehs_cy"]].sum(axis=1)
    df["SECONDARY"] = df[["hsgrad_cy", "ged_cy", "smcoll_cy"]].sum(axis=1)
    df["HIGHER"] = df[["asscdeg_cy", "bachdeg_cy", "graddeg_cy"]].sum(axis=1)

    return df


def determine_population_level(value):
    """Determines population level category."""
    source_value = value
    if value < 1000:
        category = "LOW"
    elif value <= 2000:
        category = "MEDIUM"
    else:
        category = "HIGH"
    return ([{"level": category}], [source_value])


def determine_growth_rate(value):
    """Determines population growth rate category."""
    source_value = value
    if value < 0:
        category = "NEGATIVE"
    elif value <= 1:
        category = "LOW"
    elif value <= 2:
        category = "MODERATE"
    elif value <= 3:
        category = "HIGH"
    else:
        category = "VERY_HIGH"
    return ([{"growth_rate": category}], [source_value])


def determine_age_average(value):
    """Determines age average category."""
    source_value = value
    if value < 5:
        category = "0-4"
    elif value < 15:
        category = "5-14"
    elif value < 25:
        category = "15-24"
    elif value < 45:
        category = "25-44"
    elif value < 65:
        category = "45-64"
    else:
        category = "65+"
    return ([{"group": category}], [source_value])


def determine_age_group_representations(row):
    """Determines age group representations."""
    total_population = row["totpop_cy"]

    def get_representation_level(x):
        if x < 0.05:
            return "VERY_LOW"
        elif x < 0.10:
            return "LOW"
        elif x < 0.20:
            return "MODERATE"
        elif x < 0.30:
            return "HIGH"
        else:
            return "DOMINANT"

    age_group_aggregations = {
        "0-4": row["male0"] + row["fem0"],
        "5-14": row["male5"] + row["male10"] + row["fem5"] + row["fem10"],
        "15-24": row["male15"] + row["male20"] + row["fem15"] + row["fem20"],
        "25-44": row["male25"]
        + row["male30"]
        + row["male35"]
        + row["male40"]
        + row["fem25"]
        + row["fem30"]
        + row["fem35"]
        + row["fem40"],
        "45-64": row["male45"]
        + row["male50"]
        + row["male55"]
        + row["male60"]
        + row["fem45"]
        + row["fem50"]
        + row["fem55"]
        + row["fem60"],
        "65+": row["male65"]
        + row["male70"]
        + row["male75"]
        + row["male80"]
        + row["male85"]
        + row["fem65"]
        + row["fem70"]
        + row["fem75"]
        + row["fem80"]
        + row["fem85"],
    }

    age_group_representations = []
    source_values = []

    # Handle zero total population
    if total_population == 0:
        for group in age_group_aggregations.keys():
            age_group_representations.append(
                {"group": group, "representation": "VERY_LOW"}
            )
            source_values.append(0.0)
    else:
        for group, aggregation in age_group_aggregations.items():
            representation = aggregation / total_population
            representation_level = get_representation_level(representation)
            age_group_representations.append(
                {"group": group, "representation": representation_level}
            )
            source_values.append(representation)

    return (age_group_representations, source_values)


def determine_wealth_category(value):
    """Determines wealth index category."""
    source_value = value
    if value <= 0.2:
        category = "LOW"
    elif value <= 0.4:
        category = "LOWER_MIDDLE"
    elif value <= 0.6:
        category = "MIDDLE"
    elif value <= 0.8:
        category = "UPPER_MIDDLE"
    else:
        category = "HIGH"
    return ([{"category": category}], [source_value])


def determine_education_level(row):
    """Determines education level category."""
    education_levels = {
        "BASIC": row["BASIC"],
        "SECONDARY": row["SECONDARY"],
        "HIGHER": row["HIGHER"],
    }

    def get_representation_level(x):
        if x < 0.05:
            return "VERY_LOW"
        elif x < 0.15:
            return "LOW"
        elif x < 0.30:
            return "MODERATE"
        elif x < 0.50:
            return "HIGH"
        else:
            return "VERY_HIGH"

    education_level_representations = []
    source_values = []

    # Check for zero total population
    total_pop = row["totpop_cy"]
    if total_pop == 0:
        # Return default values for zero population
        for level in education_levels.keys():
            education_level_representations.append(
                {"level": level, "representation": "VERY_LOW"}
            )
            source_values.append(0.0)
    else:
        # Calculate representations normally
        for level, value in education_levels.items():
            representation = value / total_pop
            representation_level = get_representation_level(representation)
            education_level_representations.append(
                {"level": level, "representation": representation_level}
            )
            source_values.append(representation)

    return (education_level_representations, source_values)


def determine_crime_level(value):
    """Determines crime index category."""
    source_value = value
    if value < 80:
        category = "SAFEST"
    elif value <= 119:
        category = "SAFE"
    elif value <= 199:
        category = "MODERATE"
    elif value <= 499:
        category = "UNSAFE"
    else:
        category = "MOST_UNSAFE"
    return ([{"category": category}], [source_value])


def determine_spending_level(value):
    """Determines fast food spending level category."""
    source_value = value
    if value <= 0.2:
        category = "OCCASIONAL"
    elif value <= 0.4:
        category = "LIGHT_SPENDER"
    elif value <= 0.6:
        category = "REGULAR"
    elif value <= 0.8:
        category = "ENTHUSIAST"
    else:
        category = "SUPER_FAN"
    return ([{"category": category}], [source_value])


def create_enrichment_nodes(session, constraints, verbose=False):
    """
    Creates enrichment categorical nodes.

    Args:
        session (neo4j.Session): Neo4j session object.
        constraints (dict): Constraints for data validation.
        verbose (bool): Whether to print verbose output.
    """
    success_count = 0
    failed_instances = []

    node_constraints = constraints["nodes"]

    # Get categories from constraints
    enrichment_node_categories = {
        "TotalPopulation": {
            "level": node_constraints["TotalPopulation"]["properties"]["level"]["enum"]
        },
        "PopulationGrowth": {
            "growth_rate": node_constraints["PopulationGrowth"]["properties"][
                "growth_rate"
            ]["enum"]
        },
        "AgeAverage": {
            "group": node_constraints["AgeAverage"]["properties"]["group"]["enum"]
        },
        "AgeGroup": {
            "group": node_constraints["AgeGroup"]["properties"]["group"]["enum"],
            "representation": node_constraints["AgeGroup"]["properties"][
                "representation"
            ]["enum"],
        },
        "WealthIndex": {
            "category": node_constraints["WealthIndex"]["properties"]["category"][
                "enum"
            ]
        },
        "EducationLevel": {
            "level": node_constraints["EducationLevel"]["properties"]["level"]["enum"],
            "representation": node_constraints["EducationLevel"]["properties"][
                "representation"
            ]["enum"],
        },
        "CrimeIndex": {
            "category": node_constraints["CrimeIndex"]["properties"]["category"]["enum"]
        },
        "FastFoodSpendingIndex": {
            "category": node_constraints["FastFoodSpendingIndex"]["properties"][
                "category"
            ]["enum"],
        },
    }

    # Create nodes for each enrichment type and its properties
    for node_type, enum_properties in enrichment_node_categories.items():
        if verbose:
            print(f"Creating {node_type} nodes...")
        # Get all possible combinations of enum values
        property_names = list(enum_properties.keys())
        category_combinations = product(
            *[enum_properties[prop] for prop in property_names]
        )

        # Create node for each combination
        for combination in category_combinations:
            if verbose:
                print(f"Creating {node_type} node with properties {combination}...")
            properties = dict(zip(property_names, combination))
            try:
                create_node(
                    session,
                    node_type,
                    properties,
                    match_keys=list(properties.keys()),
                    verbose=verbose,
                )

                create_node_index(session, node_type, property_names, verbose=verbose)
                success_count += 1
            except Exception as e:
                failed_instances.append(
                    {"node_type": node_type, "properties": properties, "error": str(e)}
                )

    print(f"Enrichment nodes created: {success_count}")
    if failed_instances:
        print("\nFailures:")
        for failure in failed_instances:
            print(failure)


def create_enrichment_relationships(session, df, verbose=False):
    """
    Creates HAS_ENRICHMENT relationships between BlockGroups and enrichment nodes.

    Args:
        session (neo4j.Session): Neo4j session object.
        df (pd.DataFrame): DataFrame containing BlockGroup data.
        verbose (bool): Whether to print verbose output.
    """
    success_count = 0
    failed_instances = []

    for _, row in df.iterrows():
        try:
            # Create ct_block_group from tractce and blkgrpce
            ct_block_group = str(int(row["tractce"])) + str(row["blkgrpce"])

            # Create relationships for each enrichment type
            enrichments = {
                "TotalPopulation": determine_population_level(row["totpop_cy"]),
                "PopulationGrowth": determine_growth_rate(row["popgrwcyfy"]),
                "AgeAverage": determine_age_average(row["avg_age"]),
                "AgeGroup": determine_age_group_representations(row),
                "WealthIndex": determine_wealth_category(row["normalized_wlthindxcy"]),
                "EducationLevel": determine_education_level(row),
                "CrimeIndex": determine_crime_level(row["crmcytotc"]),
                "FastFoodSpendingIndex": determine_spending_level(
                    row["normalized_fastfoodspending"]
                ),
            }
            for enrichment_type, (
                category_combinations,
                source_values,
            ) in enrichments.items():
                for combination, source_value in zip(
                    category_combinations, source_values
                ):
                    try:
                        create_relationship(
                            session,
                            start_label="BlockGroup",
                            start_props={"ct_block_group": ct_block_group},
                            start_match_keys=["ct_block_group"],
                            end_label=enrichment_type,
                            end_props=combination,
                            end_match_keys=list(combination.keys()),
                            rel_type="HAS_ENRICHMENT",
                            rel_properties={"source_value": str(source_value)},
                            verbose=verbose,
                        )
                        success_count += 1
                    except Exception as e:
                        failed_instances.append(
                            {
                                "block_group": ct_block_group,
                                "enrichment": enrichment_type,
                                "error": str(e),
                            }
                        )

        except Exception as e:
            failed_instances.append(
                {
                    "block_group": (
                        ct_block_group if "ct_block_group" in locals() else "unknown"
                    ),
                    "error": str(e),
                }
            )

    print(f"Enrichment relationships created: {success_count}")
    print(f"Failed relationships: {len(failed_instances)}")

    if failed_instances:
        print("\nFailures:")
        for failure in failed_instances:
            print(failure)


def populate_geoenrichments(driver, constraints, cleanup=False, verbose=False):
    """
    Main function to populate geoenrichment data.

    Args:
        driver (neo4j.Driver): Neo4j driver object.
        constraints (dict): Constraints for data validation.
        cleanup (bool): Cleanup existing enrichment nodes and relationships
        verbose (bool): Whether to print verbose output.
    """
    enrichment_labels = [
        "TotalPopulation",
        "PopulationGrowth",
        "AgeAverage",
        "AgeGroup",
        "WealthIndex",
        "EducationLevel",
        "CrimeIndex",
        "FastFoodSpendingIndex",
    ]
    if cleanup:
        cleanup_neo4j(
            driver,
            constraints,
            nodes=enrichment_labels,
        )

    # Get enrichment data
    df = pd.read_csv(f"{get_root_dir()}/data/bgs_sd_imp.csv")

    # Prepare the data
    df = prepare_data(df)

    with driver.session() as session:
        print("\nCreating enrichment nodes...")
        create_enrichment_nodes(session, constraints, verbose=verbose)

        print("\nCreating enrichment relationships...")
        create_enrichment_relationships(session, df, verbose=verbose)
