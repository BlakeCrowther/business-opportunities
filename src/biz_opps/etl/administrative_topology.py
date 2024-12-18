# This module contains functions to populate the City and Neighborhood nodes in the knowledge graph.
import os
import pandas as pd
import geopandas as gpd
from shapely import wkt

from biz_opps.neo4j.cleanup import cleanup_neo4j
from biz_opps.utils.postgres import get_sqlalchemy_engine
from biz_opps.neo4j.construction import (
    create_node,
    create_relationship,
)
from biz_opps.neo4j.constraints import load_constraints
from biz_opps.neo4j.validation import validate_data
from biz_opps.neo4j.spatial import (
    add_node_to_spatial_layer,
    init_wkt_layer,
)
from biz_opps.utils.file import get_root_dir


def get_administrative_topology_data(sql_filter: str = ""):
    """
    Fetches the administrative topology data from the PostgreSQL database and the zipcode csv.

    Args:
        sql_filter (str): An optional SQL filter to apply to the city and neighborhood queries.

    Returns:
        tuple: A tuple containing three pandas DataFrames: zipcode_gdf, city_df, and neighborhood_df.
    """
    sql_engine = get_sqlalchemy_engine()

    # Read zipcode data
    zipcode_gdf = gpd.read_file(os.path.join(get_root_dir(), "data", "zipcodes.csv"))

    # Read city data
    city_query = f"""
    SELECT 
        id,
        city,
        state_name,
        county,
        is_unincorporated_place,
        zipcodes,
        neighboring_cities,
        neighboring_unincorporated_places,
        nearby_unincorporated_places,
        neighborhoods,
        nearby_cities
    FROM city_neighborhoods
    {sql_filter}
    """
    city_df = pd.read_sql(city_query, sql_engine)

    # Read neighborhood data
    neighborhood_query = f"""
    SELECT 
        id,
        community,
        zipcodes, 
        neighboring_cities, 
        neighboring_unincorporated_places,
        nearby_unincorporated_places,
        neighboring_communities,
        nearby_communities, 
        nearby_cities
    FROM community_neighborhoods
    {sql_filter}
    """
    neighborhood_df = pd.read_sql(neighborhood_query, sql_engine)
    return zipcode_gdf, city_df, neighborhood_df


def create_zipcode_nodes(
    session, constraints, zipcode_df, city_df, neighborhood_df, verbose=False
):
    """
    Creates Zipcode nodes with spatial geometries(if they exist in the DataFrame).

    Args:
        session: Neo4j session
        constraints: Constraints object
        zipcode_df: DataFrame containing the Zipcode data
        city_df: DataFrame containing the City data
        neighborhood_df: DataFrame containing the Neighborhood data
        verbose (bool): Whether to print verbose output
    """
    # Initialize spatial layer
    init_wkt_layer(session, "zipcode_layer", geometry_property_name="wkt")

    success_count = 0
    failed_instances = []

    # Combine city and neighborhood zipcodes
    combined_zipcodes = (
        set(city_df["zipcodes"].explode().astype(str).unique())
        | set(neighborhood_df["zipcodes"].explode().astype(str).unique())
        | set(zipcode_df["ZIP"].astype(str).unique())
    )

    for zipcode in combined_zipcodes:
        if verbose:
            print(f"Creating Zipcode: {zipcode}...")

        properties = {"zipcode_number": str(zipcode)}

        # Add WKT if available in zipcode_df
        zipcode_row = zipcode_df[zipcode_df["ZIP"].astype(str) == str(zipcode)]
        if not zipcode_row.empty and "the_geom" in zipcode_row.columns:
            properties["wkt"] = zipcode_row.iloc[0]["the_geom"]

        node_data = {"label": "Zipcode", "properties": properties}

        try:
            validate_data(node_data, constraints)
            zipcode_node = create_node(
                session,
                "Zipcode",
                properties,
                match_keys=["zipcode_number"],
                verbose=verbose,
            )

            if not zipcode_node:
                raise Exception("Failed to create Zipcode node")

            # Add to spatial layer if WKT exists
            if "wkt" in properties:
                if not add_node_to_spatial_layer(
                    session, zipcode_node, "zipcode_layer"
                ):
                    raise Exception("Failed to add Zipcode node to spatial layer")

            success_count += 1

        except Exception as e:
            failed_instances.append(
                {"zipcode": properties.get("zipcode_number", ""), "error": str(e)}
            )

    print(f"Zipcode Nodes Created or Updated: {success_count}")
    print(f"Zipcode Nodes Failed: {len(failed_instances)}")

    if failed_instances:
        print("\nZipcode Failures:")
        for failure in failed_instances:
            print(failure)


def create_city_nodes(session, constraints, df, verbose=False):
    """
    Creates City nodes (without spatial geometries).

    Args:
        session: Neo4j session
        constraints: Constraints object
        df: DataFrame containing the City data
        verbose (bool): Whether to print verbose output
    """
    success_count = 0
    failed_instances = []

    for _, row in df.iterrows():
        if verbose:
            print(f"Creating City: {row['city']}...")

        properties = {
            "city_id": str(row["id"]),
            "city_name": str(row["city"]),
            "state_name": str(row["state_name"]),
            "county": str(row["county"]),
            "is_unincorporated": bool(row["is_unincorporated_place"]),
        }

        node_data = {"label": "City", "properties": properties}

        try:
            validate_data(node_data, constraints)

            if not create_node(
                session, "City", properties, match_keys=["city_id"], verbose=verbose
            ):
                raise Exception("Failed to create City node")
            success_count += 1
        except Exception as e:
            failed_instances.append(
                {"city": properties.get("city_name", ""), "error": str(e)}
            )

    print(f"City Nodes Created or Updated: {success_count}")
    print(f"City Nodes Failed: {len(failed_instances)}")

    if failed_instances:
        print("\nCity Failures:")
        for failure in failed_instances:
            print(failure)


def create_neighborhood_nodes(session, constraints, df, verbose=False):
    """
    Creates Neighborhood nodes (without spatial geometries).

    Args:
        session: Neo4j session
        constraints: Constraints object
        df: DataFrame containing the Neighborhood data
        verbose (bool): Whether to print verbose output
    """
    success_count = 0
    failed_instances = []

    for _, row in df.iterrows():
        if verbose:
            print(f"Creating Neighborhood: {row['community']}...")

        properties = {
            "neighborhood_id": str(row["id"]),
            "neighborhood_name": row["community"],
        }

        node_data = {"label": "Neighborhood", "properties": properties}

        try:
            validate_data(node_data, constraints)
            if not create_node(
                session,
                "Neighborhood",
                properties,
                match_keys=["neighborhood_id"],
                verbose=verbose,
            ):
                raise Exception("Failed to create Neighborhood node")
            success_count += 1
        except Exception as e:
            failed_instances.append(
                {
                    "neighborhood": properties.get("neighborhood_name", ""),
                    "error": str(e),
                }
            )

    print(f"Neighborhood Nodes Created or Updated: {success_count}")
    print(f"Neighborhood Nodes Failed: {len(failed_instances)}")

    if failed_instances:
        print("\nNeighborhood Failures:")
        for failure in failed_instances:
            print(failure)


def create_city_relationships(session, df, verbose=False):
    """
    Creates relationships for City nodes including incorporated and unincorporated places.
    """
    success_count = 0
    failed_instances = []

    for _, row in df.iterrows():
        if verbose:
            print(f"Creating relationships for City: {row['city']}...")

        # Create CITY->IS_WITHIN->ZIPCODE relationships
        if isinstance(row["zipcodes"], list) and row["zipcodes"]:
            for zipcode in row["zipcodes"]:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="Zipcode",
                        end_props={"zipcode_number": str(zipcode)},
                        end_match_keys=["zipcode_number"],
                        rel_type="IS_WITHIN",
                        rel_properties={
                            "containment_type": (
                                "Full" if len(row["zipcodes"]) == 1 else "Partial"
                            )
                        },
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {"city": row["city"], "zipcode": zipcode, "error": str(e)}
                    )

        # Create CITY->HAS_NEIGHBORHOOD->NEIGHBORHOOD relationships
        neighborhoods = row["neighborhoods"]
        if isinstance(neighborhoods, list) and neighborhoods:
            for neighborhood_name in neighborhoods:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="Neighborhood",
                        end_props={"neighborhood_name": str(neighborhood_name)},
                        end_match_keys=["neighborhood_name"],
                        rel_type="HAS_NEIGHBORHOOD",
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "city": row["city"],
                            "neighborhood": neighborhood_name,
                            "error": str(e),
                        }
                    )

        # Create CITY->HAS_NEIGHBOR->CITY relationships for incorporated cities
        neighboring_cities = row["neighboring_cities"]
        if isinstance(neighboring_cities, list) and neighboring_cities:
            for neighboring_city in neighboring_cities:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="City",
                        end_props={"city_name": str(neighboring_city)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEIGHBOR",
                        rel_properties={"neighbor_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "city": row["city"],
                            "neighboring_city": neighboring_city,
                            "error": str(e),
                        }
                    )

        # Create CITY->HAS_NEIGHBOR->CITY relationships for unincorporated places
        neighboring_unincorp = row["neighboring_unincorporated_places"]
        if isinstance(neighboring_unincorp, list) and neighboring_unincorp:
            for unincorp_place in neighboring_unincorp:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="City",
                        end_props={"city_name": str(unincorp_place)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEIGHBOR",
                        rel_properties={"neighbor_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "city": row["city"],
                            "neighboring_unincorporated": unincorp_place,
                            "error": str(e),
                        }
                    )

        # Create CITY->HAS_NEARBY->CITY relationships for incorporated cities
        nearby_cities = row["nearby_cities"]
        if isinstance(nearby_cities, list) and nearby_cities:
            for nearby_city in nearby_cities:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="City",
                        end_props={"city_name": str(nearby_city)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEARBY",
                        rel_properties={"nearby_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "city": row["city"],
                            "nearby_city": nearby_city,
                            "error": str(e),
                        }
                    )

        # Create CITY->HAS_NEARBY->CITY relationships for unincorporated places
        nearby_unincorp = row["nearby_unincorporated_places"]
        if isinstance(nearby_unincorp, list) and nearby_unincorp:
            for unincorp_place in nearby_unincorp:
                try:
                    create_relationship(
                        session,
                        start_label="City",
                        start_props={"city_id": str(row["id"])},
                        start_match_keys=["city_id"],
                        end_label="City",
                        end_props={"city_name": str(unincorp_place)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEARBY",
                        rel_properties={"nearby_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "city": row["city"],
                            "nearby_unincorporated": unincorp_place,
                            "error": str(e),
                        }
                    )

    print(f"City relationships created: {success_count}")
    print(f"City relationships failed: {len(failed_instances)}")

    if failed_instances:
        print("\nCity Relationship Failures:")
        for failure in failed_instances:
            print(failure)


def create_neighborhood_relationships(session, df, verbose=False):
    """
    Creates relationships for Neighborhood nodes.
    """
    success_count = 0
    failed_instances = []

    for _, row in df.iterrows():
        if verbose:
            print(f"Creating relationships for Neighborhood: {row['community']}...")

        # Create NEIGHBORHOOD->IS_WITHIN->ZIPCODE relationships
        if isinstance(row["zipcodes"], list) and row["zipcodes"]:
            for zipcode in row["zipcodes"]:
                try:
                    create_relationship(
                        session,
                        start_label="Neighborhood",
                        start_props={"neighborhood_id": str(row["id"])},
                        start_match_keys=["neighborhood_id"],
                        end_label="Zipcode",
                        end_props={"zipcode_number": str(zipcode)},
                        end_match_keys=["zipcode_number"],
                        rel_type="IS_WITHIN",
                        rel_properties={
                            "containment_type": (
                                "Full" if len(row["zipcodes"]) == 1 else "Partial"
                            )
                        },
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "neighborhood": row["community"],
                            "zipcode": zipcode,
                            "error": str(e),
                        }
                    )

        # Create NEIGHBORHOOD->HAS_NEIGHBOR->NEIGHBORHOOD relationships
        neighboring_communities = row["neighboring_communities"]
        if isinstance(neighboring_communities, list) and neighboring_communities:
            for neighboring_community in neighboring_communities:
                try:
                    create_relationship(
                        session,
                        start_label="Neighborhood",
                        start_props={"neighborhood_id": str(row["id"])},
                        start_match_keys=["neighborhood_id"],
                        end_label="Neighborhood",
                        end_props={"neighborhood_name": str(neighboring_community)},
                        end_match_keys=["neighborhood_name"],
                        rel_type="HAS_NEIGHBOR",
                        rel_properties={"neighbor_type": "Neighborhood"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "neighborhood": row["community"],
                            "neighboring_community": neighboring_community,
                            "error": str(e),
                        }
                    )

        # Create NEIGHBORHOOD->HAS_NEIGHBOR->CITY relationships
        neighboring_cities = row["neighboring_cities"]
        if isinstance(neighboring_cities, list) and neighboring_cities:
            for neighboring_city in neighboring_cities:
                try:
                    create_relationship(
                        session,
                        start_label="Neighborhood",
                        start_props={"neighborhood_id": str(row["id"])},
                        start_match_keys=["neighborhood_id"],
                        end_label="City",
                        end_props={"city_name": str(neighboring_city)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEIGHBOR",
                        rel_properties={"neighbor_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "neighborhood": row["community"],
                            "neighboring_city": neighboring_city,
                            "error": str(e),
                        }
                    )

        # Create NEIGHBORHOOD->HAS_NEARBY->NEIGHBORHOOD relationships
        nearby_communities = row["nearby_communities"]
        if isinstance(nearby_communities, list) and nearby_communities:
            for nearby_community in nearby_communities:
                try:
                    create_relationship(
                        session,
                        start_label="Neighborhood",
                        start_props={"neighborhood_id": str(row["id"])},
                        start_match_keys=["neighborhood_id"],
                        end_label="Neighborhood",
                        end_props={"neighborhood_name": str(nearby_community)},
                        end_match_keys=["neighborhood_name"],
                        rel_type="HAS_NEARBY",
                        rel_properties={"nearby_type": "Neighborhood"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "neighborhood": row["community"],
                            "nearby_community": nearby_community,
                            "error": str(e),
                        }
                    )

        # Create NEIGHBORHOOD->HAS_NEARBY->CITY relationships
        nearby_cities = row["nearby_cities"]
        if isinstance(nearby_cities, list) and nearby_cities:
            for nearby_city in nearby_cities:
                try:
                    create_relationship(
                        session,
                        start_label="Neighborhood",
                        start_props={"neighborhood_id": str(row["id"])},
                        start_match_keys=["neighborhood_id"],
                        end_label="City",
                        end_props={"city_name": str(nearby_city)},
                        end_match_keys=["city_name"],
                        rel_type="HAS_NEARBY",
                        rel_properties={"nearby_type": "City"},
                        verbose=verbose,
                    )
                    success_count += 1
                except Exception as e:
                    failed_instances.append(
                        {
                            "neighborhood": row["community"],
                            "nearby_city": nearby_city,
                            "error": str(e),
                        }
                    )

    print(f"Neighborhood relationships created: {success_count}")
    print(f"Failed relationships: {len(failed_instances)}")

    if failed_instances:
        print("\nNeighborhood Relationship Failures:")
        for failure in failed_instances:
            print(failure)


def create_block_group_zipcode_intersection(session, verbose=False):
    """
    Creates relationships between BlockGroups and Zipcodes based on spatial intersection.
    Uses Neo4j Spatial to find intersections and Shapely to calculate overlap ratios.

    Args:
        session: Neo4j session
        verbose (bool): Whether to print verbose output
    """
    success_count = 0
    failed_instances = []

    # Query to find intersections using spatial layers
    query = """
    MATCH (bg:BlockGroup)
    CALL spatial.intersects('zipcode_layer', bg.wkt) YIELD node
    WITH bg, node
    WHERE 'Zipcode' IN labels(node)
    WITH bg, node as zip
    RETURN bg.ct_block_group as block_group,
           bg.wkt as bg_wkt,
           zip.zipcode_number as zipcode,
           zip.wkt as z_wkt
    """

    try:
        print("Executing spatial intersection query...")
        result = session.run(query)
        print("Query executed, processing results...")

        # Process individual records
        for record in result:
            if verbose:
                print(
                    f"Processing intersection between BlockGroup {record['block_group']} and Zipcode {record['zipcode']}..."
                )

            try:
                bg_geom = wkt.loads(record["bg_wkt"])
                z_geom = wkt.loads(record["z_wkt"])

                intersection_area = bg_geom.intersection(z_geom).area
                bg_area = bg_geom.area
                overlap_ratio = intersection_area / bg_area

                if verbose:
                    print(f"Overlap ratio: {overlap_ratio}")

                create_relationship(
                    session,
                    start_label="BlockGroup",
                    start_props={"ct_block_group": record["block_group"]},
                    start_match_keys=["ct_block_group"],
                    end_label="Zipcode",
                    end_props={"zipcode_number": record["zipcode"]},
                    end_match_keys=["zipcode_number"],
                    rel_type="IS_WITHIN",
                    rel_properties={
                        "containment_type": (
                            "Full" if overlap_ratio > 0.95 else "Partial"
                        ),
                        "overlap_ratio": float(overlap_ratio),
                    },
                    verbose=verbose,
                )
                print("Relationship created successfully")
                success_count += 1

            except Exception as e:
                print(f"Error processing record: {str(e)}")
                failed_instances.append(
                    {
                        "block_group": record["block_group"],
                        "zipcode": record["zipcode"],
                        "error": str(e),
                    }
                )

    except Exception as e:
        print(f"Failed to execute spatial intersection query: {str(e)}")
        return False

    print(f"BlockGroup-Zipcode relationships created: {success_count}")
    print(f"Failed operations: {len(failed_instances)}")

    if failed_instances:
        print("\nBlockGroup-Zipcode Relationship Failures:")
        for failure in failed_instances:
            print(failure)

    return True


def populate_administrative_topology(
    driver,
    constraints,
    cleanup=False,
    verbose=False,
    sql_filter: str = "",
):
    try:
        if cleanup:
            cleanup_neo4j(
                driver,
                constraints,
                nodes=["Zipcode", "City", "Neighborhood"],
                spatial_layers=["zipcode_layer"],
            )

        # Get data
        zipcode_df, city_df, neighborhood_df = get_administrative_topology_data(
            sql_filter
        )

        # Create nodes and relationships
        with driver.session() as session:
            print("Creating Zipcode nodes...")
            create_zipcode_nodes(
                session,
                constraints,
                zipcode_df,
                city_df,
                neighborhood_df,
                verbose=verbose,
            )

            print("\nCreating City nodes...")
            create_city_nodes(session, constraints, city_df, verbose=verbose)

            print("\nCreating Neighborhood nodes...")
            create_neighborhood_nodes(
                session, constraints, neighborhood_df, verbose=verbose
            )

            print("\nCreating City relationships...")
            create_city_relationships(session, city_df, verbose=verbose)

            print("\nCreating Neighborhood relationships...")
            create_neighborhood_relationships(session, neighborhood_df, verbose=verbose)

            print("\nCreating BlockGroup-Zipcode relationships...")
            create_block_group_zipcode_intersection(session, verbose=verbose)

    except Exception as error:
        print(f"Error executing Administrative Topology: {error}")
