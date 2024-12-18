# This module contains functions to populate the BlockGroup nodes in the knowledge graph.
import geopandas as gpd

from biz_opps.neo4j.cleanup import cleanup_neo4j
from biz_opps.utils.postgres import get_sqlalchemy_engine
from biz_opps.neo4j.construction import create_node
from biz_opps.neo4j.validation import validate_data
from biz_opps.neo4j.spatial import (
    init_wkt_layer,
    add_node_to_spatial_layer,
)


def get_block_group_data(sql_filter: str = ""):
    """
    Fetches the BlockGroup data from the PostgreSQL database using GeoPandas.

    Args:
        sql_filter (str): An optional SQL filter to apply to the query.

    Returns:
        pd.DataFrame: DataFrame containing the BlockGroup data.
    """
    sql_engine = get_sqlalchemy_engine()
    try:
        sql = f"SELECT ctblockgroup, tract, blockgroup, objectid, wkb_geometry FROM sandag_layer_census_block_groups {sql_filter}"
        gdf = gpd.read_postgis(
            sql, sql_engine, geom_col="wkb_geometry", crs="epsg:2230"
        )
        return gdf.to_crs(epsg=4326)
    except Exception as e:
        print(f"Error fetching BlockGroup data from PostgreSQL: {e}")
        return None
    finally:
        sql_engine.dispose()


def create_block_group_nodes(session, constraints, gdf, verbose=False):
    """
    Create BlockGroup nodes with spatial geometries.

    Args:
        session: Neo4j session
        constraints: Constraints schema
        gdf: GeoDataFrame containing BlockGroup data
        verbose (bool): Whether to print verbose output
    """
    # Initialize spatial layer
    init_wkt_layer(session, "block_group_layer", geometry_property_name="wkt")

    success_count = 0
    failed_instances = []

    for _, row in gdf.iterrows():
        if verbose:
            print(f"Creating BlockGroup: {row.ctblockgroup}...")

        try:
            properties = {
                "ct_block_group": str(row.ctblockgroup),
                "census_tract": str(row.tract),
                "block_group": str(row.blockgroup),
                "object_id": str(row.objectid),
                "wkt": row.wkb_geometry.wkt,
            }

            node_data = {"label": "BlockGroup", "properties": properties}

            # Validate data
            validate_data(node_data, constraints)

            # Create node
            block_group_node = create_node(
                session,
                "BlockGroup",
                properties,
                match_keys=["ct_block_group"],
                verbose=verbose,
            )

            if not block_group_node:
                raise Exception("Failed to create BlockGroup node")

            # Add geometry to spatial layer and link to node
            if not add_node_to_spatial_layer(
                session, block_group_node, "block_group_layer"
            ):
                raise Exception("Failed to add geometry to BlockGroup node")

            success_count += 1

        except Exception as e:
            failed_instances.append(
                {
                    "ct_block_group": properties.get("ct_block_group", ""),
                    "error": str(e),
                }
            )
            print(f"Error processing BlockGroup: {str(e)}")

    # Summary of results
    print(f"BlockGroup Nodes Created or Updated: {success_count}")
    print(f"BlockGroup Nodes Failed: {len(failed_instances)}")

    # Optionally print details of failures
    if failed_instances:
        print("\nBlockGroup Failures:")
        for failure in failed_instances:
            print(failure)


def populate_block_groups(
    driver,
    constraints,
    cleanup=False,
    verbose=False,
    sql_filter: str = "",
):
    """
    Main function to populate the BlockGroup nodes in Neo4j.
    Connects to PostgreSQL to retrieve data, then populates Neo4j.

    Args:
        driver: Neo4j driver
        constraints: Constraints object
        cleanup: Cleanup existing BlockGroup nodes and spatial layer
        verbose: Print verbose output
        sql_filter (str): An optional SQL filter to apply to the query.
    """
    if cleanup:
        cleanup_neo4j(
            driver,
            constraints,
            nodes=["BlockGroup"],
            spatial_layers=["block_group_layer"],
        )

    print("Populating BlockGroups...")
    # Fetch BlockGroup data from PostgreSQL
    df = get_block_group_data(sql_filter)

    # Populate Neo4j with BlockGroup nodes
    with driver.session() as session:
        create_block_group_nodes(session, constraints, df, verbose=verbose)
