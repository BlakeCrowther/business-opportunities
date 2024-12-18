# This module contains functions to populate the Business nodes in the knowledge graph.
from shapely import wkt, geometry
from google.maps import places_v1
from google.auth import default
from google.maps.places_v1 import types
from google.type import latlng_pb2

from biz_opps.neo4j.cleanup import cleanup_neo4j
from biz_opps.neo4j.construction import (
    create_node,
    create_relationship,
)
from biz_opps.neo4j.validation import validate_data
from biz_opps.utils.geometry import get_minimum_enclosing_circle
from biz_opps.neo4j.spatial import (
    init_point_layer,
    add_node_to_spatial_layer,
)


def get_block_group_geometries(session, neo4j_filter: str = ""):
    """
    Fetch all BlockGroup geometries from Neo4j.

    Args:
        session: Neo4j session
        neo4j_filter: Optional Neo4j filter string

    Returns:
        dict mapping ct_block_group to (polygon: shapely.Polygon, center: shapely.Point, radius: int)
    """

    query = f"""
    MATCH (bg:BlockGroup)
    {neo4j_filter}
    RETURN bg.ct_block_group as ct_block_group, bg.wkt as wkt
    """

    result = session.run(query)
    geometries = {}

    for record in result:
        block_group_id = record["ct_block_group"]
        bg_wkt = record["wkt"]

        # Convert WKT to Shapely geometry
        polygon = wkt.loads(bg_wkt)

        # Calculate center and radius
        center, radius = get_minimum_enclosing_circle(polygon)

        geometries[block_group_id] = {
            "polygon": polygon,
            "center": center,
            "radius": radius,
        }

    return geometries


def get_business_zipcode(session, ct_block_group, longitude, latitude):
    """
    Fetch the Zipcode ID and geometry that contains the given latitude and longitude.

    Args:
        session: Neo4j session
        ct_block_group: CT BlockGroup ID
        longitude: Longitude of the business
        latitude: Latitude of the business

    Returns:
        Zipcode ID or None
    """
    query = f"""
    MATCH (bg:BlockGroup)-[:IS_WITHIN]->(z:Zipcode)
    WHERE bg.ct_block_group = '{ct_block_group}'
    RETURN z.zipcode_number as zipcode_number, z.wkt as wkt
    """
    try:
        result = session.run(query)

        for record in result:
            zipcode_number = record["zipcode_number"]
            zipcode_wkt = record["wkt"]
            polygon = wkt.loads(zipcode_wkt)

            # Check if the point is within the polygon
            point = geometry.Point(longitude, latitude)
            if polygon.contains(point):
                return zipcode_number

        return None

    except Exception as e:
        print(f"Error getting business zipcode: {e}")

    return None


async def query_nearby_places(places_client, location, radius, business_types):
    """
    Query Google Places API for nearby businesses.

    Args:
        places_client: Google places_v1 API client instance
        location: (latitude, longitude) tuple
        radius: Search radius in meters
        business_types: List of business types to search for

    Returns:
        List of business data dictionaries

    Note: This function should probably be used to query one business "category" at a time since it
    returns primary types that may be different than the requested category. e.g. "bakery" may return
    "donut shop" and "bakery" primary types.
    """
    try:
        # Create the location restriction
        location_restriction = types.SearchNearbyRequest.LocationRestriction(
            circle=types.Circle(
                center=latlng_pb2.LatLng(
                    latitude=float(location[0]),
                    longitude=float(location[1]),
                ),
                radius=radius,
            )
        )

        # Create the request
        request = types.SearchNearbyRequest(
            location_restriction=location_restriction,
            included_primary_types=business_types,
            language_code="en",
            # max_result_count=20,
        )

        # Add headers for field mask
        metadata = [
            (
                "x-goog-fieldmask",
                "places.id,places.displayName,places.primaryType,places.location,places.formattedAddress,places.rating,places.priceLevel",
            )
        ]

        # Make the request
        response = await places_client.search_nearby(request=request, metadata=metadata)
        places = response.places

        return places

    except Exception as e:
        print(f"Error querying Google Places API: {e}")
        return []


def create_business_nodes(
    session, bg_businesses, ct_block_group, constraints, verbose=False
):
    """
    Creates Business nodes and LOCATED_IN relationships.

    Args:
        session: Neo4j session
        bg_businesses: Dict mapping business type to list of businesses for a BlockGroup
        ct_block_group: CT BlockGroup ID
        constraints: Constraints schema
        verbose: Print verbose output

    Returns:
        Tuple of (business_count, failed_instances)
    """
    failed_instances = []
    business_count = 0

    for business_type, businesses in bg_businesses.items():
        for business in businesses:
            try:
                properties = {}

                # Required fields
                properties["business_id"] = business.id
                properties["business_name"] = business.display_name.text
                properties["business_type"] = business_type
                properties["latitude"] = business.location.latitude
                properties["longitude"] = business.location.longitude

                # Optional fields
                if hasattr(business, "formatted_address"):
                    properties["address"] = business.formatted_address
                if hasattr(business, "rating"):
                    properties["rating"] = business.rating
                if hasattr(business, "price_level"):
                    properties["price_level"] = business.price_level

                node_data = {"label": "Business", "properties": properties}

                # Validate data
                validate_data(node_data, constraints)

                # Create node
                business_node = create_node(
                    session,
                    "Business",
                    properties,
                    match_keys=["business_id"],
                    verbose=verbose,
                )

                if not business_node:
                    raise Exception("Failed to create Business node")

                # Add to spatial layer
                if not add_node_to_spatial_layer(
                    session, business_node, "business_layer"
                ):
                    raise Exception("Failed to add Business node to spatial layer")

                # Create BUSINESS->LOCATED_IN->BLOCKGROUP relationship
                create_relationship(
                    session,
                    start_label="Business",
                    start_props={"business_id": business.id},
                    start_match_keys=["business_id"],
                    end_label="BlockGroup",
                    end_props={"ct_block_group": ct_block_group},
                    end_match_keys=["ct_block_group"],
                    rel_type="LOCATED_IN",
                    verbose=verbose,
                )

                # Create BUSINESS->LOCATED_IN->Zipcode relationship
                zipcode = get_business_zipcode(
                    session,
                    ct_block_group,
                    properties["longitude"],
                    properties["latitude"],
                )
                if zipcode:
                    create_relationship(
                        session,
                        start_label="Business",
                        start_props={"business_id": business.id},
                        start_match_keys=["business_id"],
                        end_label="Zipcode",
                        end_props={"zipcode_number": zipcode},
                        end_match_keys=["zipcode_number"],
                        rel_type="LOCATED_IN",
                        verbose=verbose,
                    )
                else:
                    print(f"No zipcode found for {business.display_name.text}")
                business_count += 1
            except Exception as e:
                failed_instances.append(
                    {"business": business.display_name.text, "error": str(e)}
                )
                if verbose:
                    print(f"Error processing Business: {str(e)}")

    if verbose:
        print(f"Business Nodes Created: {business_count} for {ct_block_group}")
        print(f"Business Nodes Failed: {len(failed_instances)} for {ct_block_group}")

    return business_count, failed_instances


async def populate_businesses(
    driver, constraints, cleanup=False, verbose=False, neo4j_filter: str = ""
):
    """
    Main function to populate Business nodes.

    Args:
        driver: Neo4j driver
        constraints: Constraints schema
        cleanup: Cleanup existing Business nodes and spatial layer
        verbose: Print verbose output
        neo4j_filter: Optional Neo4j filter string
    """
    if cleanup:
        cleanup_neo4j(
            driver,
            constraints,
            nodes=["Business"],
            spatial_layers=["business_layer"],
        )

    print("Populating Businesses...")

    # Initialize Google Maps client
    credentials, project = default()
    places_client = places_v1.PlacesAsyncClient(credentials=credentials)

    google_business_types = {
        # "farmers_market": ["farmers_market"],
        "grocery_store": ["grocery_store"],
        "fast_food_restaurant": ["fast_food_restaurant"],
        "bakery": ["bakery"],
    }

    with driver.session() as session:
        # Initialize new point layer
        print("Initializing point layer...")
        init_point_layer(session, "business_layer")

        # Get all BlockGroup geometries
        block_groups = get_block_group_geometries(session, neo4j_filter)

        business_count = 0
        failed_instances = []

        # Process each BlockGroup
        for ct_block_group, geom_data in block_groups.items():
            polygon = geom_data["polygon"]
            center = geom_data["center"]
            radius = geom_data["radius"]

            # Get coordinates for API query
            center_coords = (center.y, center.x)
            if verbose:
                print(f"Querying businesses near {ct_block_group}")

            businesses = {
                business_type: [] for business_type in google_business_types.keys()
            }

            # Query nearby places for each business type
            for business_type, gtypes in google_business_types.items():
                places = await query_nearby_places(
                    places_client, center_coords, radius, gtypes
                )

                # Filter to businesses within BlockGroup
                businesses_in_block_group = [
                    place
                    for place in places
                    if polygon.contains(
                        geometry.Point(
                            place.location.longitude, place.location.latitude
                        )
                    )
                ]

                # Add to list of businesses for this type
                businesses[business_type].extend(businesses_in_block_group)

            # Create nodes with spatial data
            business_count, failed_instances = create_business_nodes(
                session,
                businesses,
                ct_block_group,
                constraints,
                verbose=verbose,
            )
            business_count += business_count
            failed_instances.extend(failed_instances)

        print(f"Business Nodes Created: {business_count}")
        print(f"Business Nodes Failed: {len(failed_instances)}")

        if failed_instances:
            print("\nBusiness Nodes Failed:")
            for failure in failed_instances:
                print(failure)
