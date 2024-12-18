# Helper functions for working with geometry data
from shapely.geometry import Point


def get_minimum_enclosing_circle(polygon):
    """
    Calculate centroid and minimum enclosing circle radius from a Shapely geometry.

    Args:
        polygon: A Shapely polygon or multipolygon geometry

    Returns:
        Tuple: (centroid, radius) where centroid is a Point and radius is an integer in meters.
    """
    # Get centroid
    centroid = polygon.centroid

    # Calculate minimum enclosing circle radius by checking all boundary points
    if hasattr(polygon, "geoms"):  # MultiPolygon
        distances = []
        for geom in polygon.geoms:
            distances.extend(
                [centroid.distance(Point(p)) for p in geom.exterior.coords]
            )
    else:  # Single Polygon
        distances = [centroid.distance(Point(p)) for p in polygon.exterior.coords]

    radius = max(distances)

    # Convert radius to meters (assuming coordinates are in WGS84)
    radius_meters = int(radius * 111000)  # Rough conversion from degrees to meters

    return centroid, radius_meters
