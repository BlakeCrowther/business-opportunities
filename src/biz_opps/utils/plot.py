import matplotlib.pyplot as plt
import folium


def create_folium_map():
    """
    Create a folium map centered on San Diego.
    """
    return folium.Map(location=[32.7157, -117.1611], zoom_start=10)


def show_folium_map(folium_map):
    """
    Show a folium map in the browser.
    """
    folium_map.show_in_browser()


def plot_geometry(geometry, folium_map=None, color="blue", alpha=0.5):
    """
    Plot geometry on a folium map.

    Args:
        geometry: The geometry to plot (Polygon or MultiPolygon)
        folium_map: Optional folium Map object. If None, creates new map centered on San Diego
        color: Color of the geometry
        alpha: Transparency of the fill (0-1)

    Returns:
        folium.Map object
    """
    if folium_map is None:
        folium_map = create_folium_map()
    if geometry.geom_type == "Polygon":
        folium.Polygon(
            locations=[[y, x] for x, y in zip(*geometry.exterior.xy)],
            color=color,
            fill=True,
            fill_opacity=alpha,
        ).add_to(folium_map)
    elif geometry.geom_type == "MultiPolygon":
        for polygon in geometry.geoms:
            folium.Polygon(
                locations=[[y, x] for x, y in zip(*polygon.exterior.xy)],
                color=color,
                fill=True,
                fill_opacity=alpha,
            ).add_to(folium_map)

    return folium_map


def plot_points(points, folium_map=None, color="red", radius=5):
    """
    Plot points on a folium map.

    Args:
        points: List of Point objects
        folium_map: Optional folium Map object. If None, creates new map centered on San Diego
        color: Color of the points
        radius: Size of the point markers

    Returns:
        folium.Map object
    """
    if folium_map is None:
        folium_map = create_folium_map()
    for point in points:
        folium.CircleMarker(
            location=[point.y, point.x], radius=radius, color=color, fill=True
        ).add_to(folium_map)

    return folium_map


def plot_circle(center_coords, radius, folium_map=None):
    """
    Plot a circle on a folium map.
    """
    if folium_map is None:
        folium_map = create_folium_map()
    folium.Circle(location=center_coords, radius=radius, color="red", fill=True).add_to(
        folium_map
    )
    return folium_map
