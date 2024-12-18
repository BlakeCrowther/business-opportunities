from typing import Dict, List, Any
import folium
from folium import plugins
from neo4j.graph import Node, Graph
import webbrowser
import os
import time
import geopandas as gpd
from shapely import wkt
import pandas as pd


class MapViewer:
    """Interactive map visualization for query results."""

    def __init__(
        self,
        center_lat: float = 32.7157,
        center_lon: float = -117.1611,
        verbose: bool = False,
    ):
        """Initialize map centered on San Diego by default."""
        self.map = folium.Map(
            location=[center_lat, center_lon], zoom_start=11, tiles="cartodbpositron"
        )

        # Add fullscreen option
        plugins.Fullscreen().add_to(self.map)

        # Initialize feature groups for different layers
        self.layers = {
            "Businesses": folium.FeatureGroup(name="Businesses", show=True),
            "BlockGroups": folium.FeatureGroup(name="Block Groups", show=True),
            "Zipcodes": folium.FeatureGroup(name="Zipcodes", show=True),
            "Cities": folium.FeatureGroup(name="Cities", show=True),
            "Neighborhoods": folium.FeatureGroup(name="Neighborhoods", show=True),
        }

        # Set fixed path for map file
        self.map_path = os.path.join(os.path.expanduser("~"), ".biz_opps_map.html")
        self.verbose = verbose

    def _create_popup_html(self, node: Node) -> str:
        """Create HTML for popup with all properties."""
        html = "<div style='width:300px'>"
        for key, value in node.items():
            html += f"<p><b>{key}:</b> {value}</p>"
        html += "</div>"
        return html

    def _add_businesses(self, nodes: List[Node], color: str = "red"):
        """Add business markers to map."""
        for node in nodes:
            popup_html = self._create_popup_html(node)

            folium.Marker(
                location=[node["latitude"], node["longitude"]],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon="info-sign"),
            ).add_to(self.layers["Businesses"])

    def _add_polygons(self, nodes: List[Node], layer_name: str, color: str = "blue"):
        """Add polygon features to map."""
        if self.verbose:
            print(f"\nAdding {len(nodes)} polygons to {layer_name}")

        try:
            # Convert nodes directly to DataFrame
            df = pd.DataFrame([dict(node.items()) for node in nodes])

            if self.verbose:
                print("DataFrame columns:", df.columns.tolist())

            # Convert WKT strings to geometries
            geometries = gpd.GeoSeries.from_wkt(df["wkt"])

            # Drop the WKT column since we're converting it to geometry
            df = df.drop(columns=["wkt"])

            # Create GeoDataFrame with explicit geometry column
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")

            if self.verbose:
                print(f"Created GeoDataFrame with {len(gdf)} features")
                print("GeoDataFrame columns:", gdf.columns.tolist())

            # Add to map with fixed style (not dependent on feature properties)
            style = {
                "fillColor": color,
                "color": color,
                "weight": 2,
                "fillOpacity": 0.4,
                "opacity": 0.8,
            }

            highlight = {
                "weight": 4,
                "fillOpacity": 0.7,
                "opacity": 1.0,
            }

            folium.GeoJson(
                gdf.__geo_interface__,
                style_function=lambda x: style,  # Use fixed style dictionary
                popup=folium.GeoJsonPopup(
                    fields=[col for col in gdf.columns if col != "geometry"],
                    aliases=[col for col in gdf.columns if col != "geometry"],
                    localize=True,
                    labels=True,
                ),
                highlight_function=lambda x: highlight,  # Use fixed highlight dictionary
            ).add_to(self.layers[layer_name])

            if self.verbose:
                print(f"Added layer with style: {style}")

        except Exception as e:
            if self.verbose:
                print(f"Error creating polygon layer: {str(e)}")
                print("Error details:", str(e.__class__.__name__))

    def _add_points(self, nodes: List[Node], layer_name: str, color: str = "blue"):
        """Add point features (cities/neighborhoods) to map."""
        for node in nodes:
            popup_html = self._create_popup_html(node)

            # Add marker
            folium.CircleMarker(
                location=[node["latitude"], node["longitude"]],
                popup=folium.Popup(popup_html, max_width=300),
                color=color,
                fill=True,
                radius=8,
            ).add_to(self.layers[layer_name])

    def needs_visualization(self, graph: Graph) -> bool:
        """Determine if visualization is needed."""
        spatial_nodes = {"BlockGroup", "Zipcode", "Business"}
        return any(
            any(label in spatial_nodes for label in node.labels) for node in graph.nodes
        )

    def add_results(self, graph: Graph):
        """Add results to map."""
        # Group nodes by type
        nodes_by_type = {
            "Business": [],
            "BlockGroup": [],
            "Zipcode": [],
            "City": [],
            "Neighborhood": [],
        }

        # Sort nodes into their respective types
        for node in graph.nodes:
            for label in node.labels:
                if label in nodes_by_type:
                    nodes_by_type[label].append(node)
                    break  # Each node should only go into one category

        if self.verbose:
            print("\nAdding to map:")
            for node_type, nodes in nodes_by_type.items():
                if nodes:
                    print(f"- {len(nodes)} {node_type} nodes")

        # Add each type of node to appropriate layer
        if nodes_by_type["Business"]:
            self._add_businesses(nodes_by_type["Business"], color="blue")

        if nodes_by_type["BlockGroup"]:
            self._add_polygons(nodes_by_type["BlockGroup"], "BlockGroups", color="red")

        if nodes_by_type["Zipcode"]:
            self._add_polygons(nodes_by_type["Zipcode"], "Zipcodes", color="orange")

        if nodes_by_type["City"]:
            self._add_points(nodes_by_type["City"], "Cities", color="purple")

        if nodes_by_type["Neighborhood"]:
            self._add_points(
                nodes_by_type["Neighborhood"], "Neighborhoods", color="pink"
            )

    def show(self):
        """Display the map in browser."""
        try:
            # Add layers to map
            for layer in self.layers.values():
                layer.add_to(self.map)

            # Add layer control last
            folium.LayerControl().add_to(self.map)

            # Save to consistent location
            self.map.save(self.map_path)

            # Get URL with timestamp to force refresh
            timestamp = int(time.time())
            url = f"file://{os.path.abspath(self.map_path)}?t={timestamp}"

            if self.verbose:
                print(f"Saving map to: {self.map_path}")

            webbrowser.open(url, new=0)

        except Exception as e:
            if self.verbose:
                print(f"Error showing map: {str(e)}")
