from typing import Dict
import json
import os

from biz_opps.utils.file import get_root_dir


class SchemaLoader:
    """Loads and formats schema context for LLM."""

    def __init__(
        self,
        schema_path: str = os.path.join(
            get_root_dir(), "configs", "constraints_schema.json"
        ),
        verbose: bool = False,
    ):
        self.schema = self._load_schema(schema_path, verbose)

    def _load_schema(self, path: str, verbose: bool) -> Dict:
        """Load schema from file."""
        with open(path) as f:
            schema = json.load(f)

        if verbose:
            print(f"\nLoaded schema from {path}")

        return schema

    def get_formatted_context(self) -> str:
        """Format schema for LLM context."""
        context = []

        # Node labels and properties
        context.append("Node Labels and Properties:")
        for label, info in self.schema["nodes"].items():
            props = info["properties"]
            context.append(f"\n{label}:")
            for prop, details in props.items():
                prop_info = f"  - {prop} ({details['type']})"
                if "enum" in details:
                    prop_info += f": {', '.join(details['enum'])}"
                context.append(prop_info)

        # Relationships with explicit direction
        context.append("\nRelationship Types and Directions:")
        for rel_type, info in self.schema["relationships"].items():
            context.append(f"\n{rel_type}:")
            if "properties" in info:
                context.append("  Properties:")
                for prop, details in info["properties"].items():
                    context.append(f"    - {prop} ({details['type']})")
            context.append("  Valid Directions:")
            for start, ends in info["mappings"].items():
                for end in ends:
                    context.append(f"    - ({start})-[:{rel_type}]->({end})")

        # Spatial layers
        if "spatial_layers" in self.schema:
            context.append("\nSpatial Layers:")
            for layer, info in self.schema["spatial_layers"].items():
                context.append(f"\n{layer}:")
                context.append(f"  Nodes: {', '.join(info['nodes'])}")
                context.append(f"  Type: {info['layer_class']}")

        return "\n".join(context)
