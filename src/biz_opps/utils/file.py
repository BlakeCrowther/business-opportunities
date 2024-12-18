import os
import json


def get_root_dir():
    """
    Walk upwards from this module's directory to find the repository root.
    The root is identified by the presence of a .git folder or pyproject.toml file.

    Returns:
        str: Absolute path to the repository root.
    Raises:
        RuntimeError: If the repository root is not found.
    """
    # Start from the directory containing this file
    current_dir = os.path.abspath(os.path.dirname(__file__))

    # Walk up the directory tree
    while current_dir != os.path.dirname(current_dir):  # Stop at the filesystem root
        if os.path.exists(os.path.join(current_dir, ".git")) or os.path.exists(
            os.path.join(current_dir, "pyproject.toml")
        ):
            return current_dir
        current_dir = os.path.dirname(current_dir)

    raise RuntimeError("Repository root not found.")


def load_json(file_path):
    """
    Load JSON file from the given file path.

    Args:
        file_path (str): The file path to load JSON file from.

    Returns:
        dict: The JSON file content.
    """
    with open(file_path, "r") as file:
        return json.load(file)
