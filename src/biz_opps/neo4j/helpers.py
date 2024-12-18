# Module containing helper functions that do not fit into other categories.
from neo4j import GraphDatabase
import os

# Define connection parameters for Neo4
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ------------------------------ CONNECTION FUNCTIONS ---------------------------------


def get_neo4j_driver():
    """
    Establishes a connection to the Neo4j database and returns the driver.

    Returns:
        A Neo4j driver object to create sessions and execute queries.
    """
    try:
        # Initialize driver for Neo4j connection
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        # Optional: Verify the connection by opening a session
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            if result.single()["test"] == 1:
                print("Connected to Neo4j database successfully.")

        return driver
    except Exception as error:
        print(f"Error connecting to Neo4j: {error}")
        return None
