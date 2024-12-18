import json
from biz_opps.neo4j.helpers import get_neo4j_driver
from biz_opps.query.interface.query_engine import QueryEngine
import os


async def process_query(input_data):
    """
    Handles natural language queries using the QueryEngine
    """
    try:
        # Get Neo4j driver
        neo4j_driver = get_neo4j_driver()

        # Get openai api key from environment
        openai_api_key = os.getenv("OPENAI_API_KEY")

        # Initialize query engine
        query_engine = QueryEngine(neo4j_driver, openai_api_key, verbose=False)

        # Process the query
        results = await query_engine.process_query(
            query=input_data["query"],
            additional_context=input_data.get("additional_context", ""),
        )

        # Close the driver
        neo4j_driver.close()

        return results

    except Exception as e:
        raise Exception(f"Query processing error: {str(e)}")
