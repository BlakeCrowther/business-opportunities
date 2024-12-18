import os
import asyncio
from dotenv import load_dotenv

from biz_opps.neo4j.helpers import get_neo4j_driver
from biz_opps.query.interface.query_engine import QueryEngine


async def query():
    # Load environment variables
    load_dotenv()
    try:
        # Get Neo4j driver
        neo4j_driver = get_neo4j_driver()
        # Get openai api key
        openai_api_key = os.getenv("OPENAI_API_KEY")
        # Initialize query engine
        query_engine = QueryEngine(neo4j_driver, openai_api_key, verbose=False)
        print("\nStarting continuous query mode. Press Ctrl+C to exit.")
        print("Enter natural language queries about the business landscape.")
        while True:
            query = input(
                "\n\nWhat would you like to know about the business landscape? "
            )
            additional_context = input(
                "Enter any additional context you would like to include: "
            )
            results = await query_engine.process_query(
                query=query, additional_context=additional_context
            )
            print("\n")
            print("Query:")
            print(results["query"])
            print("\n")
            print("Reasoning:")
            print(results["reasoning"])
            print("\n")
            print("Results:")
            print(results["interpretation"])
            print("\n")
            print("Suggested Follow-up Questions:")
            print(results["suggested_queries"])
    except KeyboardInterrupt:
        print("\nExiting query mode...")
    finally:
        neo4j_driver.close()


if __name__ == "__main__":
    asyncio.run(query())
