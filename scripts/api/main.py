import sys
import json
import asyncio
from services.query_processor import process_query


async def main():
    try:
        # Get input from Node.js
        input_data = json.loads(sys.argv[1])

        # Process the query
        result = await process_query(input_data)

        # Return result as JSON
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
