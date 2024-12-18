# This is the main script that will be executed to populate the entire knowledge graph.
import argparse
import asyncio
from dotenv import load_dotenv

from biz_opps.neo4j.cleanup import cleanup_neo4j
from biz_opps.etl.administrative_topology import populate_administrative_topology
from biz_opps.etl.block_groups import populate_block_groups
from biz_opps.etl.businesses import populate_businesses
from biz_opps.etl.geoenrichment import populate_geoenrichments
from biz_opps.neo4j.helpers import get_neo4j_driver
from biz_opps.neo4j.constraints import create_constraints, load_constraints

etl_components = {
    "block_groups": populate_block_groups,
    "administrative_topology": populate_administrative_topology,
    "businesses": populate_businesses,
    "geoenrichments": populate_geoenrichments,
}


async def populate():
    """
    Main function to populate the Neo4j knowledge graph.
    By default, all components are populated.
    Otherwise, components can be included or excluded using the --include OR --exclude flags.
    Args:
        --include: Comma-separated list of components to include (e.g., "block_groups,administrative_topology")
        --exclude: Comma-separated list of components to exclude (e.g., "businesses,geoenrichments")
        --cleanup: Cleanup Neo4j before populating (default: False)
        --verbose: Verbose output (default: False)
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Populate Neo4j knowledge graph")
    parser.add_argument(
        "--include",
        type=str,
        help="Comma-separated list of components to include",
        default=None,
    )
    parser.add_argument(
        "--exclude",
        type=str,
        help="Comma-separated list of components to exclude",
        default=None,
    )
    parser.add_argument(
        "--cleanup", type=bool, help="Cleanup Neo4j before populating", default=False
    )
    parser.add_argument("--verbose", type=bool, help="Verbose output", default=False)

    args = parser.parse_args()

    # Ensure only one of --include or --exclude is provided
    if args.include and args.exclude:
        raise ValueError("Only one of --include or --exclude can be provided")

    # Convert comma-separated strings to lists if provided
    include = args.include.split(",") if args.include else None
    exclude = args.exclude.split(",") if args.exclude else None

    # Load environment variables
    load_dotenv()

    # Get Neo4j driver
    neo4j_driver = get_neo4j_driver()

    if neo4j_driver:
        # Get constraints
        constraints = load_constraints()

        # Create constraints
        with neo4j_driver.session() as session:
            create_constraints(session, constraints)

        # populate data
        for component in etl_components:
            if include is None or component in include:
                if exclude is None or component not in exclude:
                    print(f"Executing {component} ETL...")
                    if component == "businesses":
                        await etl_components[component](
                            neo4j_driver,
                            constraints,
                            cleanup=args.cleanup,
                            verbose=args.verbose,
                        )
                    else:
                        etl_components[component](
                            neo4j_driver,
                            constraints,
                            cleanup=args.cleanup,
                            verbose=args.verbose,
                        )
    else:
        raise Exception("Failed to establish connection to Neo4j")

    neo4j_driver.close()


if __name__ == "__main__":
    asyncio.run(populate())
