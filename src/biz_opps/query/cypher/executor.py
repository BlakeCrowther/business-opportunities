from typing import Dict, Any, Tuple
import asyncio
from neo4j.graph import Graph


class CypherExecutor:
    """Executes and handles Cypher queries."""

    def __init__(self, driver, schema=None, verbose=False):
        self.driver = driver
        self.schema = schema
        self.verbose = verbose

    async def execute_query(self, cypher: str) -> Graph:
        """
        Execute Cypher query and return results.

        Returns:
            Tuple[Graph, Dict]: Neo4j graph object and parsed graph data for LLM interpretation and visualization determination
        """
        try:
            if self.verbose:
                print(f"\nExecuting Cypher query:\n{cypher}")

            # Run query in thread pool since neo4j-driver is synchronous
            loop = asyncio.get_event_loop()
            with self.driver.session() as session:
                if self.verbose:
                    print("Running query...")

                # Execute query and get result
                result = await loop.run_in_executor(None, session.run, cypher)

                # Collect all records to ensure they're consumed
                records = await loop.run_in_executor(None, lambda: list(result))

                # Get graph from result
                graph = result.graph()

                # Parse graph for LLM interpretation and visualization determination
                parsed_graph = self._parse_graph(graph)

                if self.verbose:
                    print(
                        f"Got {len(graph.nodes)} nodes and {len(graph.relationships)} relationships"
                    )

                return graph, parsed_graph

        except Exception as e:
            print(f"Query execution failed: {str(e)}")
            raise

    def _parse_graph(self, graph: Graph) -> Dict:
        """Parse graph for LLM interpretation and visualization determination."""
        # Format graph data for LLM consumption
        nodes_info = []
        for node in graph.nodes:
            node_info = {
                "labels": list(node.labels),
                "properties": dict(node.items()),
            }
            nodes_info.append(node_info)

        rels_info = []
        for rel in graph.relationships:
            rel_info = {
                "type": rel.type,
                "properties": dict(rel.items()),
                "start_node_labels": list(rel.start_node.labels),
                "end_node_labels": list(rel.end_node.labels),
            }
            rels_info.append(rel_info)

        return {"nodes": nodes_info, "relationships": rels_info}
