from typing import Dict, Optional, List, Union

from neo4j import graph

from biz_opps.query.context.schema_loader import SchemaLoader
from biz_opps.query.context.doc_loader import DocumentationLoader
from biz_opps.query.visualization.map_viewer import MapViewer
from biz_opps.query.cypher.executor import CypherExecutor
from biz_opps.query.llm.openai_client import OpenAIClient


class QueryEngine:
    """Main query processing engine."""

    def __init__(
        self,
        neo4j_driver,
        openai_api_key,
        verbose: bool = False,
    ):
        self.schema_context = SchemaLoader(verbose=verbose)
        self.doc_loader = DocumentationLoader(verbose=verbose)
        self.cypher_executor = CypherExecutor(
            neo4j_driver, self.schema_context.schema, verbose=verbose
        )
        self.openai_client = OpenAIClient(api_key=openai_api_key, verbose=verbose)
        self.map_viewer = MapViewer(verbose=verbose)
        self.verbose = verbose

    async def process_query(
        self,
        query: str,
        additional_context: Optional[Union[Dict[str, str], str]] = None,
        include_docs: Optional[List[str]] = None,
    ) -> Dict:
        """
        Process natural language query end-to-end.

        Args:
            query: Natural language query
            additional_context: Optional textual additional context
            include_docs: Optional list of document names to include as context.
                Defaults to all documents.
        """
        try:
            if not query:
                raise ValueError("Query cannot be empty")

            print(f"\nProcessing query: {query}")

            # Get schema and documentation context
            schema_context = self.schema_context.get_formatted_context()
            if include_docs:
                doc_context = self.doc_loader.get_context_docs(include_docs)
            else:
                doc_context = self.doc_loader.get_context_docs()

            # Combine contexts
            context = {
                "schema": schema_context,
                **doc_context,
            }

            # Add additional context if provided
            if additional_context:
                context["additional"] = additional_context

            # Generate and parse Cypher query
            query_info = await self.openai_client.generate_cypher(query, context)

            if not query_info["cypher"]:
                raise ValueError("No valid Cypher query generated")

            # Execute query and get graph
            graph, parsed_graph = await self.cypher_executor.execute_query(
                query_info["cypher"]
            )

            # Interpret results
            results_info = await self.openai_client.interpret_results(
                parsed_graph, query, query_info, schema_context
            )

            # Determine if visualization is needed
            needs_viz = self.map_viewer.needs_visualization(graph)

            print(f"Needs visualization: {needs_viz}")

            if needs_viz:
                if self.verbose:
                    print("\nCreating visualization...")

                self.map_viewer.add_results(graph)
                self.map_viewer.show()

            if self.verbose:
                print("\nQuery processing complete")

            return {
                "query": query_info["cypher"],
                "reasoning": query_info["reasoning"],
                "interpretation": results_info["interpretation"],
                "suggested_queries": results_info["suggested_queries"],
            }

        except Exception as e:
            if self.verbose:
                print(f"\nError processing query: {str(e)}")
            return {
                "error": str(e),
                "query": query,
                "context_used": bool(additional_context),
                "docs_used": include_docs or [],
            }
