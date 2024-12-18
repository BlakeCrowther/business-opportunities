import os
from openai import AsyncOpenAI
from typing import Dict, Optional
from neo4j import graph


class OpenAIClient:
    """Handles interactions with OpenAI API."""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        verbose: bool = False,
    ):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self.verbose = verbose

    def _summarize_nodes(self, nodes_info: list) -> str:
        """Create a summary of node information."""
        # Group nodes by label
        nodes_by_label = {}
        for node in nodes_info:
            for label in node["labels"]:
                if label not in nodes_by_label:
                    nodes_by_label[label] = []
                nodes_by_label[label].append(node["properties"])

        # Create summary
        summary = []
        for label, nodes in nodes_by_label.items():
            summary.append(f"\n{label} Nodes ({len(nodes)}):")
            # Show properties from first node as example
            if nodes:
                summary.append("Example properties:")
                for prop, value in nodes[0].items():
                    summary.append(f"  - {prop}: {value}")

        return "\n".join(summary)

    def _summarize_relationships(self, rels_info: list) -> str:
        """Create a summary of relationship information."""
        # Group relationships by type
        rels_by_type = {}
        for rel in rels_info:
            rel_type = rel["type"]
            if rel_type not in rels_by_type:
                rels_by_type[rel_type] = []
            rels_by_type[rel_type].append(rel)

        # Create summary
        summary = []
        for rel_type, rels in rels_by_type.items():
            summary.append(f"\n{rel_type} Relationships ({len(rels)}):")
            # Show example relationship
            if rels:
                rel = rels[0]
                summary.append(
                    f"Example: ({' '.join(rel['start_node_labels'])})-[:{rel_type}]->({' '.join(rel['end_node_labels'])})"
                )
                if rel["properties"]:
                    summary.append("Properties:")
                    for prop, value in rel["properties"].items():
                        summary.append(f"  - {prop}: {value}")

        return "\n".join(summary)

    async def generate_cypher(
        self,
        query: str,
        context: Dict[str, str],
    ) -> Dict[str, str]:
        """Generate Cypher query from natural language."""
        if self.verbose:
            print("\nGenerating Cypher query...")
            print(f"Input query: {query}")
            print("Context keys:", list(context.keys()))

        # First message: Analyze the query requirements
        analysis_messages = [
            {
                "role": "system",
                "content": """You are a Neo4j query planner. 

IMPORTANT SCHEMA GUIDELINES:
1. Spatial Data Availability:
   - BlockGroups and Zipcodes: Have polygon geometries (EditableLayerImpl)
   - Businesses: Have point geometries (SimplePointLayer)
   - Cities and Neighborhoods: NO geometries - must use IS_WITHIN relationships

2. Spatial Operations:
   - spatial.intersects()
   - spatial.closest()
   - spatial.bbox()
   - spatial.withinDistance()
   - For Cities/Neighborhoods: Must query through IS_WITHIN relationships to Zipcodes

3. Relationship Patterns:
   - IS_WITHIN flows FROM (BlockGroup/City/Neighborhood) TO (Zipcode)
   - BlockGroups->IS_WITHIN->Zipcode has overlap_ratio property
   - Cities/Neighborhoods/BlockGroups->IS_WITHIN->Zipcode has containment_type property (Partial, Full)

4. CRITICAL ENRICHMENT RULES:
   - ONLY BlockGroups have HAS_ENRICHMENT relationships
   - Cities and Neighborhoods MUST be mapped to enrichments through their contained BlockGroups
   - Pattern for City/Neighborhood enrichments:
     (City/Neighborhood)-[:IS_WITHIN]->(Zipcode)<-[:IS_WITHIN]-(BlockGroup)-[:HAS_ENRICHMENT]->(Enrichment)
   - Aggregate BlockGroup enrichments to understand City/Neighborhood characteristics

5. Schema Adherence:
   - Use ONLY properties defined in the schema
   - Match enum values EXACTLY as specified
   - Follow property types (STRING, FLOAT, etc.)
   - Use correct property names as defined

Analyze the query requirements focusing on:
1. What data needs to be retrieved
2. Which node types and relationships are involved
3. Whether spatial operations are needed and which type
4. What constraints or filters should be applied
5. Any enrichment categories that need to be matched
6. For City/Neighborhood queries: How to properly map to BlockGroup enrichments

Be specific about the requirements and verify all properties against the schema.""",
            },
            {"role": "system", "content": f"Schema Context:\n{context['schema']}"},
            {
                "role": "user",
                "content": f"Query: {query}\n\nAnalyze what's needed to answer this query.",
            },
        ]

        analysis_response = await self.client.chat.completions.create(
            model=self.model, messages=analysis_messages, temperature=0.1
        )
        query_analysis = analysis_response.choices[0].message.content

        if self.verbose:
            print("\nQuery Analysis:")
            print(query_analysis)

        # Second message: Generate the Cypher query
        cypher_messages = [
            {
                "role": "system",
                "content": """You are a Neo4j query expert. Generate a Cypher query following these guidelines:

1. Spatial Query Patterns:
   - spatial.intersects()
   - spatial.closest()
   - spatial.bbox()
   - spatial.withinDistance()
   - For Cities/Neighborhoods leverage IS_WITHIN relationships to Zipcodes

2. CRITICAL ENRICHMENT PATTERNS:
   - ONLY BlockGroups have direct HAS_ENRICHMENT relationships
   - For City/Neighborhood enrichment queries:
     MATCH (area:City|Neighborhood)-[:IS_WITHIN]->(z:Zipcode)<-[:IS_WITHIN]-(bg:BlockGroup)-[:HAS_ENRICHMENT]->(e:EnrichmentType)
   - Consider using COUNT, AVG, or other aggregations to summarize BlockGroup enrichments
   - Example City enrichment query:
     MATCH (c:City)-[:IS_WITHIN]->(z:Zipcode)<-[:IS_WITHIN]-(bg:BlockGroup)
     MATCH (bg)-[:HAS_ENRICHMENT]->(e:EnrichmentType)
     WITH c, e.category as cat, COUNT(bg) as count
     ORDER BY count DESC

3. Common Patterns:
   - Finding businesses in a city: 
     MATCH (c:City)-[:IS_WITHIN]->(z:Zipcode)<-[:IS_WITHIN]-(bg:BlockGroup)
   - Spatial with demographics:
     MATCH (bg:BlockGroup)-[:HAS_ENRICHMENT]->(e:EnrichmentType)
     WHERE spatial.intersects('block_group_layer', bg.wkt)

4. Schema Validation:
   - Verify all property names against schema
   - Use exact enum values from schema
   - Follow property types (STRING, FLOAT, etc.)
   - Include only valid relationships
   - Return complete nodes (not just properties) for spatial types to enable visualization

IMPORTANT: Your response must be in this format:
```cypher
YOUR_QUERY_HERE
```

REASONING:
Explain how the query works and why it will answer the original question.
Include confirmation that all properties and enums match schema exactly.""",
            },
            {"role": "system", "content": f"Schema Context:\n{context['schema']}"},
            {"role": "system", "content": "Query Analysis:\n" + query_analysis},
            # Add any additional context (like spatial docs)
            *[
                {"role": "system", "content": f"{k} Context:\n{v}"}
                for k, v in context.items()
                if k != "schema"
            ],
            {"role": "user", "content": query},
        ]

        cypher_response = await self.client.chat.completions.create(
            model=self.model, messages=cypher_messages, temperature=0
        )

        # Parse response
        content = cypher_response.choices[0].message.content
        parts = content.split("```")

        if len(parts) >= 3:
            cypher = parts[1].replace("cypher", "").strip()
            reasoning = parts[2].strip().replace("REASONING:", "").strip()
        else:
            raise ValueError("Response not in expected format")

        if self.verbose:
            print("\nGenerated Cypher:")
            print(cypher)
            print("\nReasoning:")
            print(reasoning)

        return {"cypher": cypher, "reasoning": reasoning, "analysis": query_analysis}

    async def interpret_results(
        self,
        parsed_graph: Dict,
        query: str,
        query_info: Dict[str, str],
        schema_context: str,
    ) -> Dict:
        """Generate natural language interpretation of query results."""
        try:
            # Build summaries
            nodes_summary = self._summarize_nodes(parsed_graph["nodes"])
            rels_summary = self._summarize_relationships(parsed_graph["relationships"])

            if self.verbose:
                print("\nGenerating statistical analysis...")

            # First message: Get statistical analysis
            stats_messages = [
                {
                    "role": "system",
                    "content": """You are a data analyst analyzing Neo4j graph results.
Analyze the results focusing on:
1. Node and relationship counts by type
2. Important property value ranges or distributions
3. Notable patterns in the data structure
Be precise and quantitative.""",
                },
                {"role": "system", "content": f"Schema Context:\n{schema_context}"},
                {
                    "role": "user",
                    "content": f"""
Query: {query}
Node Summary: {nodes_summary}
Relationship Summary: {rels_summary}
""",
                },
            ]

            stats_response = await self.client.chat.completions.create(
                model=self.model,
                messages=stats_messages,
                temperature=0.1,
            )
            statistical_analysis = stats_response.choices[0].message.content

            if self.verbose:
                print("\nStatistical Analysis:")
                print(statistical_analysis)
                print("\nGenerating interpretation and suggestions...")

            # Second message: Interpret results and suggest follow-ups
            interpret_messages = [
                {
                    "role": "system",
                    "content": """You are a business analyst interpreting Neo4j query results.

TASK:
1. Explain what the results mean in relation to the original query
2. Highlight key insights and patterns
3. Suggest 2-3 follow-up questions that can be answered using our schema

FORMAT YOUR RESPONSE AS:
Interpretation: [Your interpretation of the results]

Suggested Follow-up Questions:
1. [Question that uses available data]
2. [Question that uses available data]
3. [Question that uses available data]""",
                },
                {"role": "system", "content": f"Schema Context:\n{schema_context}"},
                {
                    "role": "user",
                    "content": f"""
Original Query: {query}
Cypher Used: {query_info['cypher']}
Query Reasoning: {query_info['reasoning']}

Statistical Analysis:
{statistical_analysis}
""",
                },
            ]

            interpret_response = await self.client.chat.completions.create(
                model=self.model,
                messages=interpret_messages,
                temperature=0.3,
            )

            # Parse response into interpretation and suggestions
            response_text = interpret_response.choices[0].message.content
            parts = response_text.split("Suggested Follow-up Questions:")

            interpretation = parts[0].replace("Interpretation:", "").strip()
            suggestions = parts[1].strip() if len(parts) > 1 else ""

            return {
                "interpretation": interpretation,
                "suggested_queries": suggestions,
            }

        except Exception as e:
            if self.verbose:
                print(f"Error interpreting results: {str(e)}")
            return {
                "interpretation": "Failed to interpret results",
                "suggested_queries": "",
                "error": str(e),
            }
