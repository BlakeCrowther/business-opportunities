# Business Opportunities Knowledge Graph

**Contributers:** Alan Li, Blake Crowther, Wesley Schiller

## Knowledge Graph

**Intent:** A graph over businesses, regional characteristics, administrative boundaries.

**Structure:** The knowledge graph integrates multiple layers of data about San Diego:
- Businesses (from Google Places API) with their types, locations, and other attributes
- Block Groups with demographic and socioeconomic characteristics
- Administrative boundaries (zip codes, cities, neighborhoods)

This multi-layered structure enables analysis of business opportunities by connecting location-based business data with regional characteristics and infrastructure.

**Users:** The city of San Diego asks "Where are the food deserts in San Diego, where there is shortage of grocery store but an abundance of fast food stores"?

## Data Sources
- [Google Place API](https://developers.google.com/maps/documentation/places/web-service/overview)
- GeoEnriched Data at the level of block group (provided by [NDP](https://www.ndpconsulting.com/))
- Administrative Topology (provided by [NDP](https://www.ndpconsulting.com/))
- [San Diego Block Groups (SANDAG)](https://geo.sandag.org/portal/home/item.html?id=0ea585f3929140cea3f84629758aea8a)
- [San Diego County Zip Codes (SANDAG)](https://geo.sandag.org/portal/home/item.html?id=e110d5f6c4fd490e8828353135676c2b)

## Documentation
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/)
- [Neo4j Spatial Procedures](https://neo4j-contrib.github.io/spatial/0.24-neo4j-3.1/index.html#spatial-procedures)
- [Google Place API](https://developers.google.com/maps/documentation/places/web-service/overview)
- [OpenAI API](https://platform.openai.com/docs/api-reference)

## Knowledge Graph Construction and Querying

### Prerequisites
1. Set up your environment variables in `.env`:
```bash
NEO4J_URI=<your-neo4j-uri>
NEO4J_USERNAME=<your-username>
NEO4J_PASSWORD=<your-password>
OPENAI_API_KEY=<your-openai-api-key>  # Required for querying
```

2. Install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
python -m ipykernel install --user --name=venv --display-name "Biz Opps" # OPTIONAL: Install kernel for jupyter notebook or interactive python
```

### Populating the Knowledge Graph
The knowledge graph is constructed using the `populate.py` script. This script supports populating different components of the graph or all components by default:

- block_groups
- administrative_topology
- businesses
- geoenrichments

```bash
python scripts/populate.py # Populate all components
```

#### Population Options
- `--include`: Specify which components to populate (comma-separated)
  ```bash
  python scripts/populate.py --include=block_groups,administrative_topology
  ```

- `--exclude`: Specify which components to exclude (comma-separated)
  ```bash
  python scripts/populate.py --exclude=businesses,geoenrichments
  ```

- `--cleanup`: Clean existing data before populating (default: False)
  ```bash
  python scripts/populate.py --cleanup=True
  ```

- `--verbose`: Enable detailed output logging (default: False)
  ```bash
  python scripts/populate.py --verbose=True
  ```

**Note**: To populate businesses you must authenticate with Google using the Google Cloud SDK.
To do this run the following commands outside of the virtual environment:
```bash
brew install --cask google-cloud-sdk
gcloud init
gcloud auth application-default login
```

### Querying the Knowledge Graph
The knowledge graph can be queried using natural language through the `query.py` script. This interface uses OpenAI to translate natural language queries into Cypher queries.

To start the query interface:
```bash
python scripts/query.py
```

#### Query Interface Features:
1. Enter natural language questions about the business landscape
2. Provide additional context to refine your queries or guide the LLM to obtain the desired results
3. For each query, you'll receive:
   - The translated Cypher query
   - Reasoning behind the translation
   - Query results
   - Suggested follow-up questions
4. If the results are visualizable (e.g. block groups, businesses, etc.) the script will attempt to generate a map of the results using the `folium` library.

Example queries:
- "Where are the best places to open a new fast food restaurant?"
- "What block groups might have a lack of grocery stores and a high density of fast food restaurants?"
- "Are there any notable crime statistics or education levels associated with the block groups in zip code 92107 that could impact local business performance?"

To exit the query interface, press `Ctrl+C` twice.

**Note**: Querying requires a valid OpenAI API key in your `.env` file.
