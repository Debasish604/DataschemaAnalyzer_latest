"""
Automated Ingest: Files -> Gemini -> Neo4j AuraDB -> Graph PNG

Flowchart (Mermaid)

```mermaid
flowchart TD
  A[User uploads .csv / .xlsx files] --> B(Flask / Upload endpoint)
  B --> C[Parse files with pandas]
  C --> D[Build structured schema summary (samples + types)]
  D --> E[Send prompt to Gemini (google-genai) asking for JSON: entities, PKs, FKs, relationships]
  E --> F[Parse Gemini JSON output]
  F --> G[Translate JSON into Cypher-like operations]
  G --> H[Use Neo4j Python driver to MERGE nodes & relationships in AuraDB]
  H --> I[Run read query to fetch a sample subgraph]
  I --> J[Convert result to NetworkX graph]
  J --> K[Draw graph with matplotlib and save PNG/JPEG locally]
  K --> L[Return status & downloadable graph to user]
```

Overview
--------
This single-file Flask app demonstrates an automated pipeline that:
1. Accepts uploaded structured files (.csv, .xlsx)
2. Uses Gemini (via the Google Gen AI Python SDK) to automatically infer entities, primary/foreign keys, and relationships from the data (prompt-based)
3. Creates/merges nodes and relationships into a Neo4j AuraDB instance
4. Exports a visualization of the resulting graph to a local image file (.png)

IMPORTANT: Replace environment variables with your own values before running. Never commit secrets to source control.

Prerequisites / pip packages
----------------------------
pip install flask pandas google-genai neo4j networkx matplotlib python-dotenv openpyxl

Environment variables (example .env)
------------------------------------
GEMINI_API_KEY=your_gemini_api_key_here
NEO4J_URI=neo4j+s://<your-aura-host>.databases.neo4j.io
NEO4J_USERNAME=<username>
NEO4J_PASSWORD=<password>
FLASK_SECRET_KEY=dev-secret

Run
---
python neo4j_gemini_ingest_app.py

Then POST files to /ingest (multipart form, field name 'files') or visit the root page for a very small upload form.

Caveats & notes
----------------
- This is a PoC / reference implementation. For production you should:
  * Add batching, deduplication, schema validation, data sanitization
  * Add error handling, rate-limiting, and retry logic for API calls
  * Secure credentials (use secret manager) and limit returned sample sizes for Gemini
  * Add transaction batching and streaming inserts when working with big datasets

"""

import os
import io
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List

from flask import Flask, request, jsonify, send_from_directory, render_template_string
import pandas as pd
from google import genai
from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret')

# Basic checks
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY not set. Set it in .env or the environment.")

if not (NEO4J_URI and NEO4J_USERNAME and NEO4J_PASSWORD):
    print("WARNING: NEO4J credentials not completely set. Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD in .env or environment.")

# Create clients
# Gemini / Google Gen AI client picks up GEMINI_API_KEY from environment by default
genai_client = genai.Client()

# Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

# Flask app
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# Output folder for graphs
OUT_DIR = Path('saved_graphs')
OUT_DIR.mkdir(exist_ok=True)

# Minimal upload form
INDEX_HTML = """
<!doctype html>
<title>Upload files</title>
<h1>Upload CSV / XLSX files</h1>
<form method=post enctype=multipart/form-data action="/ingest">
  <input type=file name=files multiple>
  <input type=submit value=Upload>
</form>
"""

@app.route('/')
def index():
    return render_template_string(INDEX_HTML)


def read_uploaded_file(file_storage) -> Dict[str, pd.DataFrame]:
    """Read an uploaded file into one or more pandas DataFrames.
    Returns a dict mapping table_name -> DataFrame.
    """
    filename = file_storage.filename
    fname = filename.lower()
    contents = file_storage.read()
    buf = io.BytesIO(contents)

    tables = {}
    try:
        if fname.endswith('.csv'):
            df = pd.read_csv(buf)
            tables[Path(filename).stem] = df
        elif fname.endswith('.xlsx') or fname.endswith('.xls'):
            xls = pd.read_excel(buf, sheet_name=None)
            # sheet_name=None -> dict of sheet_name -> df
            for sheet, df in xls.items():
                key = f"{Path(filename).stem}__{sheet}"
                tables[key] = df
        else:
            # try csv fallback
            try:
                df = pd.read_csv(buf)
                tables[Path(filename).stem] = df
            except Exception:
                raise ValueError('Unsupported file type (only .csv and .xlsx supported)')
    finally:
        file_storage.stream.seek(0)

    return tables


def build_schema_summary(tables: Dict[str, pd.DataFrame], sample_rows=5) -> Dict[str, Any]:
    """Create a compact schema summary to include in the Gemini prompt.
    For each table we include: name, columns (name + inferred dtype), sample values.
    """
    summary = {'tables': []}
    for name, df in tables.items():
        df2 = df.copy()
        df2 = df2.where(pd.notnull(df2), None)
        columns = []
        for col in df2.columns:
            col_dtype = str(df2[col].dtype)
            samples = df2[col].head(sample_rows).tolist()
            columns.append({'name': str(col), 'dtype': col_dtype, 'samples': samples})
        summary['tables'].append({'table_name': name, 'rows': int(len(df2)), 'columns': columns})
    return summary


PROMPT_TEMPLATE = r"""
You are an assistant that *only returns JSON* describing how to map relational/structured data tables into a graph model.
Input: a list of tables. Each table has a name, columns (with dtype and sample values).
Output: a JSON object with this exact structure:
{
  "entities": [
    {
      "name": "<LabelName>",
      "table_name": "<original_table_name>",
      "columns": [{"name":"col","dtype":"...","sample_values":[...]}, ...],
      "primary_key_candidates": ["col1", "col2"],  // ordered by confidence
      "chosen_primary_key": "colX" or null,
      "foreign_keys": [
         {"column":"col_in_this_table","to_table":"other_table_name","to_column":"col_in_other","confidence":0.9}
      ],
      "relationships": [
         {"to_entity":"OtherLabel","type":"RELATIONSHIP_TYPE","direction":"OUTGOING" , "explain":"short explanation"}
      ]
    }
  ],
  "global_relationships": [
    {"from_table":"A","from_column":"a_id","to_table":"B","to_column":"b_id","relationship_type":"RELATED_TO","confidence":0.9}
  ]
}

Please: - only output valid JSON (no preamble/postamble) - keep field names exactly as above.
Be conservative in matching columns with low confidence (set confidence 0.0-1.0).
If you are uncertain leave chosen_primary_key as null and put candidates in primary_key_candidates.
Limit sample sizes in your reasoning; we only need column names, types and short samples.
"""


def call_gemini_for_insights(schema_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Send the schema summary to Gemini and request the structured JSON output described above.
    Returns parsed JSON (or raises on parse error).
    """
    prompt = PROMPT_TEMPLATE + "\n\nINPUT_SCHEMA:=\n" + json.dumps(schema_summary, default=str)
    print('Sending prompt to Gemini (trimmed to 2000 chars for log)')
    # Keep logs small
    # Call the Google Gen AI SDK
    resp = genai_client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    text = resp.text

    # Try to extract JSON from response robustly
    try:
        parsed = json.loads(text)
        return parsed
    except Exception:
        # try to find first and last brace
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            maybe = text[start:end+1]
            try:
                parsed = json.loads(maybe)
                return parsed
            except Exception as e:
                raise RuntimeError(f'Failed to parse JSON from Gemini. Raw response (first 1000 chars): {text[:1000]}')
        else:
            raise RuntimeError(f'No JSON object found in Gemini response. Raw response (first 1000 chars): {text[:1000]}')


def merge_data_into_neo4j(parsed_json: Dict[str, Any], tables: Dict[str, pd.DataFrame], batch_size=200):
    """Translate parsed JSON into MERGE Cypher statements and push into Neo4j.
    This implementation focuses on safety and simplicity: MERGE by chosen_primary_key when available.
    """
    with driver.session() as session:
        # insert nodes
        for entity in parsed_json.get('entities', []):
            table_name = entity['table_name']
            label = entity['name']
            df = tables.get(table_name)
            if df is None:
                print(f"Table {table_name} not found among uploaded tables. Skipping entity {label}.")
                continue

            chosen_pk = entity.get('chosen_primary_key') or (entity.get('primary_key_candidates') or [None])[0]
            if not chosen_pk:
                # fallback to first column
                chosen_pk = df.columns[0]

            # iterate rows in batches
            records = df.where(pd.notnull(df), None).to_dict(orient='records')
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                tx = session.begin_transaction()
                for row in batch:
                    pk_val = row.get(chosen_pk)
                    if pk_val is None:
                        # skip rows without pk
                        continue
                    props = {k: v for k, v in row.items() if v is not None}
                    # Use parameterized MERGE
                    cypher = f"MERGE (n:`{label}` {{`{chosen_pk}`: $pk}}) SET n += $props"
                    tx.run(cypher, pk=pk_val, props=props)
                tx.commit()

        # create relationships
        # Use global_relationships if provided, otherwise use each entity's foreign_keys
        relationships = parsed_json.get('global_relationships', [])
        # gather per-entity fk definitions
        for entity in parsed_json.get('entities', []):
            for fk in entity.get('foreign_keys', []):
                relationships.append({
                    'from_table': entity['table_name'],
                    'from_column': fk['column'],
                    'to_table': fk['to_table'],
                    'to_column': fk['to_column'],
                    'relationship_type': fk.get('relationship_type') or 'RELATED_TO',
                    'confidence': fk.get('confidence', 0.0)
                })

        for rel in relationships:
            # map table -> entity label
            from_table = rel['from_table']
            to_table = rel['to_table']
            from_label = None
            to_label = None
            # lookup labels from parsed_json
            for e in parsed_json.get('entities', []):
                if e['table_name'] == from_table:
                    from_label = e['name']
                if e['table_name'] == to_table:
                    to_label = e['name']
            if not from_label or not to_label:
                print(f"Cannot resolve labels for relationship {rel}. Skipping.")
                continue

            from_col = rel['from_column']
            to_col = rel['to_column']
            rel_type = rel.get('relationship_type', 'RELATED_TO').upper()

            # Create relationships by matching nodes on the join columns (prefer chosen_primary_key if available)
            # For performance, iterate rows in the "from" table and create relationships
            df_from = tables.get(from_table)
            if df_from is None:
                continue

            # find chosen pk for target
            to_entity = next((e for e in parsed_json.get('entities', []) if e['table_name'] == to_table), None)
            chosen_pk_to = (to_entity.get('chosen_primary_key') if to_entity else None) or (to_entity.get('primary_key_candidates')[0] if to_entity and to_entity.get('primary_key_candidates') else to_col)

            df_from = df_from.where(pd.notnull(df_from), None)
            records = df_from.to_dict(orient='records')
            tx = session.begin_transaction()
            for row in records:
                from_val = row.get(from_col)
                if from_val is None:
                    continue
                # We assume the "to" node has property `chosen_pk_to` equal to from_val
                # If mapping is different, Gemini's output should ideally indicate mapping explicitly
                cypher = (
                    f"MATCH (a:`{from_label}` {{`{from_col}`: $from_val}}) "
                    f"MATCH (b:`{to_label}` {{`{chosen_pk_to}`: $to_val}}) "
                    f"MERGE (a)-[r:`{rel_type}`]->(b)"
                )
                # For now use same value for $to_val and $from_val; Gemini may suggest different mapping
                tx.run(cypher, from_val=from_val, to_val=from_val)
            tx.commit()


def export_graph_image(output_path: str, limit=1000):
    """Query Neo4j for a sample subgraph and save a PNG image using NetworkX + matplotlib.
    """
    G = nx.DiGraph()
    with driver.session() as session:
        # get relationships and nodes (limited)
        query = f"MATCH (a)-[r]->(b) RETURN a, r, b LIMIT {limit}"
        result = session.run(query)
        for rec in result:
            a = rec['a']
            b = rec['b']
            r = rec['r']
            # build node keys
            a_labels = list(a.labels) if hasattr(a, 'labels') else []
            b_labels = list(b.labels) if hasattr(b, 'labels') else []
            a_props = dict(a)
            b_props = dict(b)
            a_key = f"{a_labels[0] if a_labels else 'Node'}:{a_props.get(next(iter(a_props), 'id'))}"
            b_key = f"{b_labels[0] if b_labels else 'Node'}:{b_props.get(next(iter(b_props), 'id'))}"
            # add nodes
            G.add_node(a_key, **a_props)
            G.add_node(b_key, **b_props)
            # add edge
            rel_type = getattr(r, 'type', None) or str(r)
            G.add_edge(a_key, b_key, label=rel_type)

        # also add isolated nodes if any
        iso_query = f"MATCH (n) WHERE NOT (n)--() RETURN n LIMIT {limit}"
        for rec in session.run(iso_query):
            n = rec['n']
            labels = list(n.labels) if hasattr(n, 'labels') else []
            props = dict(n)
            key = f"{labels[0] if labels else 'Node'}:{props.get(next(iter(props), 'id'))}"
            G.add_node(key, **props)

    if G.number_of_nodes() == 0:
        raise RuntimeError('No nodes found in Neo4j to draw.')

    # Draw
    plt.figure(figsize=(30, 20))
    pos = nx.spring_layout(G, k=0.5)
    labels = {n: n for n in G.nodes()}
    nx.draw(G, pos, with_labels=True, labels=labels, node_size=800, font_size=8)
    # draw edge labels
    edge_labels = nx.get_edge_attributes(G, 'label')
    if edge_labels:
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


@app.route('/ingest', methods=['POST'])
def ingest():
    """Main endpoint: accept files, call Gemini for insights, insert into Neo4j, export graph image.
    Returns JSON with status and path to saved image.
    """
    uploaded_files = request.files.getlist('files')
    if not uploaded_files:
        return jsonify({'error': 'No files provided. Use multipart form field name "files".'}), 400

    all_tables = {}
    for f in uploaded_files:
        try:
            tables = read_uploaded_file(f)
        except Exception as e:
            return jsonify({'error': f'Failed to read file {f.filename}: {str(e)}'}), 400
        all_tables.update(tables)

    schema_summary = build_schema_summary(all_tables)

    try:
        parsed = call_gemini_for_insights(schema_summary)
    except Exception as e:
        return jsonify({'error': f'Failed calling Gemini: {str(e)}'}), 500

    try:
        merge_data_into_neo4j(parsed, all_tables)
    except Exception as e:
        return jsonify({'error': f'Failed inserting data into Neo4j: {str(e)}'}), 500

    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    out_file = OUT_DIR / f'graph_{ts}.png'
    try:
        export_graph_image(str(out_file))
    except Exception as e:
        return jsonify({'error': f'Failed exporting graph image: {str(e)}'}), 500

    return jsonify({'status': 'ok', 'graph_image': str(out_file)})


if __name__ == '__main__':
    # verify Neo4j connectivity at startup
    try:
        driver.verify_connectivity()
        print('Connected to Neo4j successfully.')
    except Exception as e:
        print('Warning: could not verify connectivity to Neo4j at startup:', e)

    app.run(host='0.0.0.0', port=5000, debug=True)
