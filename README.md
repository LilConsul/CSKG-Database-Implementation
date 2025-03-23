# Project 19: Graph Database Implementation
## Authors: Shevchenko Denys & Karabanov Yehor

## 📚 Technology Stack
- Dgraph - Open source, AI-ready graph database with horizontal scaling and GraphQL support
  - Provides ACID transactions, consistent replication, and linearizable reads
  - Native GraphQL integration optimizes disk arrangement for query performance
  - Reduces disk seeks and network calls in clustered environments

- Python - High-level programming language for application development
- Docker - Container platform for consistent deployment environments
- Bash - Unix shell for automation and setup scripts

## 🔧 Prerequisites
- Python 3.13+
  - Click - Python package for building elegant command-line interfaces
  - pydgraph - Official Python client for Dgraph database

## 🏗️ Architecture

<img src="./img/architecture.svg" alt="Architecture Diagram" style="width:100%; height:auto;"/>

### Components
1. Docker - Application containerization for consistent environments
2. Dgraph - Graph database running within Docker containers
3. Python - Implementation language for CLI and database interaction
4. Bash - Setup and initialization scripts

## 📐 Database Design

# Schema Definition and Details

## Dgraph Schema

```graphql
id: string @unique @index(hash) .
label: string @index(term) .
to: [uid] @reverse @facet(id, label) .

type Node {
    id
    label
    to
}
```

## Schema Visualization

<img src="./img/drgaph-shema.svg" alt="Schema Diagram" width="600" />

## Field Descriptions

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `id` | `string` | `@unique @index(hash)` | Unique identifier for each node. The hash index enables fast lookups by ID. |
| `label` | `string` | `@index(term)` | Descriptive label for the node. Term indexing supports text search functionality. |
| `to` | `[uid]` | `@reverse @facet(id, label)` | Array of references to connected nodes. The `@reverse` directive enables bidirectional navigation between nodes. Each connection includes facets (edge properties) storing `id` and `label` metadata. |

## Edge Properties (Facets)

Each edge in the `to` field contains additional properties:

- **id**: Unique identifier for the edge
- **label**: Descriptive label for the relationship

## Usage Example

```graphql
{
  nodes(func: eq(id, "root-node")) {
    id
    label
    to @facets {
      id
      label
      # Recursive traversal if needed
    }
  }
}
```

This schema design enables efficient graph traversal with metadata on both nodes and edges, supporting complex relationship modeling.

### Key Design Decisions
- Directed graph structure with rich relationship metadata
- Bidirectional navigation via @reverse directive
- Strategic indexing for optimized query performance
- Facets to store additional edge properties

## 🔄 Implementation Process
1. Data analysis to determine optimal technology selection
2. Database learning and requirement validation
3. Schema design and optimization for graph representation
4. Python converter development for data transformation
5. [Implementation details to be completed]

## 👥 Team Contributions
• Shevchenko Denys:
  - Docker configuration and containerization
  - Bulk data import functionality
  - Python converter optimization
  - Shell scripting for deployment

• Karabanov Yehor:
  - Database schema architecture
  - Python CLI implementation
  - Core database query development

## ⭐ Self-evaluation
10/10

[Installation and setup, query details, and run manual sections to be completed]