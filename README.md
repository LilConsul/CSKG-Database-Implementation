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

<img src="./img/architecture.svg" alt="Architecture Diagram" width="800"/>

### Components
1. Docker - Application containerization for consistent environments
2. Dgraph - Graph database running within Docker containers
3. Python - Implementation language for CLI and database interaction
4. Bash - Setup and initialization scripts

## 📐 Database Design

### GraphQL Type Definition
```graphql
type Node {
  id: string @unique @index(exact, term)
  label: string @index(hash)
  to: [uid] @reverse @facets(id, label)
}
```

### Schema Details
• id: string
  - Unique identifier for each node
  - Constraints: @unique, @index(exact, term)
  - Ensures exact and term-based lookups

• label: string
  - Descriptive label for each node
  - Constraints: @index(hash)
  - Enables fast equality-based searches

• to: [uid]
  - References to connected nodes
  - Constraints: @reverse, @facets(id, label)
  - Supports bidirectional relationships with metadata

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