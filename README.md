# 🚀 Intelligent SQL Query Optimizer & Index Advisor

[![Language](https://img.shields.io/badge/Language-Python_&_React-blue.svg)](https://python.org)
[![Frameworks](https://img.shields.io/badge/Frameworks-FastAPI_&_React-green.svg)](https://fastapi.tiangolo.com/)
[![DevOps](https://img.shields.io/badge/DevOps-Docker_&_PostgreSQL-brightgreen.svg)](https://docker.com)

A full-stack web application designed to analyze and optimize SQL queries. This tool parses raw SQL, identifies performance anti-patterns, visualizes execution plans, and provides actionable index and rewrite recommendations to improve database performance.



## Core Features

* **⚙️ Multi-Stage Analysis Engine:** Deconstructs queries into an Abstract Syntax Tree (AST) to analyze clauses, identify anti-patterns (`SELECT *`, functions on columns), and detect inefficient subqueries.
* **💡 Intelligent Index Advisor:** Recommends optimal single-column and composite indexes for `WHERE`, `JOIN`, and `ORDER BY` clauses to accelerate data retrieval.
* **🧠 ML-Powered Predictions:** Integrates a Scikit-learn model to provide data-driven predictions on the potential performance impact of each recommendation.
* **🌳 Interactive Execution Plan Visualization:** Connects to a live PostgreSQL database to generate and render the actual query execution plan as an interactive tree diagram using `react-d3-tree`.
* **🚀 Query Rewrite Suggestions:** Provides suggestions for rewriting inefficient queries, such as converting `UNION` to `UNION ALL` or `IN`-clause subqueries to more performant `JOIN` operations.
* **📊 Performance Benchmarking:** Includes a module to run original and optimized queries against a synthetic dataset to provide quantifiable proof of performance improvements.

---
## Tech Stack

This project is a containerized, multi-service application with a clear separation between the backend, frontend, and database.

| Category      | Technology                    | Purpose                                                  |
| :------------ | :---------------------------- | :------------------------------------------------------- |
| **Backend** | Python, FastAPI, Uvicorn      | High-performance, asynchronous REST API                  |
|               | SQLParse, Scikit-learn, Pandas | SQL parsing, ML modeling, and data handling              |
| **Frontend** | React, TypeScript             | Interactive, type-safe user interface                    |
|               | Monaco Editor, React Query    | Professional SQL editor and server state management      |
|               | D3.js (via `react-d3-tree`)   | Interactive tree diagram for query plan visualization    |
| **Database** | PostgreSQL                    | Primary database for execution plans and ML data storage |
| **DevOps** | Docker, Docker Compose        | Containerization and orchestration of the entire application stack |
|               | Nginx                         | High-performance web server for serving the production frontend |

---
## Local Setup & Installation

### Prerequisites
* Git
* Python 3.10+
* Node.js 18+
* Docker & Docker Compose

### Running with Docker (Recommended)

This is the simplest way to run the entire application, including the database.

1.  **Clone the repository:**
    ```bash
    git clone <YOUR_GITHUB_REPOSITORY_URL>
    cd <repository-name>
    ```

2.  **Create an environment file:**
    Create a `.env` file in the project root and add the following:
    ```env
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=mysecretpassword
    POSTGRES_DB=sqldb
    ```

3.  **Build and run the stack:**
    ```bash
    docker-compose up --build
    ```

4.  **Access the application:**
    * **Frontend:** `http://localhost:3000`
    * **Backend API:** `http://localhost:8000`
    * **PostgreSQL Database:** Connect on port `5432`

---
## Usage

1.  Navigate to `http://localhost:3000`.
2.  Enter a SQL query into the editor.
3.  Click **"Analyze Query"** to see anti-patterns and index/rewrite recommendations.
4.  If connected to a PostgreSQL database, click **"Visualize Execution Plan"** to render the query's execution tree.
5.  The **ML Prediction** badge on index recommendations provides a data-driven impact assessment.
