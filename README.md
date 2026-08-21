
# Kafka Data Engineering Projects

This repository contains two Kafka projects demonstrating batch ETL and near-real-time Change Data Capture (CDC).

## Project 1 - Kafka ETL Pipeline

Architecture:

Employee CSV → Producer → Kafka → Consumer → PostgreSQL

The producer:

- Reads Employee_Salaries.csv
- Keeps ECC, CIT, and EMS departments
- Filters employees hired after 2010
- Rounds salary down
- Sends employee records to Kafka

The consumer:

- Reads messages from Kafka
- Loads employee records into PostgreSQL
- Maintains total salary by department

Expected result:

| Department | Total Salary |
| ---------- | -----------: |
| CIT        |    9,102,142 |
| ECC        |    2,042,698 |
| EMS        |    3,779,570 |

## Project 2 - Kafka CDC Pipeline

Architecture:

Source PostgreSQL
→ Trigger
→ CDC Table
→ Producer
→ Kafka
→ Consumer
→ Destination PostgreSQL

Project 2 supports:

- Initial snapshot synchronization
- INSERT replication
- UPDATE replication
- DELETE replication
- Persistent CDC position tracking
- Kafka topic administration
- PostgreSQL triggers and functions
- Near-real-time synchronization

## Technologies

- Python
- Apache Kafka
- PostgreSQL
- Docker Compose
- Kafka UI
- psycopg2
- kafka-python
- pandas

## Project Structure

```text
kafka-projects/
├── project1/
│   ├── admin.py
│   ├── producer.py
│   ├── consumer.py
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── resources/
│       └── Employee_Salaries.csv
│
├── project2/
│   ├── admin.py
│   ├── producer.py
│   ├── consumer.py
│   ├── employee.py
│   ├── init-db.sql
│   ├── init-destination.sql
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── .gitignore
└── README.md
```
