import json
import math
import pandas as pd
from kafka import KafkaProducer


TOPIC = "employee_salary"


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


df = pd.read_csv("resources/Employee_Salaries.csv")

# Hire dates arrive as "10-Sep-1984" strings. Parsing once, up front, lets
# us both filter by year and reformat to ISO for the DB from the same
# column instead of re-parsing per row.
df["Initial Hire Date"] = pd.to_datetime(
    df["Initial Hire Date"],
    format="%d-%b-%Y"
)

# Spec: keep only ECC/CIT/EMS, and only employees hired in 2010 or later.
# Filtering with a vectorized pandas mask (instead of looping + if-checks)
# is both the idiomatic pandas way and avoids parsing/comparing dates for
# every one of the CSV's other departments we don't care about.
df = df[
    df["Department"].isin(["ECC", "CIT", "EMS"])
    & (df["Initial Hire Date"].dt.year >= 2010)
]


for _, row in df.iterrows():

    employee = {
        "department": row["Department"],
        "department_division": row["Department Division"],
        "position_title": row["Position Title"],
        "hire_date": row["Initial Hire Date"].strftime("%Y-%m-%d"),
        # Spec says "round off the salary to lower number" -> floor(),
        # not round(). float() first since pandas may hand back numpy
        # float types that math.floor doesn't accept directly.
        "salary": math.floor(float(row["Salary"]))
    }

    producer.send(TOPIC, employee)

# flush() blocks until every buffered message is acknowledged by the
# broker, so "Sent N employees" below is only printed once it's actually
# true, not just once it's been handed to the client library's buffer.
producer.flush()
producer.close()

print(f"Sent {len(df)} employees to Kafka")
