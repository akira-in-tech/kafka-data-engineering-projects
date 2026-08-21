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

df["Initial Hire Date"] = pd.to_datetime(
    df["Initial Hire Date"],
    format="%d-%b-%Y"
)

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
        "salary": math.floor(float(row["Salary"]))
    }

    producer.send(TOPIC, employee)

producer.flush()
producer.close()

print(f"Sent {len(df)} employees to Kafka")