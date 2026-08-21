from dataclasses import dataclass
from typing import Optional


# The producer reads raw (index-based) tuples out of emp_cdc via psycopg2.
# Wrapping each row in this dataclass gives it field names and a single
# `to_dict()` -- the one place that defines the Kafka message shape the
# consumer relies on -- instead of building the JSON payload ad hoc at the
# call site.
@dataclass
class EmployeeChange:
    """One row out of emp_cdc, with named fields instead of a raw index-based tuple."""

    cdc_id: int
    emp_id: int
    first_name: Optional[str]
    last_name: Optional[str]
    dob: Optional[str]
    city: Optional[str]
    salary: Optional[int]
    action: str

    def to_dict(self):
        """Convert this change into the plain dict the producer sends as JSON."""
        return {
            "cdc_id": self.cdc_id,
            "emp_id": self.emp_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "dob": self.dob,
            "city": self.city,
            "salary": self.salary,
            "action": self.action,
        }
