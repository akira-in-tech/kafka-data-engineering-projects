from dataclasses import dataclass
from typing import Optional


@dataclass
class EmployeeChange:
    cdc_id: int
    emp_id: int
    first_name: Optional[str]
    last_name: Optional[str]
    dob: Optional[str]
    city: Optional[str]
    salary: Optional[int]
    action: str

    def to_dict(self):
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