from werkzeug.security import check_password_hash
from flask_login import UserMixin


class HiredEmployees(UserMixin):

    def __init__(self, id, name, datetime, department_id,job_id) -> None:
        self.id = id
        self.name = name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

    @classmethod
    def check_password(self, hashed_password, password):
        return check_password_hash(hashed_password, password)

    @classmethod
    def check_dict(self):
        return {'id': self.id, 'name': self.name, 'datetime': self.datetime, 'department_id': self.department_id, 'job_id': self.job_id}
        #return check_password_hash(hashed_password, password)