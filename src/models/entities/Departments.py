from werkzeug.security import check_password_hash
from flask_login import UserMixin


class Departments(UserMixin):

    def __init__(self, id, department) -> None:
        self.id = id
        self.department = department

    @classmethod
    def check_password(self, hashed_password, password):
        return check_password_hash(hashed_password, password)

    @classmethod
    def check_dict(self):
        return {'id': self.id, 'department': self.department}
        #return check_password_hash(hashed_password, password)