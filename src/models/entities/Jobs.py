from werkzeug.security import check_password_hash
from flask_login import UserMixin


class Jobs(UserMixin):

    def __init__(self, id, job) -> None:
        self.id = id
        self.job = job

    @classmethod
    def check_password(self, hashed_password, password):
        return check_password_hash(hashed_password, password)

    @classmethod
    def check_dict(self):
        return {'id': self.id, 'job': self.job}
        #return check_password_hash(hashed_password, password)