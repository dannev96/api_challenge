


FROM python:3

COPY ./src/requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENV FLASK_APP=./src/app.py

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]