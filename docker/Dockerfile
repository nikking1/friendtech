FROM python:3.9-bullseye

RUN apt-get update && apt-get install --no-install-recommends -y \
   build-essential \
   libpq-dev

COPY ./requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

WORKDIR /app

COPY ./ /app

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["python", "./main.py"]
