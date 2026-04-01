FROM python:3.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /api_gsheet_project


RUN apt-get update \
    && apt-get install -y --no-install-recommends bash sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN chmod +x trigger.sh

CMD ["bash", "trigger.sh"]
