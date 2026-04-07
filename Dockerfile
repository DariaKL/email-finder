FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY email_finder.py web_app.py ./
COPY templates/ templates/

EXPOSE 10000

CMD ["gunicorn", "web_app:app", \
     "--bind", "0.0.0.0:10000", \
     "--workers", "1", \
     "--threads", "8", \
     "--timeout", "600"]
