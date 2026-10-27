FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install --with-deps chromium

COPY email_finder.py web_app.py ./
COPY templates/ templates/
COPY domain_availability_checker/airtable_donor_status.py domain_availability_checker/instant_bulk_to_sheets.py domain_availability_checker/netim_bulk_to_sheets.py domain_availability_checker/rdap_availability_worker.py ./domain_availability_checker/

EXPOSE 10000

CMD ["gunicorn", "web_app:app", \
     "--bind", "0.0.0.0:10000", \
     "--workers", "1", \
     "--threads", "8", \
     "--timeout", "0", \
     "--keep-alive", "75", \
     "--worker-tmp-dir", "/dev/shm"]
