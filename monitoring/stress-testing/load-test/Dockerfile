# stress-testing/load-test/Dockerfile

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_test.py .

ENTRYPOINT ["python", "load_test.py"]
CMD ["--help"]
