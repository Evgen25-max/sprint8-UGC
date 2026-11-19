FROM python:3.12-slim
WORKDIR /opt/app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/app/src
RUN groupadd -r web && useradd -r -g web web
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /opt/app/src/logs && chown -R web:web /opt/app
COPY --chown=web:web src/ ./src/
COPY --chown=web:web migrations/ ./migrations/
USER web
WORKDIR /opt/app/src
EXPOSE 9101
CMD ["python", "main.py"]
