FROM python:3.12-slim
WORKDIR /srv
COPY smtp_relay.py .
ENV PYTHONUNBUFFERED=1
EXPOSE 2525 8081
CMD ["python", "smtp_relay.py"]
