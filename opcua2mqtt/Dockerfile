FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY opcua_to_emqx.py /app/opcua_to_emqx.py

CMD ["python", "/app/opcua_to_emqx.py"]
