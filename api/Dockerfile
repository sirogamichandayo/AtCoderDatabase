FROM python:3.12-slim

# 作業ディレクトリの作成
WORKDIR /app

COPY requirements.txt .
RUN chmod +x requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
RUN chmod +x main.py

ENTRYPOINT ["python", "./main.py"]
