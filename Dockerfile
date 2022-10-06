FROM python:3.10
RUN pip install pymongo minio tqdm
WORKDIR /src
COPY . .
ENTRYPOINT ["python", "src/main.py"]
