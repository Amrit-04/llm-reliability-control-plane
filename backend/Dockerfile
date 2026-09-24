FROM python:3.13-slim
WORKDIR /app
COPY backend/pyproject.toml ./pyproject.toml
COPY backend/app ./app
RUN pip install --no-cache-dir .
ENV LRCP_DATA_DIR=/data
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
