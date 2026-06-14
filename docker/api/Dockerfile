#--STAGE 1 : Builder --------------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build
# build dependenct

RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#python dependencies into a local directory
COPY requirements/api.txt .
RUN pip install --no-cache-dir --prefix=/install -r api.txt

#--STAGE 2 : Runtime ------------------------------------------------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

WORKDIR /app

#create non root user
RUN useradd --create-home --shell /bin/bash/ apiuser

#copying installed packages from builder
COPY --from=builder /install /usr/local

#copy application code
COPY api/ ./api/
USER apiuser

EXPOSE 8000

CMD ["uvicorn","api.main:app","--host","0.0.0.0","--port","8000","--reload"]
