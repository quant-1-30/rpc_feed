# Use the official Python 3.11 image matching pyproject.toml (>=3.11,<3.15)
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip3 install --no-cache-dir poetry \
    && poetry install --no-root

COPY . .

RUN poetry run python setup.py build_ext --inplace

ENV PYTHONPATH=/app

CMD ["poetry", "run", "python", "rpc_feed/run_server.py"]
