FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src

FROM base AS test
RUN pip install --no-cache-dir . "pytest>=8.3,<9" "pytest-aiohttp>=1.1,<2" "ruff>=0.11,<1"
COPY tests ./tests
CMD ["sh", "-c", "ruff check src tests && pytest -q"]

FROM base AS app
RUN pip install --no-cache-dir . && useradd --uid 1000 --create-home app && mkdir /data && chown app /data
USER app
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/_proxy/status', timeout=4)"
CMD ["xcbuffer"]
