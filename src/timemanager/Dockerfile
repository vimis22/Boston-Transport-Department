FROM ghcr.io/astral-sh/uv:alpine3.22

WORKDIR /app

COPY pyproject.toml uv.lock .

RUN uv sync

COPY main.py .

CMD ["uv", "run", "main.py"]