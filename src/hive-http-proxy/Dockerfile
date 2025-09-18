FROM ghcr.io/astral-sh/uv:debian-slim

WORKDIR /app

COPY pyproject.toml uv.lock .

RUN uv sync

COPY main.py ./

CMD ["uv", "run", "main.py"]