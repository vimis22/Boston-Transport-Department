FROM ghcr.io/astral-sh/uv:debian-slim

WORKDIR /app

COPY pyproject.toml uv.lock .

RUN uv sync

COPY main.py config.py ./
COPY streamers/ ./streamers/
COPY utils/ ./utils/

CMD ["uv", "run", "main.py"]