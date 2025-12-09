FROM oven/bun:1.3-alpine

WORKDIR /app

# Cache packages installation
COPY package.json package.json
COPY bun.lock bun.lock

RUN bun install

# Copy source files
COPY ./src ./src
COPY ./public ./public
COPY tsconfig.json tsconfig.json

ENV NODE_ENV=production
ENV PORT=3000

# Build tailwind CSS
RUN bun run tailwind:build

# Build the application
RUN bun build --outdir=out --target=node src/index.ts

EXPOSE 3000

CMD ["bun", "out/index.js"]