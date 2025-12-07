import { Elysia } from "elysia";
import { staticPlugin } from "@elysiajs/static";
import { cors } from "@elysiajs/cors";
import { join } from "path";
import { env, topics } from "./env";
import { timemanagerRoutes } from "./routes/timemanager";
import { hiveRoutes } from "./routes/hive";
import { kafkaRoutes } from "./routes/kafka";

const publicDir = new URL("../public", import.meta.url).pathname;
const indexFile = join(publicDir, "index.html");

const app = new Elysia()
  .use(cors())
  .get("/health", () => ({
    status: "ok",
    timestamp: new Date().toISOString(),
  }))
  .get("/api/config", () => ({
    endpoints: {
      timemanager: "/api/timemanager",
      hive: "/api/hive",
      kafka: "/api/kafka",
    },
    topics,
  }))
  .group("/api", (api) =>
    api.use(timemanagerRoutes).use(hiveRoutes).use(kafkaRoutes)
  )
  .use(
    staticPlugin({
      assets: publicDir,
      prefix: "/",
    })
  )
  .get("*", () => Bun.file(indexFile))
  .listen({
    hostname: "0.0.0.0",
    port: env.port,
  });

console.log(`Dashboard server listening on http://0.0.0.0:${env.port}`);

