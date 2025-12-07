import { Elysia } from "elysia";
import { env } from "../env";
import { fetchJson } from "../http";

const base = env.hiveUrl.replace(/\/$/, "");

export const hiveRoutes = new Elysia({ prefix: "/hive" }).post(
  "/query",
  async ({ body }) =>
    fetchJson(`${base}/`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
);

