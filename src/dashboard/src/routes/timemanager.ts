import { Elysia } from "elysia";
import { env } from "../env";
import { fetchJson } from "../http";

const base = `${env.timemanagerUrl.replace(/\/$/, "")}/api/v1/clock`;

export const timemanagerRoutes = new Elysia({ prefix: "/timemanager" })
  .get("/clock/state", async () => fetchJson(`${base}/state`))
  .put("/clock/speed", async ({ body }) =>
    fetchJson(`${base}/speed`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  )
  .put("/clock/time", async ({ body }) =>
    fetchJson(`${base}/time`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  );

