import React from "react";
import { Card } from "../../props/Card";
import { BarChart, Bar, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from "recharts";
import { BIKE_WEATHER_PLACEHOLDER_DATA } from "../../../references/MockData";

export function BikeVsWeather() {
  return (
    <Card title="Bike vs Weather (placeholder)">
      <p className="mb-3 text-sm text-slate-600">
        Placeholder chart wired for future ETL outputs; swap the data source
        to new Kafka topics when ready.
      </p>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={BIKE_WEATHER_PLACEHOLDER_DATA}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="label" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="bikes" fill="#0f172a" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
