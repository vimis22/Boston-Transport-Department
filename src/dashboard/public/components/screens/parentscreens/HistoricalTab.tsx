import React from "react";
import { TaxiTripsByHour } from "../childscreens/TaxiTripsByHour";
import { BikeVsWeather } from "../childscreens/BikeVsWeather";

export function HistoricalTab() {
  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <TaxiTripsByHour />
      <BikeVsWeather />
    </div>
  );
}
