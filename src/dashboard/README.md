Boston Transport Dashboard (UI)

This folder contains a lightweight React (Vite + TypeScript) user interface to visualize data produced by the Python jobs in this repository (ETL/Spark/Kafka streamers).

Quick start
- Open a terminal in this folder: src/dashboard
- Install dependencies: npm install
- Start the dev server: npm run dev (or npm start)
- Open your browser to the URL printed by Vite (typically http://localhost:5173)

What you’ll see
- A simple dashboard with three sample visualizations (weather, taxi, bike) built using Recharts.
- Dark/Light theme toggle in the navbar.

Wiring real data
- App.tsx currently uses mock arrays for weatherData, taxiData, and bikeData.
- Replace those arrays with data loaded from your Python services or generated files. Example with a local HTTP API:

  useEffect(() => {
    fetch('http://localhost:8000/api/weather')
      .then(r => r.json())
      .then(setWeatherData)
  }, [])

- You can implement tiny proxy endpoints or host static JSON files produced by your ETL/Spark jobs.

Project structure
- package.json: Scripts and dependencies
- index.html: Vite HTML entry
- vite.config.ts: Vite configuration
- tsconfig.json: TypeScript configuration
- src/main.tsx: React entry point
- App.tsx: Main dashboard layout and charts
- components/: Navbar and Buttons components

Notes
- This setup is intentionally minimal and framework-agnostic. No global CSS or external UI toolkit—just inline styles and Recharts.
- For production, run npm run build and serve the dist/ folder Vite generates.
