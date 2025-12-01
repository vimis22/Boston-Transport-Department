"""
Time Manager Service
Coordinates simulation clock for replaying historical transportation data.
Provides REST API for clock control and synchronization.
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from starlette.responses import RedirectResponse


class SimulationClock:
    """Manages the simulation clock state"""

    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.current_time = datetime.fromisoformat("2018-01-01T00:00:00")
        self.speed = 1.0
        self.last_update = datetime.now(timezone.utc)
        logging.getLogger(__name__).info(f"Simulation initialized with run_id: {self.run_id}")

    def get_state(self) -> "SimulationState":
        """Get current simulation state"""
        return SimulationState(
            run_id=self.run_id,
            current_time=self.current_time.isoformat(),
            speed=self.speed,
            last_update=self.last_update.isoformat(),
        )

    def set_speed(self, speed: float):
        """Set simulation speed (0 = paused)"""
        self._update_time()
        self.speed = speed
        logging.getLogger(__name__).info(f"Speed set to {speed}x")

    def set_time(self, new_time: str):
        """Set simulation time (generates new run_id)"""
        self.current_time = datetime.fromisoformat(new_time.replace("Z", "+00:00"))
        self.last_update = datetime.now(timezone.utc)
        self.run_id = str(uuid.uuid4())
        logging.getLogger(__name__).info(f"Time set to {new_time}, new run_id: {self.run_id}")

    def tick(self):
        """Update simulation time based on speed"""
        self._update_time()

    def _update_time(self):
        """Internal method to update time"""
        if self.speed == 0:
            self.last_update = datetime.now(timezone.utc)
            return

        now = datetime.now(timezone.utc)
        elapsed_real = (now - self.last_update).total_seconds()
        simulated_advancement = elapsed_real * self.speed

        self.current_time += timedelta(seconds=simulated_advancement)
        self.last_update = now


async def clock_ticker(clock: SimulationClock):
    """Background task to advance simulation clock"""
    logger = logging.getLogger(__name__)
    logger.info("Clock ticker started")
    while True:
        try:
            clock.tick()
            await asyncio.sleep(0.1)  # Update every 100ms
        except Exception as e:
            logger.error(f"Clock ticker error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    logger = logging.getLogger(__name__)
    # Startup: Create clock and start ticker task
    clock = SimulationClock()
    app.state.simulation_clock = clock
    task = asyncio.create_task(clock_ticker(clock))
    logger.info("Application startup complete")
    yield
    # Shutdown: cancel background task
    task.cancel()
    logger.info("Application shutdown")


app = FastAPI(
    title="Time Manager Service",
    description="Coordinates simulation clock for replaying historical transportation data",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure logging - use force=True to avoid duplicate handlers when running under uvicorn
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s", force=True)
logger = logging.getLogger(__name__)


class SimulationState(BaseModel):
    run_id: str
    current_time: str
    speed: float
    last_update: str


class SetSpeedRequest(BaseModel):
    speed: float


class SetTimeRequest(BaseModel):
    time: str


def get_simulation_clock() -> SimulationClock:
    """Dependency to get the simulation clock instance"""
    return app.state.simulation_clock


ClockDep = Annotated[SimulationClock, Depends(get_simulation_clock)]


# API Endpoints
@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/api/v1/clock/state", response_model=SimulationState)
def get_state(clock: ClockDep):
    """Get current simulation state"""
    return clock.get_state()


@app.put("/api/v1/clock/speed")
def set_speed(request: SetSpeedRequest, clock: ClockDep):
    """Set simulation speed (0 = paused)"""
    if request.speed < 0:
        raise HTTPException(status_code=400, detail="Speed must be non-negative")

    clock.set_speed(request.speed)
    return {
        "message": f"Speed set to {request.speed}x",
        "state": clock.get_state(),
    }


@app.put("/api/v1/clock/time")
def set_time(request: SetTimeRequest, clock: ClockDep):
    """Set simulation time (generates new run_id)"""
    try:
        clock.set_time(request.time)
        return {"message": "Time updated", "state": clock.get_state()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app, host="0.0.0.0", port=8000, reload=False, log_level="debug", workers=1
    )
