"""
Time Manager Service
Coordinates simulation clock for replaying historical transportation data.
Provides REST API for clock control and synchronization.
"""

from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import redis
import os
import logging
from threading import Thread
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Redis connection
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Simulation defaults
DEFAULT_START_TIME = "2018-01-01T00:00:00"
DEFAULT_END_TIME = "2018-12-31T23:59:59"
DEFAULT_SPEED = 1.0  # 1x real-time

class SimulationClock:
    """Manages the simulation clock state in Redis"""

    @staticmethod
    def initialize():
        """Initialize simulation with default values"""
        if not redis_client.exists('sim:current_time'):
            redis_client.set('sim:current_time', DEFAULT_START_TIME)
            redis_client.set('sim:start_time', DEFAULT_START_TIME)
            redis_client.set('sim:end_time', DEFAULT_END_TIME)
            redis_client.set('sim:speed', DEFAULT_SPEED)
            redis_client.set('sim:state', 'stopped')
            redis_client.set('sim:last_update', datetime.utcnow().isoformat())
            logger.info(f"Simulation initialized: {DEFAULT_START_TIME} to {DEFAULT_END_TIME}")

    @staticmethod
    def get_status():
        """Get current simulation status"""
        return {
            'state': redis_client.get('sim:state'),
            'current_time': redis_client.get('sim:current_time'),
            'start_time': redis_client.get('sim:start_time'),
            'end_time': redis_client.get('sim:end_time'),
            'speed': float(redis_client.get('sim:speed')),
            'last_update': redis_client.get('sim:last_update')
        }

    @staticmethod
    def start():
        """Start the simulation clock"""
        redis_client.set('sim:state', 'running')
        redis_client.set('sim:last_update', datetime.utcnow().isoformat())
        logger.info("Simulation started")

    @staticmethod
    def stop():
        """Stop the simulation clock"""
        redis_client.set('sim:state', 'stopped')
        logger.info("Simulation stopped")

    @staticmethod
    def reset():
        """Reset simulation to start time"""
        start_time = redis_client.get('sim:start_time')
        redis_client.set('sim:current_time', start_time)
        redis_client.set('sim:state', 'stopped')
        redis_client.set('sim:last_update', datetime.utcnow().isoformat())
        logger.info(f"Simulation reset to {start_time}")

    @staticmethod
    def set_speed(speed: float):
        """Set simulation speed multiplier"""
        if speed <= 0:
            raise ValueError("Speed must be positive")
        redis_client.set('sim:speed', speed)
        logger.info(f"Simulation speed set to {speed}x")

    @staticmethod
    def set_time_range(start_time: str, end_time: str):
        """Set simulation time range"""
        # Validate datetime format
        datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        datetime.fromisoformat(end_time.replace('Z', '+00:00'))

        redis_client.set('sim:start_time', start_time)
        redis_client.set('sim:end_time', end_time)
        redis_client.set('sim:current_time', start_time)
        logger.info(f"Time range set: {start_time} to {end_time}")

    @staticmethod
    def tick():
        """Update simulation time based on speed"""
        state = redis_client.get('sim:state')
        if state != 'running':
            return

        try:
            # Get current values
            current_time_str = redis_client.get('sim:current_time')
            last_update_str = redis_client.get('sim:last_update')
            speed = float(redis_client.get('sim:speed'))
            end_time_str = redis_client.get('sim:end_time')

            # Parse times
            current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))
            last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))

            # Calculate elapsed real time
            now = datetime.utcnow()
            elapsed_real = (now - last_update).total_seconds()

            # Calculate simulated time advancement
            simulated_advancement = elapsed_real * speed
            new_time = current_time + timedelta(seconds=simulated_advancement)

            # Check if simulation has ended
            if new_time >= end_time:
                new_time = end_time
                redis_client.set('sim:state', 'stopped')
                logger.info("Simulation completed")

            # Update Redis
            redis_client.set('sim:current_time', new_time.isoformat())
            redis_client.set('sim:last_update', now.isoformat())

        except Exception as e:
            logger.error(f"Error in clock tick: {e}")

def clock_ticker():
    """Background thread to advance simulation clock"""
    logger.info("Clock ticker thread started")
    while True:
        try:
            SimulationClock.tick()
        except Exception as e:
            logger.error(f"Clock ticker error: {e}")
        time.sleep(0.1)  # Update every 100ms

# API Endpoints

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        redis_client.ping()
        return jsonify({'status': 'healthy', 'redis': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

@app.route('/api/v1/clock/status', methods=['GET'])
def get_status():
    """Get current simulation status"""
    try:
        status = SimulationClock.get_status()
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/time', methods=['GET'])
def get_current_time():
    """Get current simulation time"""
    try:
        current_time = redis_client.get('sim:current_time')
        return jsonify({'current_time': current_time}), 200
    except Exception as e:
        logger.error(f"Error getting time: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/start', methods=['POST'])
def start_simulation():
    """Start the simulation"""
    try:
        SimulationClock.start()
        return jsonify({'message': 'Simulation started', 'status': SimulationClock.get_status()}), 200
    except Exception as e:
        logger.error(f"Error starting simulation: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/stop', methods=['POST'])
def stop_simulation():
    """Stop the simulation"""
    try:
        SimulationClock.stop()
        return jsonify({'message': 'Simulation stopped', 'status': SimulationClock.get_status()}), 200
    except Exception as e:
        logger.error(f"Error stopping simulation: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/reset', methods=['POST'])
def reset_simulation():
    """Reset simulation to start time"""
    try:
        SimulationClock.reset()
        return jsonify({'message': 'Simulation reset', 'status': SimulationClock.get_status()}), 200
    except Exception as e:
        logger.error(f"Error resetting simulation: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/speed', methods=['PUT'])
def set_speed():
    """Set simulation speed multiplier"""
    try:
        data = request.get_json()
        speed = data.get('speed')

        if speed is None:
            return jsonify({'error': 'speed parameter required'}), 400

        speed = float(speed)
        SimulationClock.set_speed(speed)

        return jsonify({'message': f'Speed set to {speed}x', 'status': SimulationClock.get_status()}), 200
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error setting speed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/clock/range', methods=['PUT'])
def set_time_range():
    """Set simulation time range"""
    try:
        data = request.get_json()
        start_time = data.get('start_time')
        end_time = data.get('end_time')

        if not start_time or not end_time:
            return jsonify({'error': 'start_time and end_time required'}), 400

        SimulationClock.set_time_range(start_time, end_time)

        return jsonify({'message': 'Time range updated', 'status': SimulationClock.get_status()}), 200
    except ValueError as e:
        return jsonify({'error': f'Invalid datetime format: {e}'}), 400
    except Exception as e:
        logger.error(f"Error setting time range: {e}")
        return jsonify({'error': str(e)}), 500

# Initialize simulation when module loads (before gunicorn forks workers)
SimulationClock.initialize()

# Start clock ticker thread
ticker_thread = Thread(target=clock_ticker, daemon=True)
ticker_thread.start()

if __name__ == '__main__':
    # Run Flask app directly (for development)
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
