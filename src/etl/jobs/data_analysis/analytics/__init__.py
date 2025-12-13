"""
Core analytics computations for weather-transport-safety correlations.

This module implements domain-specific analytics including:
1. Weather-transport correlation metrics
2. Weather-safety risk analysis
3. Transport elasticity calculations
4. Predictive risk scores

This parent module delegates to specialized logic_children modules
following Separation of Concerns and SOLID principles.
"""

# Import all analytics functions from the analytics module
from .calculate_weather_transport_correlation import (
    calculate_weather_transport_correlation,
)
from .calculate_weather_safety_risk import calculate_weather_safety_risk
from .calculate_surge_weather_correlation import calculate_surge_weather_correlation
from .generate_transport_usage_summary import generate_transport_usage_summary
from .calculate_pearson_correlations import calculate_pearson_correlations
from .calculate_binned_weather_aggregations import calculate_binned_weather_aggregations
from .calculate_precipitation_impact_analysis import (
    calculate_precipitation_impact_analysis,
)
from .calculate_temporal_segmented_correlations import (
    calculate_temporal_segmented_correlations,
)
from .calculate_multi_variable_correlation_summary import (
    calculate_multi_variable_correlation_summary,
)
from .calculate_accident_weather_correlation import (
    calculate_accident_weather_correlation,
)

# Re-export all functions to maintain backward compatibility
__all__ = [
    "calculate_weather_transport_correlation",
    "calculate_weather_safety_risk",
    "calculate_surge_weather_correlation",
    "generate_transport_usage_summary",
    "calculate_pearson_correlations",
    "calculate_binned_weather_aggregations",
    "calculate_precipitation_impact_analysis",
    "calculate_temporal_segmented_correlations",
    "calculate_multi_variable_correlation_summary",
    "calculate_accident_weather_correlation",
]
