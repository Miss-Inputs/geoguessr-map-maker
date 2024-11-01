from collections.abc import Sequence
from typing import TYPE_CHECKING

import numpy
import pyproj

if TYPE_CHECKING:
	import pandas

geod = pyproj.Geod(ellps='WGS84')

# TODO: Less shite type hints


def geod_distance_and_bearing(
	lat1: 'float | Sequence[float] | numpy.ndarray | pandas.Series[float]',
	lng1: 'float | Sequence[float] | numpy.ndarray | pandas.Series[float]',
	lat2: 'float | Sequence[float] | numpy.ndarray | pandas.Series[float]',
	lng2: 'float | Sequence[float] | numpy.ndarray | pandas.Series[float]',
	*,
	radians: bool = False,
) -> tuple[float | numpy.ndarray, float | numpy.ndarray]:
	"""
	Returns:
		(Distance in metres, heading in degrees/radians) between point A and point B"""
	bearing, _, dist = geod.inv(lng1, lat1, lng2, lat2, radians=radians)
	if isinstance(bearing, list):
		# y u do this
		bearing = numpy.array(bearing)
	return (dist, bearing)


def geod_distance(
	lat1: float | list[float] | numpy.ndarray,
	lng1: float | list[float] | numpy.ndarray,
	lat2: float | list[float] | numpy.ndarray,
	lng2: float | list[float] | numpy.ndarray,
	*,
	radians: bool = False,
) -> float | numpy.ndarray:
	"""
	Returns:
		Distance in metres between point A and point B
	"""
	return geod_distance_and_bearing(lat1, lng1, lat2, lng2, radians=radians)[0]


def get_bearing(
	lat1: float | list[float] | numpy.ndarray,
	lng1: float | list[float] | numpy.ndarray,
	lat2: float | list[float] | numpy.ndarray,
	lng2: float | list[float] | numpy.ndarray,
	*,
	radians: bool = False,
) -> float | numpy.ndarray:
	"""
	Returns:
		Direction from point A to point B
	"""
	return geod_distance_and_bearing(lat1, lng1, lat2, lng2, radians=radians)[1]
