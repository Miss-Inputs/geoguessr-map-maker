from collections.abc import Sequence
from typing import overload

import numpy
import pyproj

geod = pyproj.Geod(ellps='WGS84')


@overload
def geod_distance_and_bearing(
	lat1: float, lng1: float, lat2: float, lng2: float, *, radians: bool = False
) -> tuple[float, float]: ...


@overload
def geod_distance_and_bearing(
	lat1: Sequence[float],
	lng1: Sequence[float],
	lat2: Sequence[float],
	lng2: Sequence[float],
	*,
	radians: bool = False,
) -> tuple[numpy.ndarray, numpy.ndarray]: ...


def geod_distance_and_bearing(
	lat1: float | Sequence[float],
	lng1: float | Sequence[float],
	lat2: float | Sequence[float],
	lng2: float | Sequence[float],
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


@overload
def geod_distance(
	lat1: float, lng1: float, lat2: float, lng2: float, *, radians: bool = False
) -> float: ...


@overload
def geod_distance(
	lat1: Sequence[float],
	lng1: Sequence[float],
	lat2: Sequence[float],
	lng2: Sequence[float],
	*,
	radians: bool = False,
) -> numpy.ndarray: ...


# It doesn't seem to like it if I type any of the positional params here as float | Sequence[float]
def geod_distance(lat1, lng1, lat2, lng2, *, radians: bool = False) -> float | numpy.ndarray:
	"""
	Returns:
		Distance in metres between point A and point B
	"""
	return geod_distance_and_bearing(lat1, lng1, lat2, lng2, radians=radians)[0]


@overload
def get_bearing(
	lat1: float, lng1: float, lat2: float, lng2: float, *, radians: bool = False
) -> float: ...


@overload
def get_bearing(
	lat1: Sequence[float],
	lng1: Sequence[float],
	lat2: Sequence[float],
	lng2: Sequence[float],
	*,
	radians: bool = False,
) -> float | numpy.ndarray: ...


def get_bearing(lat1, lng1, lat2, lng2, *, radians: bool = False) -> float | numpy.ndarray:
	"""
	Returns:
		Direction from point A to point B
	"""
	return geod_distance_and_bearing(lat1, lng1, lat2, lng2, radians=radians)[1]
