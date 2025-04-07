import logging
from collections.abc import Collection
from functools import partial

import numpy
import shapely
import shapely.ops
from pyproj import CRS, Transformer
from pyproj.enums import TransformDirection

logger = logging.getLogger(name=__name__)

wgs84 = CRS('wgs84')
mercator = CRS('Web Mercator')
wgs84_to_mercator = Transformer.from_crs(wgs84, mercator, always_xy=True)
mercator_to_wgs84 = partial(wgs84_to_mercator.transform, direction=TransformDirection.INVERSE)


def get_polygon_lattice(
	poly: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing,
	resolution: float = 10,
	*,
	reproject: bool = True,
) -> Collection[shapely.Point]:
	"""Returns points from a grid covering a polygon

	Parameters:
		reproject: If true, temporarily projects to mercator to make the resolution consistent
		resolution: Grid resolution in metres
	"""
	projected = shapely.ops.transform(wgs84_to_mercator.transform, poly) if reproject else poly
	shapely.prepare(projected)
	min_x, min_y, max_x, max_y = projected.bounds

	x, y = numpy.meshgrid(
		numpy.arange(min_x, max_x, resolution, dtype='float64'),
		numpy.arange(min_y, max_y, resolution, dtype='float64'),
	)
	points = shapely.MultiPoint(list(zip(x.flat, y.flat, strict=True)))
	intersection = points.intersection(projected)
	if intersection.is_empty:
		# maybe this could happen if polygon is less than radius in either dimension
		return ()

	if reproject:
		intersection = shapely.ops.transform(mercator_to_wgs84, intersection)
	if isinstance(intersection, shapely.MultiPoint):
		return tuple(intersection.geoms)
	if not isinstance(intersection, shapely.Point):
		logger.info('Somehow the intersection was a %s, returning empty list of points')
		return ()
	return (intersection,)


def random_point_in_bbox(
	min_x: float,
	min_y: float,
	max_x: float,
	max_y: float,
	random: numpy.random.Generator | int | None = None,
) -> shapely.Point:
	"""Uniformly generates a point somewhere in a bounding box."""
	if not isinstance(random, numpy.random.Generator):
		random = numpy.random.default_rng(random)
	x = random.uniform(min_x, max_x)
	y = random.uniform(min_y, max_y)
	return shapely.Point(x, y)


def random_points_in_bbox(
	min_x: float,
	min_y: float,
	max_x: float,
	max_y: float,
	n: int,
	random: numpy.random.Generator | int | None = None,
) -> numpy.ndarray:
	"""Uniformly generates several points somewhere in a bounding box."""
	if not isinstance(random, numpy.random.Generator):
		random = numpy.random.default_rng(random)
	x = random.uniform(min_x, max_x, n)
	y = random.uniform(min_y, max_y, n)
	points = shapely.points(x, y)
	assert isinstance(points, numpy.ndarray), type(points)
	return points


def random_point_in_poly(
	poly: shapely.Polygon | shapely.MultiPolygon, random: numpy.random.Generator | int | None = None
) -> shapely.Point:
	"""
	Uniformly-ish generates a point somewhere within a polygon.
	This won't choose anywhere directly on the edge (I think). If poly is a MultiPolygon, it will be inside one of the components, but the distribution of which one might not necesarily be uniform.

	Arguments:
		poly: shapely Polygon or MultiPolygon
		random: Optionally a numpy random generator or seed, otherwise default_rng is used
	"""
	min_x, max_x, min_y, max_y = poly.bounds
	shapely.prepare(poly)
	while True:
		point = random_point_in_bbox(min_x, max_x, min_y, max_y, random)
		if poly.contains_properly(point):
			return point
