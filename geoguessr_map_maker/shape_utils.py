import logging
from collections.abc import Collection
from functools import partial

import numpy
import shapely
import shapely.ops
from pyproj import CRS, Transformer
from pyproj.enums import TransformDirection

logger = logging.getLogger(__name__)

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
	# TODO: Option to get random points instead
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
		return (poly.representative_point(),)

	if reproject:
		intersection = shapely.ops.transform(mercator_to_wgs84, intersection)
	if isinstance(intersection, shapely.MultiPoint):
		return tuple(intersection.geoms)
	if not isinstance(intersection, shapely.Point):
		logger.info('Somehow the intersection was a %s, returning representative point')
		return (poly.representative_point(),)
	return (intersection,)
