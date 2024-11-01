from collections.abc import Iterator

import numpy
import shapely
import shapely.ops
from pyproj import CRS, Transformer
from pyproj.enums import TransformDirection
from tqdm.auto import tqdm

wgs84 = CRS('wgs84')
mercator = CRS('Web Mercator')
wgs84_to_mercator = Transformer.from_crs(wgs84, mercator, always_xy=True)


def iter_coordinates_in_polygon(
	poly: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing,
	resolution: float = 10,
	*,
	reproject: bool = True,
	use_tqdm: bool = True,
) -> Iterator[shapely.Point]:
	"""Yields points from a grid covering a polygon

	Parameters:
		reproject: If true, temporarily projects to mercator to make the resolution consistent
		resolution: Grid resolution in metres
	"""
	# TODO: Option to get random points instead
	shapely.prepare(poly)
	transformed = shapely.ops.transform(wgs84_to_mercator.transform, poly) if reproject else poly
	shapely.prepare(transformed)
	min_x, min_y, max_x, max_y = transformed.bounds

	x, y = numpy.meshgrid(
		numpy.arange(min_x, max_x, resolution, dtype='float64'),
		numpy.arange(min_y, max_y, resolution, dtype='float64'),
	)
	if reproject:
		wgs_lng, wgs_lat = wgs84_to_mercator.transform(
			x.flatten(), y.flatten(), direction=TransformDirection.INVERSE
		)  # pylint: disable=unpacking-non-sequence #what?
		points = shapely.points(wgs_lng, wgs_lat)
	else:
		points = shapely.points(x, y)
	assert not isinstance(
		points, shapely.Point
	), 'points should not be a single Point, array was passed in'

	for point in tqdm(
		points, desc='Testing points', unit='point', leave=False, disable=not use_tqdm
	):
		if poly.contains(point):
			yield point
