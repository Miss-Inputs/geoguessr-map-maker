"""Tools for region mode maps"""
import itertools
from collections.abc import Iterator, Mapping
from typing import Any

import shapely

from .split_polygon import split_around_interiors


def iter_boundaries(
	poly: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing, *, use_triangle: bool = False
) -> Iterator[shapely.LinearRing]:
	if isinstance(poly, shapely.LinearRing):
		yield poly
		return
	if isinstance(poly, shapely.MultiPolygon):
		yield from itertools.chain.from_iterable(iter_boundaries(p) for p in poly.geoms)
		return
	if poly.interiors:
		yield from (
			split.exterior for split in split_around_interiors(poly, use_triangle=use_triangle)
		)
	else:
		yield poly.exterior


def polygon_to_geoguessr_map(
	poly: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing,
) -> Mapping[str, Any]:
	# TODO: Another thing that takes a GeoDataFrame and outputs each row as a different region
	regions = []
	for ring in iter_boundaries(poly):
		coords = [{'lat': y, 'lng': x} for x, y in ring.coords]
		regions.append({'coordinates': coords})
	# TODO: Is this all we need?
	return {'mode': 'regions', 'regions': regions}
