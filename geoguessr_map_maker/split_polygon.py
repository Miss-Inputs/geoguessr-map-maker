"""Functions to split polygons into smaller holeless ones (for region maps)"""

import itertools
from collections.abc import Iterable

import shapely
from shapely.geometry.base import BaseMultipartGeometry
from tqdm.auto import tqdm  # TODO: Should be an optional dependency probably


def constrained_triangulate(poly: shapely.Polygon) -> Iterable[shapely.Polygon]:
	import triangle  # noqa: PLC0415

	exterior_point_count = len(poly.exterior.coords) - 1
	vertices = poly.exterior.coords[:-1]
	segments = [[i, i + 1] for i in range(exterior_point_count - 1)] + [
		[exterior_point_count - 1, 0]
	]
	holes: list[list[tuple[float, float]]] = []

	offset = exterior_point_count
	for hole in poly.interiors:
		hole_point_count = len(hole.coords) - 1
		vertices += list(hole.coords[:-1])
		segments += [[i, i + 1] for i in range(offset, offset + hole_point_count - 1)] + [
			[offset + hole_point_count - 1, offset]
		]
		representative_point = shapely.Polygon(hole.coords).representative_point()
		holes.append(list(*representative_point.coords))
		offset += hole_point_count

	tri = {'vertices': vertices, 'segments': segments}
	if holes:
		tri['holes'] = holes

	t = triangle.triangulate(tri, 'p')
	vertices = t['vertices']
	triangles = t['triangles']
	return shapely.polygons([[vertices[index] for index in tri] for tri in triangles])  # type: ignore[no-any-return]

	# """Not used now, because it generates too many triangles which is slower for GeoGuessr to work with, but for reference"""
	# triangles: shapely.GeometryCollection = shapely.delaunay_triangles(poly)
	# return geopandas.GeoSeries(triangles.geoms).clip(poly, keep_geom_type=True).boundary.values
	# Note that geopandas as of 0.14.0 now has a GeoSeries.delaunay_triangles


def find_splitting_line_with_tree(
	interior: shapely.LinearRing, tree: shapely.STRtree
) -> shapely.LineString:
	all_intersecting = tree.query(interior, 'intersects')
	if all_intersecting.size > 0:
		# Any of them will do the trick
		return tree.geometries[all_intersecting][0]
	nearest = tree.nearest(interior)
	if nearest:
		return tree.geometries[nearest]
	raise AssertionError('uh oh!!')


def find_splitting_line(
	exterior: shapely.LinearRing, interior: shapely.LinearRing
) -> shapely.LineString:
	shapely.prepare(interior)
	try:
		for (x1, y1), (x2, y2) in itertools.combinations(exterior.coords, 2):
			line = shapely.LineString([(x1, y1), (x2, y2)])
			if line.intersects(interior):
				return line
		raise AssertionError('uh oh!!!')
	finally:
		shapely.destroy_prepared(interior)


def split_around_interiors(
	poly: shapely.Polygon, *, use_tqdm: bool = True, use_triangle: bool = False
) -> Iterable[shapely.Polygon]:
	"""Returns polygons that are guaranteed to not have holes in them, which are poly but split up"""
	if use_triangle:
		return constrained_triangulate(poly)
	shapely.prepare(poly)
	exterior: shapely.LinearRing = poly.exterior
	# coords = exterior.coords
	lines = [
		find_splitting_line(exterior, interior)
		for interior in (
			tqdm(
				poly.interiors, desc='Finding splittling line', unit='polygon interior', leave=False
			)
			if use_tqdm
			else poly.interiors
		)
	]
	union = shapely.unary_union([poly.boundary, *lines])
	assert isinstance(union, BaseMultipartGeometry), f'Union was instead {type(union)}'
	return [
		polygon
		for polygon in shapely.polygonize(tuple(union.geoms)).geoms
		if poly.contains(polygon.representative_point()) and isinstance(polygon, shapely.Polygon)
	]
