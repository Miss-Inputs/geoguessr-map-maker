import logging
from collections.abc import AsyncIterator, Collection, Hashable, Iterator
from typing import TYPE_CHECKING

import shapely
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm

from .coordinate import Coordinate, PanningModeType, find_point, pano_to_coordinate
from .regions import iter_boundaries

if TYPE_CHECKING:
	import geopandas
	import pandas

	from .pano_finder import PanoFinder

logger = logging.getLogger(__name__)

_allowed_extra_types = (int, float, str)


def _get_extra_info(row: 'pandas.Series'):
	row = row.drop('geometry').dropna()
	d = row.to_dict()
	# to_dict converts int64/float64/whatever to normal Python types, so we don't need to worry about that
	return {str(k): v for k, v in d.items() if isinstance(v, _allowed_extra_types)}


async def find_locations_in_row(
	finder: 'PanoFinder',
	row: 'pandas.Series',
	name: str | None = None,
	panning: PanningModeType = None,
	*,
	snap_to_original_point: bool = False,
	include_row_data: bool = True,
) -> AsyncIterator[Coordinate]:
	"""
	Finds locations from a row of a GeoDataFrame, depending on whether it contains a Point or something else.

	Arguments:
		finder: PanoFinder used for finding points in polygons/multipolygons/etc, point geometries will be found directly.
		name: Name for this row. Only used for logging/displaying progress bars.
		snap_to_original_points: For point geometries, returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.
		include_row_data: Include scalar data from the row in the extra field.
	"""
	geometry = row.geometry
	if not isinstance(geometry, BaseGeometry):
		logger.error('%s does not have geometry: %s', name or 'Row', row)
		return
	extra = _get_extra_info(row) if include_row_data else None

	if isinstance(geometry, shapely.Point):
		loc = await find_point(
			geometry.y,
			geometry.x,
			finder.session,
			finder.radius,
			extra,
			finder.options,
			panning,
			None,
			finder.locale,
			snap_to_original_point=snap_to_original_point,
		)
		if loc:
			yield loc
	else:
		async for pano in finder.find_locations_in_geometry(geometry, name):
			yield pano_to_coordinate(
				pano.pano, extra=extra, panning=panning, snap_to_original_point=False
			)


async def find_locations_in_geodataframe(
	finder: 'PanoFinder',
	gdf: 'geopandas.GeoDataFrame',
	name_col: Hashable | None = None,
	panning: PanningModeType = None,
	*,
	snap_to_original_point: bool = False,
	include_row_data: bool = True,
) -> Collection[Coordinate]:
	"""Finds all the locations that it can using geometries in a GeoDataFrame.

	Arguments:
		name_col: Column in gdf to use for displayng progress bars, logging, etc.
		snap_to_original_points: For point geometries, returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.
		include_row_data: Include scalar data from the row in the extra field.
	"""
	coords: list[Coordinate] = []
	for index, row in (
		t := tqdm(gdf.iterrows(), 'Finding points in rows', unit='row', total=gdf.index.size)
	):
		if name_col:
			name = str(row.get(name_col, index)).replace('\r', ' ').replace('\n', ' ')
			t.set_postfix({'index': index, str(name_col): name})
		else:
			name = str(index)
			t.set_postfix(index=index)

		found = find_locations_in_row(
			finder,
			row,
			name,
			panning,
			snap_to_original_point=snap_to_original_point,
			include_row_data=include_row_data,
		)
		locations = {location.pano_id: location async for location in found}
		logger.info('Found %d locations in %s', len(locations), name)
		coords += locations.values()

	return coords


def gdf_to_regions(
	gdf: 'geopandas.GeoDataFrame', name_col: Hashable | None = None
) -> Iterator[tuple[str, shapely.LinearRing]]:
	with tqdm(gdf.iterrows(), 'Converting rows', unit='row', total=gdf.index.size) as t:
		for index, row in t:
			if name_col:
				name = str(row.get(name_col, index)).replace('\r', ' ').replace('\n', ' ')
				t.set_postfix({'index': index, str(name_col): name})
			else:
				name = str(index)
				t.set_postfix(index=index)
			poly = row.geometry
			if not isinstance(poly, (shapely.Polygon, shapely.MultiPolygon, shapely.LinearRing)):
				logger.info('%s is not a polygon or ring, skipping', name)
			for ring in iter_boundaries(poly):
				yield name, ring


def gdf_to_regions_map(gdf: 'geopandas.GeoDataFrame', name_col: Hashable | None = None):
	regions = []
	for name, ring in gdf_to_regions(gdf, name_col):
		coords = [{'lat': y, 'lng': x} for x, y in ring.coords]
		regions.append({'coordinates': coords, 'extra': {'name': name}})
	return {'mode': 'regions', 'regions': regions}
