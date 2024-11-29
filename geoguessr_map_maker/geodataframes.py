import logging
from collections.abc import Collection, Hashable
from typing import TYPE_CHECKING

import pandas
import shapely
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm

from .coordinate import Coordinate, find_point, pano_to_coordinate
from .pano_finder import LocationOptions, find_locations_in_geometry
from .regions import iter_boundaries

if TYPE_CHECKING:
	import aiohttp
	import geopandas

logger = logging.getLogger(__name__)


async def find_locations_in_row(
	row: 'pandas.Series',
	session: 'aiohttp.ClientSession',
	radius: int,
	options: LocationOptions | None = None,
	name: str | None = None,
	locale: str = 'en',
	*,
	allow_third_party: bool = False,
	return_original_point: bool = True,
):
	"""
	Parameters:
		name: Only used for logging/displaying progress bars"""
	geometry = row.geometry
	if not isinstance(geometry, BaseGeometry):
		logger.error('%s does not have geometry: %s', name or 'Row', row)
		return
	extra = {
		str(k): v
		for k, v in row.drop(index='geometry').to_dict().items()
		if isinstance(v, (int, float, str)) and not pandas.isna(v)
	}

	if isinstance(geometry, shapely.Point):
		loc = await find_point(
			geometry.y,
			geometry.x,
			session,
			radius,
			extra,
			allow_third_party=allow_third_party,
			return_original_point=return_original_point,
		)
		if loc:
			yield loc
	else:
		async for pano in find_locations_in_geometry(
			geometry,
			session,
			radius,
			name,
			allow_third_party=allow_third_party,
			locale=locale,
			options=options,
		):
			# TODO: Do we always want to keep the original pano's heading/pitch? Or all of the row's data?
			yield pano_to_coordinate(pano.pano, extra=extra, return_original_point=False)


async def find_locations_in_geodataframe(
	gdf: 'geopandas.GeoDataFrame',
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	options: LocationOptions | None = None,
	name_col: Hashable | None = None,
	*,
	allow_third_party: bool = False,
) -> Collection[Coordinate]:
	"""
	Parameters:
		name_col: Column in gdf to use for displayng progress bars, logging, etc
	"""
	coords: list[Coordinate] = []
	for index, row in (t := tqdm(gdf.iterrows(), 'Finding rows', unit='row', total=gdf.index.size)):
		if name_col:
			name = str(row.get(name_col, index)).replace('\r', ' ').replace('\n', ' ')
			t.set_postfix({'index': index, str(name_col): name})
		else:
			name = str(index)
			t.set_postfix(index=index)

		found = find_locations_in_row(
			row, session, radius, options, name, allow_third_party=allow_third_party
		)
		locations = {location.pano_id: location async for location in found}
		logger.info('Found %d locations in %s', len(locations), name)
		coords += locations.values()

	return coords


def gdf_to_regions(gdf: 'geopandas.GeoDataFrame', name_col: Hashable | None = None):
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
