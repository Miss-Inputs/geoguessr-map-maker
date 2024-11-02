import logging
from collections.abc import Collection, Hashable
from typing import TYPE_CHECKING, Any

import shapely
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm

from .coordinate import Coordinate, pano_to_coordinate
from .pano_finder import LocationOptions, find_location, find_locations_in_geometry

if TYPE_CHECKING:
	import aiohttp
	import geopandas
	import pandas

logger = logging.getLogger(__name__)


async def find_point(
	lat: float,
	lng: float,
	radius: int = 20,
	extra: dict[str, Any] | None = None,
	*,
	allow_third_party: bool = False,
	return_original_point: bool = True,
	session: 'aiohttp.ClientSession',
	options: LocationOptions | None = None,
):
	pano = await find_location(
		(lat, lng),
		radius=radius,
		allow_third_party=allow_third_party,
		session=session,
		options=options,
	)
	if not pano:
		return None
	return pano_to_coordinate(pano, lat, lng, extra, return_original_point=return_original_point)


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
		for k, v in row.drop(index='geometry').to_dict()
		if isinstance(v, (int, float, str))
	}

	if isinstance(geometry, shapely.Point):
		loc = await find_point(
			geometry.y,
			geometry.x,
			radius,
			extra,
			allow_third_party=allow_third_party,
			session=session,
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
			yield pano_to_coordinate(pano, extra=extra, return_original_point=False)


async def find_locations_in_geodataframe(
	gdf: 'geopandas.GeoDataFrame',
	session: 'aiohttp.ClientSession',
	radius: int = 10,
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
	for index, row in (t := tqdm(gdf.iterrows(), 'Finding rows', unit='row')):
		if name_col:
			name = str(row.get(name_col, index))
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
