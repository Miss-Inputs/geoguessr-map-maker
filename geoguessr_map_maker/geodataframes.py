import logging
from collections.abc import Hashable
from typing import TYPE_CHECKING, Any, cast

import numpy
import shapely
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm

from geoguessr_map_maker.coordinate import Coordinate

from .geo_utils import get_bearing
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
	return Coordinate(
		lat if return_original_point else pano.lat,
		lng if return_original_point else pano.lon,
		pano.id,
		# Pan the location towards whatever point we were originally looking at
		cast(float, get_bearing(pano.lat, pano.lon, lat, lng, radians=False))
		if return_original_point
		else numpy.degrees(pano.heading),
		pano.pitch,
		None,
		pano.country_code,
	)


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

	if isinstance(geometry, shapely.Point):
		loc = await find_point(
			geometry.y,
			geometry.x,
			radius,
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
			yield Coordinate(
				pano.lat,
				pano.lon,
				pano.id,
				pano.heading,
				pano.pitch,
				None,
				pano.country_code,
				row.drop('geometry').to_dict(),
			)


async def find_locations_in_geodataframe(
	gdf: 'geopandas.GeoDataFrame',
	session: 'aiohttp.ClientSession',
	radius: int = 10,
	options: LocationOptions | None = None,
	name_col: Hashable | None = None,
	*,
	allow_third_party: bool = False,
):
	"""
	Parameters:
		name_col: Column in gdf to use for displayng progress bars, logging, etc
	"""
	map_json: dict[str, Any] = {
		# Not filling this in completely, as we create the map as a draft and then press the import button
		# avatar: {background, decoration, ground, landscape}
		# coordinates: gets filled in?
		# 'created': datetime.now().isoformat(),
		'customCoordinates': []
		# description
		# highlighted: false
		# name
	}

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
		map_json['customCoordinates'] += locations.values()

	return map_json
