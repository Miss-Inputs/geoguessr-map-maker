import logging
from typing import TYPE_CHECKING, Any, cast

import numpy
import shapely
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm

from .geo_utils import get_bearing
from .pano_finder import LocationOptions, find_location, find_locations_in_geometry

if TYPE_CHECKING:
	import aiohttp
	import geopandas
	import pandas

logger = logging.getLogger(__name__)


async def point_to_custom_coordinate(
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
	# Pan the location towards whatever point we were originally looking at
	bearing = cast(float, get_bearing(pano.lat, pano.lon, lat, lng, radians=False))
	return {
		'lat': lat if return_original_point else pano.lat,
		'lng': lng if return_original_point else pano.lon,
		'panoId': pano.id,
		'heading': bearing if return_original_point else numpy.degrees(pano.heading),
		'pitch': pano.pitch,
		'countryCode': pano.country_code,
	}


async def find_locations_in_row(
	row: 'pandas.Series',
	session: 'aiohttp.ClientSession',
	radius: int,
	options: LocationOptions | None = None,
	*,
	locale: str = 'en',
	allow_third_party: bool = False,
	return_original_point: bool = True,
):
	geometry = row.geometry
	if not isinstance(geometry, BaseGeometry):
		logger.error('Row does not have geometry: %s', row)
		return

	if isinstance(geometry, shapely.Point):
		loc = await point_to_custom_coordinate(
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
			allow_third_party=allow_third_party,
			locale=locale,
			options=options,
		):
			yield {
				'lat': pano.lat,
				'lng': pano.lon,
				'panoId': pano.id,
				'heading': pano.heading,
				'pitch': pano.pitch,
				'countryCode': pano.country_code,
				'extra': row.drop('geometry').to_dict(),
			}


async def find_locations_in_geodataframe(
	gdf: 'geopandas.GeoDataFrame',
	session: 'aiohttp.ClientSession',
	radius: int = 10,
	options: LocationOptions | None = None,
	*,
	allow_third_party: bool = False,
):
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
		t.set_postfix(index=index, row=row)
		found = find_locations_in_row(
			row, session, radius=radius, allow_third_party=allow_third_party, options=options
		)
		locations = {location['panoId']: location async for location in found}
		logger.info('Found %d locations', len(locations))
		map_json['customCoordinates'] += locations.values()

	return map_json