from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy

from .geo_utils import get_bearing
from .pano_finder import LocationOptions, find_location

if TYPE_CHECKING:
	import aiohttp
	from streetlevel.streetview import StreetViewPanorama


@dataclass
class Coordinate:
	"""Represents an individual list item in the "customCoordinates" field in a GeoGuessr map."""
	lat: float
	lng: float
	pano_id: str | None = None
	heading: float | None = None
	pitch: float | None = None
	zoom: float | None = None
	country_code: str | None = None
	"""Is this even used by GeoGuessr? Or is it just there"""
	extra: Mapping[str, Any] | None = None
	"""Ignored by GeoGuessr, but can be used by third party extensions/tools"""

	def to_dict(self):
		d: dict[str, Any] = {'lat': self.lat, 'lng': self.lng}
		if self.pano_id:
			d['panoId'] = self.pano_id
		if self.heading is not None:
			d['heading'] = self.heading
		if self.pitch is not None:
			d['pitch'] = self.pitch
		if self.zoom is not None:
			d['zoom'] = self.zoom
		if self.country_code:
			# Don't know if this needs to be lowercase or uppercase
			d['countryCode'] = self.country_code
		if self.extra:
			d['extra'] = self.extra if isinstance(self.extra, dict) else dict(self.extra)
		return d


def pano_to_coordinate(
	pano: 'StreetViewPanorama',
	original_lat: float | None = None,
	original_lng: float | None = None,
	extra: dict[str, Any] | None = None,
	*,
	pan_to_original_point: bool | None=None,
	snap_to_original_point: bool = False,
) -> Coordinate:
	"""
	Creates a GeoGuessr map location object from a panorama.
	
	Arguments:
		pan_to_original_point: Whether to pan towards the original point, the default of None means it will do this if original_lat and original_lng are passed in and will not otherwise
		snap_to_original_points: Returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.

	Raises:
		ValueError: If pan_to_original_point or snap_to_original_point are True but original_lat and original_lng are not provided.
		
	"""
	#TODO: Argument to offset panning (e.g. pass in 90 to make a skewed map)
	#TODO: Argument to override country code
	if pan_to_original_point is None:
		pan_to_original_point = original_lat is not None and original_lng is not None

	if pan_to_original_point:
		if original_lat is None:
			raise ValueError('original_lat must be provided if using pan_to_original_point')
		if original_lng is None:
			raise ValueError('original_lng must be provided if using pan_to_original_point')
		heading = get_bearing(
			pano.lat, pano.lon, original_lat, original_lng, radians=False
		)
	else:
		heading = numpy.degrees(pano.heading)

	lat = pano.lat
	lng = pano.lon
	if snap_to_original_point:
		if original_lat is None:
			raise ValueError('original_lat must be provided if using snap_to_original_point')
		if original_lng is None:
			raise ValueError('original_lng must be provided if using snap_to_original_point')
		lat = original_lat
		lng = original_lng
	
	return Coordinate(
		lat,
		lng,
		pano.id,
		heading,
		pano.pitch,
		None,
		pano.country_code,
		extra,
	)


async def find_point(
	lat: float,
	lng: float,
	session: 'aiohttp.ClientSession',
	radius: int = 50,
	extra: dict[str, Any] | None = None,
	options: LocationOptions | None = None,
	*,
	pan_to_original_point: bool = True,
	snap_to_original_point: bool = False,
) -> Coordinate | None:
	"""Attempts to find a panorama at a given point within a radius, and converts it to a `Coordinate`.
	
	Arguments:
		extra: Optional extra information to be stored in the "extra" field. Ignored by GeoGuessr but can be used by other tools.
		pan_to_original_point: Whether to pan towards the original point, defaults to true.
		snap_to_original_points: Returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.
	"""
	pano = await find_location(
		(lat, lng),
		radius=radius,
		session=session,
		options=options,
	)
	if not pano:
		return None
	return pano_to_coordinate(
		pano.pano, lat, lng, extra, pan_to_original_point=pan_to_original_point, snap_to_original_point=snap_to_original_point
	)


@dataclass
class CoordinateMap:
	coordinates: Collection[Coordinate] = field(default_factory=list)
	name: str | None = None
	description: str | None = None

	def to_dict(self) -> dict[str, Any]:
		d = {
			# avatar: {background, decoration, ground, landscape}, who cares
			# 'created': datetime.now().isoformat(),
			'mode': 'coordinates',
			'customCoordinates': [c.to_dict() for c in self.coordinates],
		}
		if self.name:
			d['name'] = self.name
		if self.description:
			d['description'] = self.description
		return d
