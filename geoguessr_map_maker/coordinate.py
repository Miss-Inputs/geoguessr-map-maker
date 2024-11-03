from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import numpy

from .geo_utils import get_bearing
from .pano_finder import LocationOptions, find_location

if TYPE_CHECKING:
	import aiohttp
	from streetlevel.streetview import StreetViewPanorama


@dataclass
class Coordinate:
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
	return_original_point: bool = True,
):
	if return_original_point:
		if original_lat is None:
			raise ValueError('original_lat must be provided if using return_original_point')
		if original_lng is None:
			raise ValueError('original_lng must be provided if using return_original_point')
		return Coordinate(
			original_lat,
			original_lng,
			pano.id,
			# Ensure the camera is looking at the thing when you get the location
			cast(float, get_bearing(pano.lat, pano.lon, original_lat, original_lng, radians=False)),
			pano.pitch,
			None,
			pano.country_code,
			extra,
		)
	return Coordinate(
		pano.lat,
		pano.lon,
		pano.id,
		numpy.degrees(pano.heading),
		pano.pitch,
		None,
		pano.country_code,
		extra,
	)


async def find_point(
	lat: float,
	lng: float,
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	extra: dict[str, Any] | None = None,
	options: LocationOptions | None = None,
	*,
	allow_third_party: bool = False,
	return_original_point: bool = True,
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
	return pano_to_coordinate(
		pano.pano, lat, lng, extra, return_original_point=return_original_point
	)


@dataclass
class CoordinateMap:
	coordinates: Collection[Coordinate] = field(default_factory=list)
	name: str | None = None
	description: str | None = None

	def to_dict(self):
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
