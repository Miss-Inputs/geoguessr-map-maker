from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import numpy

from .geo_utils import get_bearing
from .pano_finder import LocationOptions, find_location

if TYPE_CHECKING:
	import aiohttp
	from streetlevel.streetview import StreetViewPanorama

	from .shape_utils import RandomType


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


class PanningMode(Enum):
	Default = auto()
	Skewed = auto()
	OriginalPoint = auto()
	Random = auto()


PanningModeType = float | PanningMode | None


def get_panning(
	pano: 'StreetViewPanorama',
	mode: PanningMode,
	original_lat: float | None = None,
	original_lng: float | None = None,
	random: 'RandomType' = None,
) -> float:
	if mode == PanningMode.OriginalPoint:
		if original_lat is None:
			raise ValueError('original_lat must be provided if using panning = OriginalPoint')
		if original_lng is None:
			raise ValueError('original_lng must be provided if using panning = OriginalPoint')
		return get_bearing(pano.lat, pano.lon, original_lat, original_lng, radians=False)
	if mode == PanningMode.Random:
		if not isinstance(random, numpy.random.Generator):
			random = numpy.random.default_rng(random)
		return random.uniform(0, 360)
	panning = numpy.degrees(pano.heading)
	if mode == PanningMode.Skewed:
		panning = (panning + 90) % 360
	return panning


def pano_to_coordinate(
	pano: 'StreetViewPanorama',
	original_lat: float | None = None,
	original_lng: float | None = None,
	country_code: str | None = None,
	extra: dict[str, Any] | None = None,
	panning: PanningModeType = None,
	random: 'RandomType' = None,
	*,
	snap_to_original_point: bool = False,
) -> Coordinate:
	"""
	Creates a GeoGuessr map location object from a panorama.

	Arguments:
		panning: If a float, heading in degrees.
			If Default, follow the panorama's original heading (so it should be facing the same direction as the road).
			If Skewed, rotate the panorama's original heading by 90 degrees clockwise, so it should be facing the side.
			If OriginalPoint, pan towards the original point, which requires original_lat and original_lng to be provided.
			If Random, pan in a random direction.
			If None, use OriginalPoint if original_lat and original_lng are passed in, or Default otherwise.
		snap_to_original_point: Returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.

	Raises:
		ValueError: If panning = original_point or snap_to_original_point is True but original_lat and original_lng are not provided.

	"""
	if panning is None:
		panning = (
			PanningMode.OriginalPoint
			if original_lat is not None and original_lng is not None
			else PanningMode.Default
		)
	if isinstance(panning, PanningMode):
		panning = get_panning(pano, panning, original_lat, original_lng, random)

	lat = pano.lat
	lng = pano.lon
	if snap_to_original_point:
		if original_lat is None:
			raise ValueError('original_lat must be provided if using snap_to_original_point')
		if original_lng is None:
			raise ValueError('original_lng must be provided if using snap_to_original_point')
		lat = original_lat
		lng = original_lng
	# TODO: Different modes for pitch, zoom
	country_code = country_code or pano.country_code
	pitch = pano.pitch

	return Coordinate(lat, lng, pano.id, panning, pitch, None, country_code, extra)


async def find_point(
	lat: float,
	lng: float,
	session: 'aiohttp.ClientSession',
	radius: int = 50,
	extra: dict[str, Any] | None = None,
	options: LocationOptions | None = None,
	panning: PanningModeType = PanningMode.OriginalPoint,
	random: 'RandomType' = None,
	locale: str = 'en',
	*,
	snap_to_original_point: bool = False,
) -> Coordinate | None:
	"""Attempts to find a panorama at a given point within a radius, and converts it to a `Coordinate`.

	Arguments:
		extra: Optional extra information to be stored in the "extra" field. Ignored by GeoGuessr but can be used by other tools.
		snap_to_original_point: Returns the original point as the actual location in the map, so while the panorama will be loaded wherever it is found, the point where players actually click is potentially somewhere else. Not recommended as it would be unexpected.
	"""
	pano = await find_location((lat, lng), session, radius, locale=locale, options=options)
	if not pano:
		return None
	return pano_to_coordinate(
		pano.pano,
		lat,
		lng,
		None,
		extra,
		panning,
		random,
		snap_to_original_point=snap_to_original_point,
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
