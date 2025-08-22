"""Additional methods and properties for StreetViewPanorama."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from streetlevel import streetview

if TYPE_CHECKING:
	import aiohttp

logger = logging.getLogger(__name__)

address_component_place_types = {
	# https://developers.google.com/maps/documentation/cloud-customization/taxonomy
	# Even then, that is a subset…
	# Political
	'Country',  # Although it only seems to be used for Christmas Island/Guam/NMI/Martinique, which are not countries as such
	# Country border
	'Reservation',
	'Administrative Area1',  # I think 'State or province' in that documentation is this
	'Administrative Area2',
	'Locality',  # I think 'City' in that documentation is this
	'Sublocality1',
	'Sublocality2',
	'Sublocality3',
	'Neighborhood',
	# Natural
	'Continent',
	'Archipelago',
	'Island',
	'Lake',
	'Fjord',
	# Huh, at least this can have a name though
	'Area',
	# ???? What do these ones do? No names?
	'Geocoded address',
	'Intersection',
}
"""Place types that represent areas or whatever, not buildings"""

not_building_place_types = (
	address_component_place_types
	| {
		'Crater',
		'Peninsula',
		'Volcano',
		'Water',
		'Colloquial area',  # Have seen Manihi
		'Colloquial city',  # Have seen Flying Fish Cove and Melbourne, which aren't colloquial, or maybe because they technically aren't the name of the actual administrative division or something
		'Road',
		'Route',
		'Trail',
		'Nature preserve',
		'National park',
		'National reserve',
		'Wetland',
		'Wildlife refuge',
		'Wildlife park',
		'Bay',
		'Harbor',
		'Hiking area',
		'Mountain peak',
		'Woods',
		'Beach',
		'Botanical garden',
		'Trail head',
		'Park',
		'Amusement park',
		'Agricultural production',
		# Maybe Airport
		# Compound grounds? Community garden? City park? Memorial park? Sometimes Landmark isn't, or it's around the outside of one
		# Dam might not be a building… hrm
	}
)
"""Types for a place that indicate the place is probably not a building, and is instead an area or geographical feature."""
# Note: Building, River, Terminal point can sometimes have blank name


@dataclass
class Panorama:
	"""Wrapper class for streetview.StreetViewPanorama that ensures we have all the fields if we need them by re-requesting the ID."""

	pano: streetview.StreetViewPanorama
	has_extended_info: bool = True
	"""False if the panorama is from neighbours/historical/etc and only has basic location info"""
	has_places: bool = False
	"""If the pano should have places (false if from find_panorama)"""
	has_depth: bool = False
	"""If the pano would have a depth map (if and only if we requested it)"""

	@property
	def has_full_info(self) -> bool:
		"""If this panorama has all the information that can be returned (disregarding depth map)"""
		return self.has_extended_info and self.has_places


async def ensure_full_pano(
	pano: Panorama,
	session: 'aiohttp.ClientSession',
	locale: str = 'en',
	*,
	download_depth: bool = False,
) -> Panorama:
	if not download_depth and pano.has_full_info:
		return pano
	try:
		full_pano = await streetview.find_panorama_by_id_async(
			pano.pano.id, session, locale=locale, download_depth=download_depth
		)
	except (ValueError, IndexError) as e:
		# Sometimes depth maps are broken (this also causes problems in GeoGuessr because you can only move with the arrows, so it's just a thing that happens I guess)
		if not download_depth:
			raise
		logger.info(
			'Tried to get depth map for pano %s but encountered an error: %s %s',
			pano.pano.id,
			type(e),
			e,
		)
		# We still set has_depth to True, because it has as much depth map as it can possibly have
		full_pano = (
			pano.pano
			if pano.has_full_info
			else await streetview.find_panorama_by_id_async(
				pano.pano.id, session, download_depth=False, locale=locale
			)
		)
	if full_pano:
		return Panorama(full_pano, has_places=True, has_depth=download_depth)
	# This probably shouldn't happen
	return pano


async def is_trekker(pano: Panorama, session: 'aiohttp.ClientSession') -> bool:
	if not pano.has_extended_info:
		pano = await ensure_full_pano(pano, session)
	return pano.pano.source in {'scout', 'innerspace', 'cultural_institute'}


async def is_intersection(pano: Panorama, session: 'aiohttp.ClientSession') -> bool:
	"""Returns true if the panorama appears to be on an intersection.

	Note: May have a false positive if the panorama is on a road curve where the road name changes on one side.
	"""
	if not pano.has_extended_info:
		pano = await ensure_full_pano(pano, session)

	if not pano.pano.street_names:
		return False
	# _could_ have a 3-way intersection with only one label if both roads have the same name, but otherwise 2 of the same road label happens on straight sections of road
	return len(pano.pano.street_names) > 1 or len(pano.pano.street_names[0].angles) > 2


async def max_image_size(pano: Panorama, session: 'aiohttp.ClientSession'):
	if not pano.has_extended_info:
		pano = await ensure_full_pano(pano, session)
	# return max(pano.image_sizes, key=lambda size: size.x * size.y)
	return pano.pano.image_sizes[-1]


async def camera_gen(pano: Panorama, session: 'aiohttp.ClientSession'):
	"""Returns 2.5 if it is either gen 2 or gen 3, which are not programmatically distinguishable from each other.

	Note that shitcam (officially uploaded third party coverage) tends to have the same resolution as gen 2/3, and will probably also be detected as that.

	Returns None if it cannot be determined, such as on third party uploads."""

	if pano.pano.is_third_party:
		return None
	size = await max_image_size(pano, session)
	if size.x == 3328 and size.y == 1664:
		return 1
	if size.x == 13312 and size.y == 6656:
		return 2.5
	if size.x == 16384 and size.y == 8192:
		return 4
	return None


async def has_building(pano: Panorama, session: 'aiohttp.ClientSession') -> bool | None:
	"""Returns true if the panorama has a building nearby, or is a trekker of a building. None if we cannot be sure."""
	# TODO: Does this only work if the locale is set to en?
	if not pano.has_places:
		pano = await ensure_full_pano(pano, session)

	if not pano.pano.places:
		return None

	return any(place.type.value not in not_building_place_types for place in pano.pano.places)

async def is_terminus(pano: Panorama, session: 'aiohttp.ClientSession') -> bool:
	if not pano.has_extended_info:
		pano = await ensure_full_pano(pano, session)
	return len(pano.pano.links) == 1