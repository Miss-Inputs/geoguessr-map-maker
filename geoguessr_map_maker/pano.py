"""Additional methods and properties for StreetViewPanorama."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from streetlevel import streetview

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
		# Maybe Airport
		# Compound grounds? Community garden? City park? Memorial park? Sometimes Landmark isn't, or it's around the outside of one
		# Dam might not be a building… hrm
	}
)
"""Types for a place that indicate the place is probably not a building, and is instead an area or geographical feature."""
# Note: Building, River, Terminal point can sometimes have blank name


def is_intersection(pano: 'streetview.StreetViewPanorama'):
	if not pano.street_names:
		return False
	#_could_ have a 3-way intersection with only one label if both roads have the same name, but otherwise 2 of the same road label happens on straight sections of road
	return len(pano.street_names) > 1 or len(pano.street_names[0].angles) > 2


def max_image_size(pano: 'streetview.StreetViewPanorama'):
	# return max(pano.image_sizes, key=lambda size: size.x * size.y)
	return pano.image_sizes[-1]


def camera_gen(pano: 'streetview.StreetViewPanorama'):
	"""Returns 2.5 if it is either gen 2 or gen 3, which are not programmatically distinguishable from each other.

	Returns None if it cannot be determined, such as on third party uploads."""
	if pano.is_third_party:
		return None
	size = max_image_size(pano)
	if size.x == 3328 and size.y == 1664:
		return 1
	if size.x == 13312 and size.y == 6656:
		return 2.5
	if size.x == 16384 and size.y == 8192:
		return 4
	return None


def has_building(pano: 'streetview.StreetViewPanorama'):
	"""Returns true if the panorama has a building nearby, or is a trekker of a building."""
	# TODO: Does this only work if the locale is set to en?
	if not pano.places:
		return False
	return any(place.type.value not in not_building_place_types for place in pano.places)
