import logging
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

import aiohttp
import backoff
import shapely
from streetlevel import streetview
from tqdm import tqdm

from .pano import camera_gen, has_building, is_intersection
from .shape_utils import get_polygon_lattice

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)

find_panorama_backoff = backoff.on_exception(backoff.expo, aiohttp.ClientConnectionError)(
	streetview.find_panorama_async
)


class PredicateOption(Enum):
	# hm couldn't think of a better name for this
	Ignore = auto()
	"""The default, ignore this option, just allow it"""
	Require = auto()
	"""Require something to be true about the panorama"""
	Reject = auto()
	"""Require something to _not_ be true about the panorama"""


def _check_predicate(
	pano: streetview.StreetViewPanorama,
	option: PredicateOption,
	predicate: Callable[[streetview.StreetViewPanorama], bool],
):
	if option == PredicateOption.Require:
		return not predicate(pano)
	if option == PredicateOption.Reject:
		return predicate(pano)
	return True


@dataclass
class LocationOptions:
	allow_normal: bool = True
	"""Allow ordinary car coverage"""
	allow_trekker: bool = True
	"""Allow trekker coverage"""
	reject_gen_1: bool = False
	"""Do not allow panoramas that are official coverage and gen 1"""
	intersections: PredicateOption = PredicateOption.Ignore
	"""If Require, only allow panoramas that are at an intersection, or if Reject, only allow panoramas that are not at an intersection"""
	buildings: PredicateOption = PredicateOption.Ignore
	"""If Require, only allow panoramas that have a building nearby or are a trekker of a building, or if Reject, only allow panoramas that do not"""
	# TODO: More parameters:
	# On a road curve
	# Minimum resolution (for third party)
	# Gen 4 only
	# Gen 1 only, because at that point why not


def is_panorama_wanted(pano: streetview.StreetViewPanorama, options: LocationOptions | None = None):
	if options is None:
		options = LocationOptions()

	if pano.source == 'launch' and not options.allow_normal:
		return False
	if pano.source in {'scout', 'innerspace', 'cultural_institute'} and not options.allow_trekker:
		return False
	if not _check_predicate(pano, options.intersections, is_intersection):
		return False
	if not _check_predicate(pano, options.buildings, has_building):
		return False

	gen = camera_gen(pano)
	if options.reject_gen_1:
		return gen and gen > 1
	return True


def filter_panos(
	panos: Iterable[streetview.StreetViewPanorama], options: LocationOptions | None = None
):
	return [pano for pano in panos if is_panorama_wanted(pano, options)]


async def find_location(
	point: shapely.Point | tuple[float, float],
	session: 'aiohttp.ClientSession',
	radius: int = 10,
	*,
	locale: str = 'en',
	allow_third_party: bool = False,
	options: LocationOptions | None = None,
) -> streetview.StreetViewPanorama | None:
	"""
	Parameters:
		point: Shapely Point object, or (latitude, longitude) tuple
		session: Session to use for finding panoramas
		locale: Locale to use for addresses etc in the returned panorama
		pano_options: See `is_pano_wanted`
	"""
	if isinstance(point, shapely.Point):
		return await find_location(
			(point.y, point.x),
			session=session,
			radius=radius,
			locale=locale,
			allow_third_party=allow_third_party,
			options=options,
		)
	lat, lon = point
	# Try official coverage first, because it is unlikely we would ever _prefer_ third party even if looking for both
	pano = await find_panorama_backoff(
		lat, lon, session=session, radius=radius, locale=locale, search_third_party=False
	)
	if not pano or not is_panorama_wanted(pano, options):
		if allow_third_party:
			pano = await find_panorama_backoff(
				lat, lon, session=session, radius=radius, locale=locale, search_third_party=True
			)
			if not pano or not is_panorama_wanted(pano, options):
				return None
		return None

	return pano


async def find_locations(
	points: Iterable[shapely.Point | tuple[float, float]],
	session: 'aiohttp.ClientSession',
	radius: int = 10,
	*,
	allow_third_party: bool = False,
	use_tqdm: bool = True,
	locale: str = 'en',
	options: LocationOptions | None = None,
) -> AsyncIterator[streetview.StreetViewPanorama]:
	for point in tqdm(
		points, 'Finding locations for points', unit='point', leave=False, disable=not use_tqdm
	):
		pano = await find_location(
			point,
			session=session,
			radius=radius,
			allow_third_party=allow_third_party,
			locale=locale,
			options=options,
		)
		if pano:
			yield pano


async def find_locations_in_geometry(
	geom: 'BaseGeometry',
	session: 'aiohttp.ClientSession',
	radius: int = 10,
	*,
	allow_third_party: bool = False,
	locale: str = 'en',
	options: LocationOptions | None = None,
	use_tqdm: bool = True,
) -> AsyncIterator[streetview.StreetViewPanorama]:
	if isinstance(geom, shapely.Point):
		loc = await find_location(
			geom,
			session,
			radius,
			locale=locale,
			allow_third_party=allow_third_party,
			options=options,
		)
		if loc:
			yield loc
	elif isinstance(geom, shapely.MultiPoint):
		async for loc in find_locations(
			geom.geoms,
			session,
			radius,
			locale=locale,
			allow_third_party=allow_third_party,
			options=options,
			use_tqdm=use_tqdm,
		):
			yield loc
	elif isinstance(geom, (shapely.Polygon, shapely.MultiPolygon, shapely.LinearRing)):
		points = get_polygon_lattice(geom, radius)
		async for loc in find_locations(
			points,
			session,
			radius,
			locale=locale,
			allow_third_party=allow_third_party,
			options=options,
			use_tqdm=use_tqdm,
		):
			yield loc
	elif isinstance(geom, shapely.geometry.base.BaseMultipartGeometry):
		for part in tqdm(
			geom.geoms,
			'Finding locations in multi-part geometry',
			unit='part',
			leave=False,
			disable=not use_tqdm,
		):
			async for loc in find_locations_in_geometry(
				part,
				session,
				radius,
				allow_third_party=allow_third_party,
				locale=locale,
				options=options,
				use_tqdm=use_tqdm,
			):
				yield loc
	# TODO: LineString (sample points along line)
	else:
		logger.warning('Unhandled geometry type: %s', geom.geom_type)
