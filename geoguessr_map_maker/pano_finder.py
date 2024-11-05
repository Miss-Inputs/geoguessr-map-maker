import json
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import aiohttp
import backoff
import shapely
from streetlevel import streetview
from tqdm.auto import tqdm

from .pano import Panorama, camera_gen, ensure_full_pano, has_building, is_intersection, is_trekker
from .shape_utils import get_polygon_lattice

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, (aiohttp.ClientConnectionError, json.JSONDecodeError))
async def find_panorama_backoff(
	lat: float,
	lng: float,
	session: aiohttp.ClientSession,
	radius: int = 20,
	locale: str = 'en',
	*,
	search_third_party: bool = False,
):
	pano = await streetview.find_panorama_async(
		lat, lng, session, radius, locale, search_third_party=search_third_party
	)
	if pano is None:
		return None
	return Panorama(pano, has_extended_info=True, has_places=False, has_depth=False)


class PredicateOption(Enum):
	# hm couldn't think of a better name for this
	Ignore = auto()
	"""The default, ignore this option, just allow it"""
	Require = auto()
	"""Require something to be true about the panorama"""
	Reject = auto()
	"""Require something to _not_ be true about the panorama"""


async def _check_predicate(
	pano: Panorama,
	session: aiohttp.ClientSession,
	option: PredicateOption,
	predicate: Callable[[Panorama, aiohttp.ClientSession], Coroutine[Any, Any, bool]],
):
	if option == PredicateOption.Require:
		return await predicate(pano, session)
	if option == PredicateOption.Reject:
		return not await predicate(pano, session)
	return True


@dataclass
class LocationOptions:
	allow_normal: bool = True
	"""Allow ordinary car coverage"""
	trekker: PredicateOption = PredicateOption.Ignore
	"""Allow/reject/require trekker coverage"""
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


async def is_panorama_wanted(
	pano: Panorama, session: aiohttp.ClientSession, options: LocationOptions | None = None
):
	if options is None:
		options = LocationOptions()

	if not options.allow_normal:
		if not pano.has_extended_info:
			pano = await ensure_full_pano(pano, session)
		if pano.pano.source == 'launch':
			return False
	if not await _check_predicate(pano, session, options.trekker, is_trekker):
		return False
	if not await _check_predicate(pano, session, options.intersections, is_intersection):
		return False
	if not await _check_predicate(pano, session, options.buildings, has_building):
		return False

	gen = await camera_gen(pano, session)
	if options.reject_gen_1:
		return gen is None or gen > 1
	return True


async def filter_panos(
	panos: Iterable[Panorama],
	session: aiohttp.ClientSession,
	options: LocationOptions | None = None,
):
	return [pano for pano in panos if await is_panorama_wanted(pano, session, options)]


async def find_location(
	point: shapely.Point | tuple[float, float],
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	*,
	locale: str = 'en',
	allow_third_party: bool = False,
	options: LocationOptions | None = None,
) -> Panorama | None:
	"""
	Parameters:
		point: Shapely Point object, or (latitude, longitude) tuple
		session: Session to use for finding panoramas
		locale: Locale to use for addresses etc in the returned panorama
		options: See `LocationOptions`
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
	if not pano or not await is_panorama_wanted(pano, session, options):
		if allow_third_party:
			pano = await find_panorama_backoff(
				lat, lon, session=session, radius=radius, locale=locale, search_third_party=True
			)
			if not pano or not await is_panorama_wanted(pano, session, options):
				return None
		return None

	return pano


async def find_locations(
	points: Iterable[shapely.Point | tuple[float, float]],
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	name: str | None = None,
	locale: str = 'en',
	options: LocationOptions | None = None,
	*,
	allow_third_party: bool = False,
	use_tqdm: bool = True,
) -> AsyncIterator[Panorama]:
	for point in tqdm(
		points,
		f'Finding locations for {name or 'points'}',
		unit='point',
		leave=False,
		disable=not use_tqdm,
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
	radius: int = 20,
	name: str | None = None,
	locale: str = 'en',
	*,
	allow_third_party: bool = False,
	options: LocationOptions | None = None,
	use_tqdm: bool = True,
) -> AsyncIterator[Panorama]:
	if isinstance(geom, shapely.Point):
		pano = await find_location(
			geom,
			session,
			radius,
			locale=locale,
			allow_third_party=allow_third_party,
			options=options,
		)
		if pano:
			yield pano
		return
	if isinstance(geom, shapely.MultiPoint):
		points = geom.geoms
	elif isinstance(geom, (shapely.Polygon, shapely.MultiPolygon, shapely.LinearRing)):
		points = get_polygon_lattice(geom, radius)
		if not points:
			logger.info('No points in %s, trying representative point instead', name or 'polygon')
			points = (geom.representative_point(),)
	elif isinstance(geom, shapely.geometry.base.BaseMultipartGeometry):
		for part in tqdm(
			geom.geoms,
			'Finding locations in multi-part geometry',
			unit='part',
			leave=False,
			disable=not use_tqdm,
			postfix={'name': name},
		):
			async for pano in find_locations_in_geometry(
				part,
				session,
				radius,
				name,
				locale,
				allow_third_party=allow_third_party,
				options=options,
				use_tqdm=use_tqdm,
			):
				yield pano
		return

	else:
		logger.warning(
			'Unhandled geometry type%s: %s', f' in {name}' if name else '', geom.geom_type
		)
		return

	# TODO: LineString (sample points along line)
	async for pano in find_locations(
		points,
		session,
		radius,
		name,
		locale=locale,
		allow_third_party=allow_third_party,
		options=options,
		use_tqdm=use_tqdm,
	):
		yield pano
