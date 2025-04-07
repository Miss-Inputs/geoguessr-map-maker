import itertools
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import aiohttp
import backoff
import numpy
import shapely
from streetlevel import streetview
from streetlevel.geo import tile_coord_to_wgs84, wgs84_to_tile_coord
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
			return pano
		return None

	return pano


class PanoFinder(ABC):
	def __init__(
		self,
		session: 'aiohttp.ClientSession',
		radius: int = 20,
		options: LocationOptions | None = None,
		locale: str = 'en',
		*,
		search_third_party: bool = False,
		use_tqdm: bool = True,
	) -> None:
		self.session = session
		self.radius = radius
		self.options = options
		self.locale = locale
		self.search_third_party = search_third_party
		self.use_tqdm = use_tqdm

	async def find_locations(
		self, points: Iterable[shapely.Point | tuple[float, float]], name: str | None = None
	) -> AsyncIterator[Panorama]:
		for point in tqdm(
			points,
			f'Finding locations for {name or 'points'}',
			unit='point',
			leave=False,
			disable=not self.use_tqdm,
		):
			pano = await find_location(
				point,
				session=self.session,
				radius=self.radius,
				allow_third_party=self.search_third_party,
				locale=self.locale,
				options=self.options,
			)
			if pano:
				yield pano

	def points_in_multipoint(self, multipoint: shapely.MultiPoint, name: str | None=None) -> Iterable[shapely.Point]:
		return multipoint.geoms

	@abstractmethod
	def points_in_polygon(
		self, polygon: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing, name: str | None=None
	) -> Iterable[shapely.Point]: ...

	def points_in_multipolygon(self, multipolygon: shapely.MultiPolygon, name: str | None=None) -> Iterable[shapely.Point]:
		"""By default, same as points_in_polygon, but can be overridden if desired"""
		return self.points_in_polygon(multipolygon, name)

	@abstractmethod
	def points_in_linear_ring(self, linear_ring: shapely.LinearRing, name: str | None=None) -> Iterable[shapely.Point]: ...

	@abstractmethod
	def points_in_linestring(self, linestring: shapely.LineString, name: str | None=None) -> Iterable[shapely.Point]: ...

	def points_in_mutlilinestring(self, multilinestring: shapely.MultiLineString, name: str | None=None) -> Iterable[shapely.Point]:
		"""By default, concatenates the results of points_in_linestring for all lines, but can be overridden"""
		return itertools.chain.from_iterable(self.points_in_linestring(line) for line in multilinestring.geoms)

	def _points_in_geometry(self, geometry: 'BaseGeometry', name: str | None=None) -> Iterable[shapely.Point]:
		if isinstance(geometry, shapely.MultiPoint):
			return self.points_in_multipoint(geometry, name)
		if isinstance(geometry, shapely.Polygon):
			return self.points_in_polygon(geometry, name)
		if isinstance(geometry, shapely.MultiPolygon):
			return self.points_in_multipolygon(geometry, name)
		if isinstance(geometry, shapely.LinearRing):
			return self.points_in_linear_ring(geometry, name)
		if isinstance(geometry, shapely.LineString):
			return self.points_in_linestring(geometry, name)
		if isinstance(geometry, shapely.MultiLineString):
			return self.points_in_mutlilinestring(geometry, name)
		raise NotImplementedError(geometry.geom_type)


	async def find_locations_in_geometry(
		self, geometry: 'BaseGeometry', name: str | None = None
	) -> AsyncIterator[Panorama]:
		if isinstance(geometry, shapely.Point):
			pano = await find_location(
				geometry,
				self.session,
				self.radius,
				locale=self.locale,
				allow_third_party=self.search_third_party,
				options=self.options,
			)
			if pano:
				yield pano
			return
		elif isinstance(geometry, shapely.GeometryCollection):
			for part in tqdm(
				geometry.geoms,
				'Finding locations in multi-part geometry',
				unit='part',
				leave=False,
				disable=not self.use_tqdm,
				postfix={'name': name},
			):
				async for pano in self.find_locations_in_geometry(
					#should name here append like the index of the part? mayhaps
					part,
					name,
				):
					yield pano
			return
		else:
			try:
				points = self._points_in_geometry(geometry)
			except NotImplementedError:
				logger.warning(
					'Unhandled geometry type%s: %s', f' in {name}' if name else '', geometry.geom_type
				)
				return
			else:
				async for pano in self.find_locations(points, name):
					yield pano
	
class LatticeFinder(PanoFinder):
	"""Finds every point in an evenly spaced grid across each polygon."""
	#TODO: Lattice radius should be a separate parameter, but maybe can default to search radius
	def points_in_polygon(self, polygon: shapely.Polygon | shapely.MultiPolygon | shapely.LinearRing, name: str | None=None) -> Iterable[shapely.Point]:
		points = get_polygon_lattice(polygon, self.radius)
		if not points:
			logger.info('No points in %s, trying representative point instead', name or 'polygon')
			points = (polygon.representative_point(),)
		return points
	
	def points_in_linear_ring(self, linear_ring: shapely.LinearRing, name: str | None = None) -> Iterable[shapely.Point]:
		points = get_polygon_lattice(linear_ring, self.radius)
		if not points:
			logger.info('No points in %s, trying representative point instead', name or 'linear ring')
			points = (linear_ring.representative_point(),)
		return points
	
	def points_in_linestring(self, linestring: shapely.LineString, name: str | None = None) -> Iterable[shapely.Point]:
		#TODO: Sample all the points along the line according to the radius
		raise NotImplementedError(linestring.geom_type)

async def get_panos_in_geometry_via_tiles(
	poly: 'BaseGeometry', session: 'aiohttp.ClientSession', name: str | None = None
) -> AsyncIterator[Panorama]:
	shapely.prepare(poly)
	west, south, east, north = poly.bounds
	start_x, start_y = wgs84_to_tile_coord(north, west, 17)
	end_x, end_y = wgs84_to_tile_coord(south, east, 17)

	x_range = numpy.arange(start_x, end_x + 1)
	y_range = numpy.arange(start_y, end_y + 1)
	tiles = numpy.vstack(numpy.dstack(numpy.meshgrid(x_range, y_range)))  # type: ignore[overload]

	for tile_x, tile_y in tqdm(
		tiles, f'Getting tiles for {name}' if name else 'Getting tiles', unit='tile'
	):
		tile_max_lat, tile_min_lng = tile_coord_to_wgs84(tile_x, tile_y, 17)
		tile_min_lat, tile_max_lng = tile_coord_to_wgs84(tile_x + 1, tile_y + 1, 17)
		if not shapely.box(tile_min_lng, tile_min_lat, tile_max_lng, tile_max_lat).intersects(poly):
			continue

		tile_panos = await streetview.get_coverage_tile_async(tile_x, tile_y, session)

		for pano in tile_panos:
			if shapely.contains_xy(poly, pano.lon, pano.lat):
				yield Panorama(pano, has_extended_info=False)
