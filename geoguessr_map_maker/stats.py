import json
import logging
from collections import Counter
from collections.abc import Hashable, Iterable, Mapping, Sequence
from enum import Enum, auto
from pathlib import Path
from typing import Any

import aiofiles
import pandas
import shapely
from geopandas import GeoDataFrame, GeoSeries
from tqdm.auto import tqdm

from .gdf_utils import autodetect_name_col, count_points_in_each_region, read_geo_file_async

logger = logging.getLogger(__name__)


async def _read_json(path: Path):
	async with aiofiles.open(path) as f:
		data = await f.read()
		return json.loads(data)


CoordinateList = Sequence[Mapping[str, Any]]


async def get_region_stats(
	coords: CoordinateList | GeoSeries,
	regions_file: Path | GeoDataFrame,
	regions_name_col: Hashable | None = None,
	*,
	as_percentage: bool = True,
):
	"""
	Arguments:
		coords: List of coordinates in GeoGuessr map
		regions_file: Path to GeoJSON/etc (anything readable by geopandas)
		regions_name_col: Column name in regions_file to use, or the first column if omitted
		as_percentage: Whether to return results as a percentage of total points, instead of a count

	Returns:
		Series of int (float if as_percentage is True) with a row for each region, the name being the index, and the count/percentage as values
	"""
	regions = (
		regions_file
		if isinstance(regions_file, GeoDataFrame)
		else await read_geo_file_async(regions_file)
	)
	regions_name_col = regions_name_col or autodetect_name_col(regions, should_fallback=True)
	if isinstance(coords, GeoSeries):
		points = coords
	else:
		points = shapely.points([(c['lng'], c['lat']) for c in coords])
	stats = count_points_in_each_region(points, regions, regions_name_col)
	if as_percentage:
		stats /= points.size
	return stats


def get_country_code_stats(coords: CoordinateList, *, as_percentage: bool = True):
	"""
	Counts the country codes in a list of coordinates. Doesn't work if the countryCode field is not used.

	Returns:
		Series of int (float if as_percentage is True) with a row for each country, the country code being the index, and the count/percentage as values
	"""
	counter = Counter(c.get('countryCode') for c in coords)
	stats = pandas.Series(counter).sort_values(ascending=False)
	if as_percentage:
		stats /= counter.total()
	return stats


class StatsType(Enum):
	CountryCode = auto()
	"""Use the country code in each coordinate, generally copied from the panorama metadata and hence using Google's borders"""
	Regions = auto()
	"""Use a GeoJSON (or other compatible) file and count how many coordinates are inside each region"""


async def get_stats(
	coords: CoordinateList,
	stats_type: StatsType,
	regions_file: Path | GeoDataFrame | None,
	regions_name_col: Hashable | None,
	*,
	as_percentage: bool = True,
):
	if stats_type == StatsType.CountryCode:
		return get_country_code_stats(coords, as_percentage=as_percentage)
	if stats_type == StatsType.Regions:
		if regions_file is None:
			raise ValueError('Cannot use region stats without a regions file')
		return await get_region_stats(
			coords, regions_file, regions_name_col, as_percentage=as_percentage
		)
	raise ValueError(f'Unhandled stats type: {stats_type}')


class NoCoordinatesError(ValueError):
	"""Raised for a GeoGuessr map file not having coordinates (maybe a polygon map)."""

	def __init__(self, path: Path, *args: object) -> None:
		self.path = path
		super().__init__(*args)


async def _read_coords_from_file(file: Path):
	map_data = await _read_json(file)
	if isinstance(map_data, list):
		coords = map_data
	elif isinstance(map_data, dict):
		coords = map_data.get('customCoordinates')
	else:
		raise TypeError(f'Loading {file} as map failed, map_data is {type(map_data)}')
	if not coords:
		raise NoCoordinatesError(file)
	return coords


async def get_stats_for_file(
	file: Path,
	stats_type: StatsType,
	regions_file: Path | GeoDataFrame | None,
	regions_name_col: Hashable | None,
	*,
	as_percentage: bool = True,
):
	coords = await _read_coords_from_file(file)
	stats = await get_stats(
		coords, stats_type, regions_file, regions_name_col, as_percentage=as_percentage
	)
	return stats.rename(file.stem)


async def get_stats_for_files(
	files: Iterable[Path],
	stats_type: StatsType,
	regions_file: Path | GeoDataFrame | None,
	regions_name_col: Hashable | None,
	*,
	as_percentage: bool = True,
):
	regions = (
		await read_geo_file_async(regions_file)
		if isinstance(regions_file, (Path, str))
		else regions_file
	)
	# I could use as_completed here but maybe we're just opening up too much things at once with that
	columns = []
	with tqdm(files, desc='Getting stats') as t:
		for file in t:
			t.set_postfix(file=file)
			try:
				col = await get_stats_for_file(
					file, stats_type, regions, regions_name_col, as_percentage=as_percentage
				)
			except NoCoordinatesError as ex:
				logger.warning('%s did not contain coordinates, skipping', ex.path)
				continue
			else:
				columns.append(col)
	return pandas.DataFrame({col.name: col for col in columns}).fillna(0)


async def output_stats(
	file: Path | Iterable[Path],
	stats_type: StatsType,
	regions_file: Path | None = None,
	regions_name_col: Hashable | None = None,
	output_file: Path | None = None,
	*,
	as_percentage: bool = True,
) -> None:
	# Debatable if this really needs to be separate from __main__ stats()
	if isinstance(file, Path):
		stats = await get_stats_for_file(
			file, stats_type, regions_file, regions_name_col, as_percentage=as_percentage
		)
	else:
		stats = await get_stats_for_files(
			file, stats_type, regions_file, regions_name_col, as_percentage=as_percentage
		)
	# TODO: Do we want to count things in the "extra" dict too?

	if output_file:
		# TODO: To a different format if output extension is not csv
		ext = output_file.suffix[1:].lower()
		if ext in {'ods', 'xls', 'xlsx'}:
			# TODO: Format as percentage if as_percentage, though this requires specific things for each engine I think
			stats.to_excel(output_file)
		elif ext == 'csv':
			stats.to_csv(output_file)
		elif ext in {'htm', 'html'}:
			stats.to_html(output_file)  # pyright: ignore[reportCallIssue] #wtf Pyright?
		else:
			logger.warning('Extension %s not known, outputting csv by default', ext)
			stats.to_csv(output_file)
	else:
		print(stats.to_string())
