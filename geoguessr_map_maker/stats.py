import json
from collections import Counter
from collections.abc import Hashable, Mapping, Sequence
from enum import Enum, auto
from pathlib import Path
from typing import Any

import aiofiles
import pandas
import shapely
from geopandas import GeoDataFrame

from .gdf_utils import autodetect_name_col, count_points_in_each_region, read_geo_file_async


async def _read_json(path: Path):
	async with aiofiles.open(path) as f:
		data = await f.read()
		return json.loads(data)


CoordinateList = Sequence[Mapping[str, Any]]


async def get_region_stats(
	coords: CoordinateList,
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


async def _read_coords_from_file(file: Path):
	map_data = await _read_json(file)
	if isinstance(map_data, list):
		coords = map_data
	elif isinstance(map_data, dict):
		coords = map_data.get('customCoordinates')
	else:
		raise TypeError(f'Loading {file} as map failed, map_data is {type(map_data)}')
	if not coords:
		raise ValueError(f'{file} contains no coordinates')
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
	return await get_stats(
		coords, stats_type, regions_file, regions_name_col, as_percentage=as_percentage
	)


async def print_stats(
	file: Path,
	stats_type: StatsType,
	regions_file: Path | None = None,
	regions_name_col: Hashable | None = None,
	output_file: Path | None = None,
	*,
	as_percentage: bool = True,
) -> None:
	stats = await get_stats_for_file(
		file, stats_type, regions_file, regions_name_col, as_percentage=as_percentage
	)

	if output_file:
		# TODO: To a different format if output extension is not csv
		stats.to_csv(output_file)
	else:
		print(stats.to_string())

	# TODO: Do we want to count things in the "extra" dict too?
