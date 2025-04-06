import asyncio
import json
from argparse import ArgumentParser
from enum import Enum, auto
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
from tqdm.contrib.logging import logging_redirect_tqdm

from geoguessr_map_maker.pano_finder import LatticeFinder, LocationOptions

from .coordinate import CoordinateMap
from .gdf_utils import read_geo_file
from .geodataframes import find_locations_in_geodataframe, gdf_to_regions_map
from .gtfs import find_stops, load_gtfs_stops
from .stats import get_stats


class InputFileType(Enum):
	GeoJSON = auto()
	"""GeoJSON, or other file that can be opened by geopandas"""
	GeoguessrMap = auto()
	"""GeoGuessr map"""
	GTFS = auto()
	"""GTFS feed"""


async def _write_json(path: Path, data: Any):
	map_json = json.dumps(data, indent='\t')
	async with aiofiles.open(path, mode='w', encoding='utf-8') as f:
		await f.write(map_json)


async def generate(
	input_file: Path,
	input_file_type: InputFileType,
	output_file: Path | None = None,
	name_col: str | None = None,
	radius: int | None = None,
	*,
	allow_unofficial: bool = False,
	as_region_map: bool = False,
):
	# TODO: Allow input_file to not actually be a filesystem path, because geopandas read_file can get URLs and that sort of thing
	# TODO: Autodetect input_file_type, e.g. if zip (and contains stops.txt) then it should be GTFS

	if output_file is None:
		# TODO: Avoid clobbering output_file
		# Even better: If it is a map, only overwrite the customCoordinates field
		output_file = input_file.with_suffix('.json')
	if radius is None:
		radius = 20
	options = LocationOptions()

	if input_file_type == InputFileType.GeoJSON:
		gdf = read_geo_file(input_file)
		if name_col is None and 'name' in gdf.columns:
			name_col = 'name'
		if as_region_map:
			await _write_json(output_file, gdf_to_regions_map(gdf, name_col))
			return

		async with aiohttp.ClientSession() as session:
			finder = LatticeFinder(session, radius, options, search_third_party=allow_unofficial)
			locations = await find_locations_in_geodataframe(finder, gdf, name_col)
	elif input_file_type == InputFileType.GTFS:
		stops = await load_gtfs_stops(input_file)
		async with aiohttp.ClientSession() as session:
			locations = [
				loc
				async for loc in find_stops(
					stops, session, radius, options, allow_third_party=allow_unofficial
				)
			]
	else:
		raise ValueError(f'Whoops I have not implemented {input_file_type} yet')

	geoguessr_map = CoordinateMap(locations, input_file.stem)
	print(f'Found {len(locations)} locations')
	await _write_json(output_file, geoguessr_map.to_dict())


async def stats(
	input_file: Path,
	stats_region_file: Path | None = None,
	name_col: str | None = None,
	output_file: Path | None = None,
):
	# TODO: Another argument to output raw numbers instead of percentages, but this is overcomplicated enough aaaaa
	await get_stats(input_file, stats_region_file, name_col, output_file)


def main():
	argparser = ArgumentParser()
	subparsers = argparser.add_subparsers(dest='subcommand', required=False)

	gen = subparsers.add_parser('generate', help='Generate a map', aliases=['gen'])
	gen.add_argument('input_file', type=Path, help='File to convert')
	# TODO: Clean up output_file handling
	gen.add_argument(
		'output_file',
		type=Path,
		help='Path to output file, or default to input_file with .json suffix',
		nargs='?',
	)
	gen.add_argument(
		'--name-col',
		help='Column in input_file to interpret as the name of each row, for logging/progress purposes',
	)
	gen.add_argument(
		'--radius', type=int, help='Search radius for panoramas in metres, default 20m', default=20
	)
	gen.add_argument(
		'--region-map',
		action='store_true',
		help='Generate a region map instead of finding coordinates in each area, ignoring arguments like radius etc.',
	)
	gen.add_argument(
		'--from-gtfs',
		action='store_const',
		const=InputFileType.GTFS,
		dest='file_type',
		help='Read input_file as a GTFS feed and make a map of the stops',
	)
	# TODO: Arguments for LocationOptions, etc
	gen.add_argument('--allow-unofficial', action='store_true', help='Allow unofficial coverage')
	stats = subparsers.add_parser('stats', help='Generate statistics for an input GeoGuessr map')
	stats.add_argument('input_file', type=Path, help='File to generate stats for')
	stats.add_argument(
		'--stats-regions',
		type=Path,
		help='Path to GeoJSON etc file containing regions to count each location in',
	)
	stats.add_argument(
		'--name-col', help='Column in stats_regions to interpret as the name of each row, or "name"'
	)
	stats.add_argument(
		'output_file', type=Path, help='Path to output file, or print instead', nargs='?'
	)

	args = argparser.parse_args()
	if args.subcommand == 'generate':
		asyncio.run(
			generate(
				args.input_file,
				args.file_type or InputFileType.GeoJSON,
				args.output_file,
				args.name_col,
				args.radius,
				allow_unofficial=args.allow_unofficial,
				as_region_map=args.region_map or False,
			)
		)
	elif args.subcommand == 'stats':
		asyncio.run(get_stats(args.input_file, args.stats_regions, args.name_col, args.output_file))


with logging_redirect_tqdm():
	main()
