import asyncio
import json
import logging
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from collections.abc import Collection, Hashable
from enum import Enum, auto
from pathlib import Path
from typing import Any, Literal

import aiofiles
import aiohttp
import geopandas
import shapely
from tqdm.contrib.logging import logging_redirect_tqdm

from .coordinate import Coordinate, CoordinateMap, PanningMode, PanningModeType
from .gdf_finder import find_locations_in_geodataframe, gdf_to_regions_map
from .gdf_utils import autodetect_name_col, read_geo_file_async
from .gtfs import find_stops, load_gtfs_stops
from .pano_finder import LatticeFinder, LocationOptions, PointFinder, PredicateOption, RandomFinder
from .stats import StatsType, print_stats


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


async def generate_gtfs(input_file: Path, radius: int, options: LocationOptions):
	stops = await load_gtfs_stops(input_file)
	async with aiohttp.ClientSession() as session:
		return [loc async for loc in find_stops(stops, session, radius, options)]


FinderType = Literal['lattice', 'random', 'points']


async def generate_points(
	gdf: 'geopandas.GeoDataFrame',
	name_col: Hashable | None,
	radius: int,
	n: int | None,
	max_retries: int | None,
	max_connections: int,
	finder_type: FinderType,
	options: LocationOptions,
	panning: PanningModeType,
	*,
	ensure_n: bool,
) -> Collection[Coordinate]:
	async with aiohttp.ClientSession() as session:
		if finder_type == 'lattice':
			finder = LatticeFinder(session, max_connections, radius, options)
		elif finder_type == 'random':
			if not n:
				n = 100_000 // gdf.index.size
			finder = RandomFinder(
				session, max_connections, radius, n, max_retries, options, ensure_n=ensure_n
			)
		elif finder_type == 'points':
			finder = PointFinder(session, max_connections, radius, options)
		return await find_locations_in_geodataframe(finder, gdf, name_col, panning)


async def output_locations(locations: Collection[Coordinate], name: str, output_path: Path):
	if not locations:
		print('Did not find any locations!')
		return
	if output_path.suffix.lower() == '.geojson':
		rows = [
			{
				'geometry': shapely.Point(loc.lng, loc.lat),
				'pano_id': loc.pano_id,
				'heading': loc.heading,
				'pitch': loc.pitch,
				'zoom': loc.zoom,
				'country_code': loc.country_code,
				**(loc.extra or {}),
			}
			for loc in locations
		]
		gdf = geopandas.GeoDataFrame(rows, crs='wgs84')
		print(gdf)
		await asyncio.to_thread(gdf.to_file, output_path)
		return
	geoguessr_map = CoordinateMap(locations, name)
	print(f'Found {len(locations)} locations')
	await _write_json(output_path, geoguessr_map.to_dict())


async def generate(
	input_file: Path,
	input_file_type: InputFileType,
	options: LocationOptions,
	output_file: Path | None = None,
	name_col: Hashable | None = None,
	radius: int | None = None,
	n: int | None = None,
	max_retries: int | None = 50,
	max_connections: int = 1,
	finder_type: FinderType = 'random',
	panning: PanningModeType = None,
	*,
	as_region_map: bool = False,
	ensure_n: bool = False,
):
	# TODO: Allow input_file to not actually be a filesystem path, because geopandas read_file can get URLs and that sort of thing
	# TODO: Autodetect input_file_type, e.g. if zip (and contains stops.txt) then it should be GTFS

	if output_file is None:
		output_file = input_file.with_suffix('.json')
	if radius is None:
		radius = 50

	if input_file_type == InputFileType.GeoJSON:
		gdf = await read_geo_file_async(input_file)
		name_col = name_col or autodetect_name_col(gdf)
		if as_region_map:
			await _write_json(output_file, gdf_to_regions_map(gdf, name_col))
			return

		locations = await generate_points(
			gdf,
			name_col,
			radius,
			n,
			max_retries,
			max_connections,
			finder_type,
			options,
			panning,
			ensure_n=ensure_n,
		)
	elif input_file_type == InputFileType.GTFS:
		locations = await generate_gtfs(input_file, radius, options)
	else:
		raise ValueError(f'Whoops I have not implemented {input_file_type} yet')

	await output_locations(locations, input_file.stem, output_file)


async def stats(
	input_file: Path,
	stats_type: str | StatsType,
	stats_region_file: Path | None = None,
	name_col: Hashable | None = None,
	output_file: str | Path | None = None,
	*,
	as_percentage: bool = True,
):
	if isinstance(stats_type, str):
		stats_type = StatsType[stats_type]
	output_file = Path(output_file) if output_file else None  # convert empty string
	await print_stats(
		input_file,
		stats_type,
		stats_region_file,
		name_col,
		output_file,
		as_percentage=as_percentage,
	)


_predicates = {
	'allow': PredicateOption.Ignore,
	'require': PredicateOption.Require,
	'reject': PredicateOption.Reject,
}
_panning_modes = {
	'auto': None,
	'default': PanningMode.Default,
	'original_point': PanningMode.OriginalPoint,
	'random': PanningMode.Random,
	'skewed': PanningMode.Skewed,
}


def parse_location_option_args(args: Namespace) -> LocationOptions:
	return LocationOptions(
		_predicates[args.trekker],
		_predicates[args.gen_1],
		_predicates[args.intersections],
		_predicates[args.buildings],
		_predicates[args.unofficial],
		_predicates[args.terminus],
	)


def main():
	argparser = ArgumentParser()
	subparsers = argparser.add_subparsers(dest='subcommand', required=False)

	gen_parser = subparsers.add_parser('generate', help='Generate a map', aliases=['gen'])
	gen_parser.add_argument('input_file', type=Path, help='File to convert')
	gen_parser.add_argument(
		'output_file',
		type=Path,
		help='Path to output file, or default to input_file with .json suffix',
		nargs='?',
	)
	gen_parser.add_argument(
		'--from-gtfs',
		action='store_const',
		const=InputFileType.GTFS,
		dest='file_type',
		help='Read input_file as a GTFS feed and make a map of the stops',
	)
	gen_parser.add_argument(
		'--method',
		help='Method of finding points: lattice (use a fixed grid across the polygons), random (find n random points in each polygon), or points (only find points in each geometry). Defaults to random',
		choices=('lattice', 'random', 'points'),
		default='random',
	)
	gen_parser.add_argument(
		'--name-col',
		help='Column in input_file to interpret as the name of each row, for logging/progress purposes',
	)
	gen_parser.add_argument(
		'--radius', type=int, help='Search radius for panoramas in metres, default 50m', default=50
	)
	gen_parser.add_argument(
		'-n',
		type=int,
		help='For method = random, number of points to generate per region (row in input_file), or 0 (default) to sum up to 100K locations',
		default=0,
	)
	gen_parser.add_argument(
		'--max-retries',
		type=int,
		help='For method = random and --ensure-balance, maximum number of attempts to find n locations before giving up, default 50',
		default=50,
	)
	gen_parser.add_argument(
		'--ensure-balance',
		action=BooleanOptionalAction,
		help='Try and ensure n points end up being found in each geometry, continually retrying until this happens, defualt true',
		default=False,
	)
	gen_parser.add_argument(
		'--max-connections',
		type=int,
		help='Maximum number of simultaneous connections to Google Maps, defaults to 1',
		default=1,
	)
	gen_parser.add_argument(
		'--region-map',
		action='store_true',
		help='Generate a region map instead of finding coordinates in each area, ignoring arguments like radius etc.',
	)

	pano_group = gen_parser.add_argument_group(
		'Location options', 'How to handle the generated location once found'
	)
	# Hmm this probably could use a better name and description (pitch/zoom would go in here, for example)
	# I don't think there's a good way to allow a constant value to panning as well, without getting rid of choices making it less user-friendly for a very niche use case
	pano_group.add_argument(
		'--panning',
		help="How to set panning/heading: default (keep the panorama's panning, generally same direction as the road), original point (for points, find the original point), random (random panning each time), skewed (90 degrees from default), auto (original_point for points and default otherwise)",
		choices=_panning_modes,
		default='auto',
	)

	options_group = gen_parser.add_argument_group(
		'Location options', 'What locations to allow/require/reject'
	)
	options_group.add_argument(
		'--gen-1',
		help='Allow/ignore, require or reject official gen 1 coverage, defaults to reject',
		choices=_predicates,
		default='reject',
	)
	options_group.add_argument(
		'--intersections',
		help='Allow/ignore, require, or reject locations at intersections, default allow',
		choices=_predicates,
		default='allow',
	)
	options_group.add_argument(
		'--buildings',
		help='Allow/ignore, require, or reject locations with buildings nearby, default allow',
		choices=_predicates,
		default='allow',
	)
	options_group.add_argument(
		'--unofficial',
		help='Allow/ignore, require, or reject unofficial coverage, default reject',
		choices=_predicates,
		default='reject',
	)
	options_group.add_argument(
		'--trekker',
		help='Allow/ignore, require, or reject trekker (non-car) coverage, default allow',
		choices=_predicates,
		default='allow',
	)
	options_group.add_argument(
		'--terminus',
		help='Allow/ignore, require, or reject locations at the end of coverage, default allow',
		choices=_predicates,
		default='allow',
	)

	stats_parser = subparsers.add_parser(
		'stats', help='Generate statistics for an input GeoGuessr map'
	)
	stats_parser.add_argument('input_file', type=Path, help='File to generate stats for')
	stats_parser.add_argument(
		'type',
		nargs='?',
		help='What to use to get stats. Defaults to CountryCode',
		choices=StatsType._member_names_,
		default='CountryCode',
	)
	stats_parser.add_argument(
		'--regions-file',
		type=Path,
		help='Path to GeoJSON etc file containing regions to count each location in',
	)
	stats_parser.add_argument(
		'--name-col',
		help='Column in --regions-file to interpret as the name of each row, or the first column if not specified',
	)
	stats_parser.add_argument(
		'output_file', type=str, help='Path to output file, or print instead', nargs='?', default=''
	)
	stats_parser.add_argument(
		'--as-percentage',
		action=BooleanOptionalAction,
		help='Output distribution as a percentage of total locations instead of counts',
	)

	args = argparser.parse_args()
	if args.subcommand in {'generate', 'gen'}:
		asyncio.run(
			generate(
				args.input_file,
				args.file_type or InputFileType.GeoJSON,
				parse_location_option_args(args),
				args.output_file,
				args.name_col,
				args.radius,
				args.n,
				args.max_retries,
				args.max_connections,
				args.method,
				_panning_modes[args.panning],
				as_region_map=args.region_map or False,
				ensure_n=args.ensure_balance,
			)
		)
	elif args.subcommand == 'stats':
		asyncio.run(
			stats(
				args.input_file,
				args.type,
				args.regions_file,
				args.name_col,
				args.output_file,
				as_percentage=args.as_percentage,
			)
		)
	else:
		raise ValueError(f'Somehow got incorrect subcommand: {args.subcommand!r}')


with logging_redirect_tqdm():
	logging.basicConfig(level=logging.INFO)
	main()
