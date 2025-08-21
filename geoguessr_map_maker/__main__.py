import asyncio
import json
from argparse import ArgumentParser, BooleanOptionalAction
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import aiofiles
import aiohttp
from tqdm.contrib.logging import logging_redirect_tqdm

from .coordinate import CoordinateMap
from .gdf_utils import read_geo_file_async
from .geodataframes import find_locations_in_geodataframe, gdf_to_regions_map
from .gtfs import find_stops, load_gtfs_stops
from .pano_finder import LatticeFinder, LocationOptions, PointFinder, PredicateOption, RandomFinder
from .stats import StatsType, print_stats

if TYPE_CHECKING:
	import geopandas


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


async def generate_gtfs(
	input_file: Path, radius: int, options: LocationOptions, *, allow_unofficial: bool
):
	stops = await load_gtfs_stops(input_file)
	async with aiohttp.ClientSession() as session:
		return [
			loc
			async for loc in find_stops(
				stops, session, radius, options, allow_third_party=allow_unofficial
			)
		]


FinderType = Literal['lattice', 'random', 'points']


async def generate_points(
	gdf: 'geopandas.GeoDataFrame',
	name_col: str | None,
	radius: int,
	n: int | None,
	finder_type: FinderType,
	options: LocationOptions,
	*,
	allow_unofficial: bool,
):
	async with aiohttp.ClientSession() as session:
		if finder_type == 'lattice':
			finder = LatticeFinder(session, radius, options, search_third_party=allow_unofficial)
		elif finder_type == 'random':
			if not n:
				n = 100_000 // gdf.index.size
			finder = RandomFinder(session, radius, n, options, search_third_party=allow_unofficial)
		elif finder_type == 'points':
			finder = PointFinder(session, radius, options, search_third_party=allow_unofficial)
		return await find_locations_in_geodataframe(finder, gdf, name_col)


async def generate(
	input_file: Path,
	input_file_type: InputFileType,
	output_file: Path | None = None,
	name_col: str | None = None,
	radius: int | None = None,
	n: int | None = None,
	finder_type: FinderType = 'random',
	*,
	reject_gen_1: bool = False,
	allow_unofficial: bool = False,
	trekker: PredicateOption = PredicateOption.Ignore,
	intersections: PredicateOption = PredicateOption.Ignore,
	buildings: PredicateOption = PredicateOption.Ignore,
	as_region_map: bool = False,
):
	# TODO: Allow input_file to not actually be a filesystem path, because geopandas read_file can get URLs and that sort of thing
	# TODO: Autodetect input_file_type, e.g. if zip (and contains stops.txt) then it should be GTFS

	if output_file is None:
		# TODO: Avoid clobbering output_file
		# Even better: If it is a map, only overwrite the customCoordinates field
		output_file = input_file.with_suffix('.json')
	if radius is None:
		radius = 50
	options = LocationOptions(
		reject_gen_1=reject_gen_1, trekker=trekker, intersections=intersections, buildings=buildings
	)

	if input_file_type == InputFileType.GeoJSON:
		gdf = await read_geo_file_async(input_file)
		if name_col is None and 'name' in gdf.columns:
			name_col = 'name'
		if as_region_map:
			await _write_json(output_file, gdf_to_regions_map(gdf, name_col))
			return

		locations = await generate_points(
			gdf, name_col, radius, n, finder_type, options, allow_unofficial=allow_unofficial
		)
	elif input_file_type == InputFileType.GTFS:
		locations = await generate_gtfs(
			input_file, radius, options, allow_unofficial=allow_unofficial
		)
	else:
		raise ValueError(f'Whoops I have not implemented {input_file_type} yet')

	geoguessr_map = CoordinateMap(locations, input_file.stem)
	print(f'Found {len(locations)} locations')
	await _write_json(output_file, geoguessr_map.to_dict())


async def stats(
	input_file: Path,
	stats_type: str | StatsType,
	stats_region_file: Path | None = None,
	name_col: str | None = None,
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


def main():
	argparser = ArgumentParser()
	subparsers = argparser.add_subparsers(dest='subcommand', required=False)

	gen_parser = subparsers.add_parser('generate', help='Generate a map', aliases=['gen'])
	gen_parser.add_argument('input_file', type=Path, help='File to convert')
	# TODO: Clean up output_file handling
	gen_parser.add_argument(
		'output_file',
		type=Path,
		help='Path to output file, or default to input_file with .json suffix',
		nargs='?',
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
		'--region-map',
		action='store_true',
		help='Generate a region map instead of finding coordinates in each area, ignoring arguments like radius etc.',
	)
	gen_parser.add_argument(
		'--from-gtfs',
		action='store_const',
		const=InputFileType.GTFS,
		dest='file_type',
		help='Read input_file as a GTFS feed and make a map of the stops',
	)
	# TODO: The rest of LocationOptions
	gen_parser.add_argument(
		'--allow-gen-1',
		action=BooleanOptionalAction,
		help='Allow official gen 1 coverage, defaults to false',
		default=False,
	)
	gen_parser.add_argument(
		'--intersections',
		help='Allow/ignore, require, or reject locations at intersections, default allow',
		choices=_predicates,
		default='allow',
	)
	gen_parser.add_argument(
		'--buildings',
		help='Allow/ignore, require, or reject locations with buildings nearby, default allow',
		choices=_predicates,
		default='allow',
	)
	gen_parser.add_argument(
		'--allow-unofficial', action='store_true', help='Allow unofficial coverage'
	)

	stats_parser = subparsers.add_parser(
		'stats', help='Generate statistics for an input GeoGuessr map'
	)
	stats_parser.add_argument('input_file', type=Path, help='File to generate stats for')
	stats_parser.add_argument(
		'type',
		nargs='?',
		help=StatsType.__doc__,
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
				args.output_file,
				args.name_col,
				args.radius,
				args.n,
				args.method,
				reject_gen_1=not args.allow_gen_1,
				allow_unofficial=args.allow_unofficial,
				as_region_map=args.region_map or False,
				intersections=_predicates[args.intersections],
				buildings=_predicates[args.buildings],
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
	main()
