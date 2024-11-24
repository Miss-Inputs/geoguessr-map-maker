import asyncio
import json
from argparse import ArgumentParser
from enum import Enum, auto
from pathlib import Path

import aiofiles
import aiohttp
from tqdm.contrib.logging import logging_redirect_tqdm

from geoguessr_map_maker.gdf_utils import read_geo_file
from geoguessr_map_maker.stats import get_stats

from .coordinate import CoordinateMap
from .geodataframes import find_locations_in_geodataframe, gdf_to_regions_map
from .gtfs import find_stops, load_gtfs_stops


class InputFileType(Enum):
	GeoJSON = auto()
	"""GeoJSON, or other file that can be opened by geopandas"""
	GeoguessrMap = auto()
	"""GeoGuessr map"""
	GTFS = auto()
	"""GTFS feed"""


async def _write_json(path: Path, data):
	map_json = json.dumps(data, indent='\t')
	async with aiofiles.open(path, mode='w', encoding='utf-8') as f:
		await f.write(map_json)


async def amain(
	input_file: Path,
	input_file_type: InputFileType,
	output_file: Path | None = None,
	name_col: str | None = None,
	radius: int | None = None,
	*,
	as_region_map: bool = False,
	stats: bool = False,
	stats_region_file: Path | None = None,
):
	# TODO: Allow input_file to not actually be a filesystem path, because geopandas read_file can get URLs and that sort of thing
	# TODO: Autodetect input_file_type, e.g. if zip (and contains stops.txt) then it should be GTFS
	if stats:
		# TODO: Another argument to output raw numbers instead of percentages, but this is overcomplicated enough aaaaa
		await get_stats(input_file, stats_region_file, name_col, output_file)
		return

	if output_file is None:
		# TODO: Avoid clobbering output_file
		# Even better: If it is a map, only overwrite the customCoordinates field
		output_file = input_file.with_suffix('.json')
	if radius is None:
		radius = 20

	if input_file_type == InputFileType.GeoJSON:
		gdf = read_geo_file(input_file)
		if name_col is None and 'name' in gdf.columns:
			name_col = 'name'
		if as_region_map:
			await _write_json(output_file, gdf_to_regions_map(gdf, name_col))
			return

		async with aiohttp.ClientSession() as session:
			locations = await find_locations_in_geodataframe(
				gdf, session, radius, name_col=name_col
			)
	elif input_file_type == InputFileType.GTFS:
		stops = await load_gtfs_stops(input_file)
		async with aiohttp.ClientSession() as session:
			locations = [loc async for loc in find_stops(stops, session, radius)]
	else:
		raise ValueError(f'Whoops I have not implemented {input_file_type} yet')

	geoguessr_map = CoordinateMap(locations, input_file.stem)
	await _write_json(output_file, geoguessr_map.to_dict())


def main():
	argparser = ArgumentParser()
	argparser.add_argument('input_file', type=Path, help='File to convert')
	argparser.add_argument(
		'output_file',
		type=Path,
		help='Path to output file, or default to input_file with .geojson suffix',
		nargs='?',
	)
	argparser.add_argument(
		'--name-col',
		help='Column in input_file to interpret as the name of each row, for logging/progress purposes',
	)
	argparser.add_argument(
		'--radius', type=int, help='Search radius for panoramas in metres, default 20m', default=20
	)
	argparser.add_argument(
		'--region-map',
		action='store_true',
		help='Generate a region map instead of finding coordinates in each area',
	)
	argparser.add_argument(
		'--from-gtfs',
		action='store_const',
		const=InputFileType.GTFS,
		dest='file_type',
		help='Read input_file as a GTFS feed and make a map of the stops',
	)
	# TODO: Whoops we should probably use subcommands
	argparser.add_argument(
		'--stats', action='store_true', help='Generate statistics for an input GeoGuessr map'
	)
	# TODO: This help text sucks and needs to be worded better but I was tired when I wrote it, sorry
	argparser.add_argument(
		'--stats-regions',
		type=Path,
		help='With --stats, path to GeoJSON etc file containing regions to count each location in',
	)
	# TODO: Arguments for LocationOptions, allow_third_party, etc

	args = argparser.parse_args()

	asyncio.run(
		amain(
			args.input_file,
			args.file_type or InputFileType.GeoJSON,
			args.output_file,
			args.name_col,
			args.radius,
			as_region_map=args.region_map or False,
			stats=args.stats or False,
			stats_region_file=args.stats_regions,
		)
	)


with logging_redirect_tqdm():
	main()
