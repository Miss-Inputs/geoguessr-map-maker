import asyncio
import json
from argparse import ArgumentParser
from pathlib import Path

import aiofiles
import aiohttp
import geopandas
from tqdm.contrib.logging import logging_redirect_tqdm

from .coordinate import CoordinateMap
from .geodataframes import find_locations_in_geodataframe


async def amain(
	input_file: Path, output_file: Path | None = None, name_col: str | None = None, radius: int = 10
):
	print(input_file)
	if output_file is None:
		output_file = input_file.with_suffix('.geojson')

	gdf = geopandas.read_file(input_file)
	if not isinstance(gdf, geopandas.GeoDataFrame):
		print(f'What the, gdf is {type(gdf)}')
		return
	if name_col is None and 'name' in gdf.columns:
		name_col = 'name'

	async with aiohttp.ClientSession() as session:
		locations = await find_locations_in_geodataframe(gdf, session, radius, name_col=name_col)

	geoguessr_map = CoordinateMap(locations, input_file.stem)
	map_json = json.dumps(geoguessr_map.to_dict(), indent='\t')
	async with aiofiles.open(output_file, mode='w', encoding='utf-8') as f:
		await f.write(map_json)


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
	argparser.add_argument('--radius', type=int, help='Search radius for panoramas')
	# TODO: Arguments for LocationOptions, allow_third_party, etc
	# TODO: Argument for input_file to be a GeoGuessr map instead, although we could try and autodetect this
	# TODO: Argument for regions mode

	args = argparser.parse_args()
	asyncio.run(amain(args.input_file, args.output_file, args.name_col, args.radius))


with logging_redirect_tqdm():
	main()
