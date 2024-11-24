import json
from collections import Counter
from pathlib import Path

import aiofiles
import pandas
import shapely

from .gdf_utils import count_points_in_each_region, read_geo_file


async def _read_json(path: Path):
	async with aiofiles.open(path) as f:
		data = await f.read()
		return json.loads(data)

async def get_stats(file: Path, regions_file: Path | None=None, regions_name_col: str|None=None, output_file: Path|None=None, *, as_percentage: bool=True):
	map_data = await _read_json(file)
	if isinstance(map_data, list):
		coords = map_data
	elif isinstance(map_data, dict):
		coords = map_data.get('customCoordinates')
	else:
		raise TypeError(f'Loading {file} as map failed, map_data is {type(map_data)}')
	if not coords:
		raise ValueError(f'{file} contains no coordinates')
	
	if regions_file:
		regions = read_geo_file(regions_file)
		if regions_name_col is None:
			regions_name_col = regions.columns.drop('geometry')[0]
		points = shapely.points([(c['lng'], c['lat']) for c in coords])
		stats = count_points_in_each_region(points, regions, regions_name_col)
		if as_percentage:
			stats /= points.size
	else:
		counter = Counter(c.get('countryCode') for c in coords)
		stats = pandas.Series(counter).sort_values(ascending=False)
		if as_percentage:
			stats /= counter.total()

	if output_file:
		#TODO: To a different format if output extension is not csv
		stats.to_csv(output_file)
	else:
		print(stats.to_string())

	#TODO: Do we want to count things in the "extra" dict too?
	

