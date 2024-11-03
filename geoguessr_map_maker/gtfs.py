"""Make maps from GTFS feeds (justification: because funny)"""

# I don't think we really need partridge here
import csv
from collections.abc import AsyncIterator, Iterable, Mapping
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING
from zipfile import ZipFile

import aiofiles
from tqdm.auto import tqdm

from .coordinate import Coordinate, find_point

if TYPE_CHECKING:
	import aiohttp

	from geoguessr_map_maker.pano_finder import LocationOptions


class Stop:
	def __init__(self, row: Mapping[str, str]):
		self.row = row

	@property
	def lat(self):
		return float(self.row['stop_lat'])

	@property
	def lng(self):
		return float(self.row['stop_lon'])

	@property
	def name(self):
		return self.row.get('stop_name', self.row['stop_id'])


async def load_gtfs_stops(path: Path):
	async with aiofiles.open(path, 'rb') as f:
		z = ZipFile(BytesIO(await f.read()))
		stops_txt = z.read('stops.txt').decode('utf-8-sig', errors='ignore')
		stop_reader = csv.DictReader(stops_txt.splitlines())
		return [Stop(d) for d in stop_reader if 'stop_lat' in d]


async def find_stop(
	stop: Stop,
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	options: 'LocationOptions | None' = None,
	*,
	allow_third_party: bool = False,
):
	extra = {
		k: v for k, v in stop.row.items() if k not in {'stop_lat', 'stop_lon'} and v
	}
	if 'stop_code' in extra and extra['stop_id'] == extra['stop_code']:
		del extra['stop_code']
	return await find_point(
		stop.lat, stop.lng, session, radius, extra, options, allow_third_party=allow_third_party
	)


async def find_stops(
	stops: Iterable[Stop],
	session: 'aiohttp.ClientSession',
	radius: int = 20,
	options: 'LocationOptions | None' = None,
	*,
	allow_third_party: bool = False,
) -> AsyncIterator[Coordinate]:
	with tqdm(stops, desc='Finding stops', unit='stop') as t:
		for stop in t:
			t.set_postfix(stop=stop.name)
			loc = await find_stop(
				stop, session, radius, options, allow_third_party=allow_third_party
			)
			if loc:
				yield loc
