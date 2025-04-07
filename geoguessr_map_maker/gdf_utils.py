import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, cast

import geopandas
import numpy
import pandas
from tqdm.auto import tqdm

if TYPE_CHECKING:
	from shapely import Point


def read_geo_file(path: Path) -> geopandas.GeoDataFrame:
	with (
		path.open('rb') as f,
		tqdm.wrapattr(f, 'read', total=path.stat().st_size, desc=f'Reading {path}') as t,
	):
		gdf = geopandas.read_file(t)
		if not isinstance(gdf, geopandas.GeoDataFrame):
			raise TypeError(f'{path} contains {type(gdf)}, expected GeoDataFrame')
		return gdf

async def read_geo_file_async(path: Path) -> geopandas.GeoDataFrame:
	return await asyncio.to_thread(read_geo_file, path)

def count_points_in_each_region(
	points: Iterable['Point'], regions: geopandas.GeoDataFrame, name_col: str
):
	if not isinstance(points, (list, numpy.ndarray)):
		points = list(points)
	gs = geopandas.GeoSeries(points, crs=regions.crs)  # type: ignore[overload]
	gdf = cast('geopandas.GeoDataFrame', gs.to_frame()).sjoin(
		regions[[name_col, 'geometry']], how='left'
	)
	assert isinstance(gdf, geopandas.GeoDataFrame), type(gdf)
	sizes = gdf.groupby(name_col, dropna=False, observed=False).size()
	assert isinstance(sizes, pandas.Series), type(sizes)
	return sizes.sort_values(ascending=False)
