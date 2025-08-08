import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import geopandas
import numpy
import pandas
from tqdm.auto import tqdm

if TYPE_CHECKING:
	from shapely import Geometry, Point


def read_geo_file(
	path: Path | str, crs: Any = None, mask: 'Geometry | None' = None
) -> geopandas.GeoDataFrame:
	"""Reads a file with geopandas, showing a progress bar.

	Arguments:
		crs: If set, converts the GeoDataFrame to this CRS after reading.
		mask: If set, only loads rows inside a geometry as with the mask parameter to geopandas.read_file.
	
	Raises:
		TypeError: If reading the file resulted in something other than a GeoDataFrame.
	"""
	if isinstance(path, str):
		path = Path(path)

	with (
		path.open('rb') as f,
		tqdm.wrapattr(f, 'read', total=path.stat().st_size, desc=f'Reading {path}') as t,
	):
		gdf = geopandas.read_file(t, mask=mask)
		if not isinstance(gdf, geopandas.GeoDataFrame):
			raise TypeError(f'{path} contains {type(gdf)}, expected GeoDataFrame')
		if crs is not None:
			gdf = gdf.to_crs(crs)
		return gdf


async def read_geo_file_async(path: Path | str, crs: Any = None, mask: 'Geometry | None' = None) -> geopandas.GeoDataFrame:
	return await asyncio.to_thread(read_geo_file, path, crs, mask)


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
