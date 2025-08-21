import asyncio
from collections.abc import Hashable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import geopandas
import numpy
import pandas
from pandas.api.types import is_string_dtype
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


async def read_geo_file_async(
	path: Path | str, crs: Any = None, mask: 'Geometry | None' = None
) -> geopandas.GeoDataFrame:
	return await asyncio.to_thread(read_geo_file, path, crs, mask)


def count_points_in_each_region(
	points: Iterable['Point'], regions: geopandas.GeoDataFrame, name_col: Hashable
):
	if not isinstance(points, (list, numpy.ndarray)):
		points = list(points)
	gs = geopandas.GeoSeries(points, crs=regions.crs)  # type: ignore[overload]
	gdf = cast('geopandas.GeoDataFrame', gs.to_frame()).sjoin(
		regions[[name_col, 'geometry']], how='left'
	)
	assert isinstance(gdf, geopandas.GeoDataFrame), type(gdf)
	if not isinstance(name_col, (int, str)):
		# TODO: Implement this, it's probably fine with groupby but maybe you need to ignore the type hint
		raise TypeError(
			f"TODO: The type of name_col is a funny type ({type(name_col)}), which is unusual and hasn't been implemented yet"
		)
	sizes = gdf.groupby(name_col, dropna=False, observed=False).size()
	assert isinstance(sizes, pandas.Series), type(sizes)
	return sizes.sort_values(ascending=False)


@overload
def autodetect_name_col(df: pandas.DataFrame) -> str | None: ...
@overload
def autodetect_name_col(df: pandas.DataFrame, *, should_fallback: Literal[False]) -> str | None: ...
@overload
def autodetect_name_col(
	df: pandas.DataFrame, *, should_fallback: Literal[True]
) -> str | Hashable | None: ...


def autodetect_name_col(
	df: pandas.DataFrame, *, should_fallback: bool = False
) -> str | Hashable | None:
	"""Attempts to find the name of a column within `df` that represents a name of each object. May return None.

	Arguments:
		df: DataFrame.
		should_fallback: If set to true, if we don't find anything that looks like a name column, return the first column containing a string.
	"""
	if 'name' in df.columns:
		return 'name'
	lower = df.columns.str.lower() == 'name'
	if lower.any():
		return df.columns[lower][0]
	contains = df.columns.str.contains('name', case=False, na=False, regex=False)
	if contains.any():
		return df.columns[contains][0]

	if should_fallback:
		for name, col in df.select_dtypes(object).items():
			if is_string_dtype(col):
				return name

	return None
