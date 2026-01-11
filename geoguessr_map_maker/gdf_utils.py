import asyncio
from collections.abc import Hashable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import geopandas
import numpy
from pandas.api.types import is_string_dtype
from tqdm.auto import tqdm

if TYPE_CHECKING:
	import pandas
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
	points: Iterable['Point'] | geopandas.GeoSeries,
	regions: geopandas.GeoDataFrame,
	name_col: Hashable,
):
	if not isinstance(points, geopandas.GeoSeries):
		if not isinstance(points, (list, numpy.ndarray)):
			points = list(points)
		points = geopandas.GeoSeries(points, crs=regions.crs)  # type: ignore[overload]
	regions = regions[[name_col, 'geometry']]
	gdf = cast('geopandas.GeoDataFrame', points.to_frame()).sjoin(regions, 'left', 'within')
	assert isinstance(gdf, geopandas.GeoDataFrame), type(gdf)
	return gdf[name_col].value_counts(dropna=False)


@overload
def autodetect_name_col(df: 'pandas.DataFrame') -> str | None: ...
@overload
def autodetect_name_col(
	df: 'pandas.DataFrame', *, should_fallback: Literal[False]
) -> str | None: ...
@overload
def autodetect_name_col(
	df: 'pandas.DataFrame', *, should_fallback: Literal[True]
) -> str | Hashable | None: ...


def autodetect_name_col(
	df: 'pandas.DataFrame', *, should_fallback: bool = False
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
