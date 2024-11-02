from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class Coordinate:
	lat: float
	lng: float
	pano_id: str | None = None
	heading: float | None = None
	pitch: float | None = None
	zoom: float | None = None
	countryCode: str | None = None
	"""Is this even used by GeoGuessr? Or is it just there"""
	extra: dict[str, Any] | None = None
	"""Ignored by GeoGuessr, but can be used by third party extensions/tools"""

	def to_dict(self):
		return {k: v for k, v in asdict(self).items() if v is not None}
