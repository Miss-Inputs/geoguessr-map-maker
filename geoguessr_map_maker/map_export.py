"""For converting GeoGuessr maps back to other formats"""

import json
from typing import Any


def geoguessr_region_map_to_geojson(map_json: str):
	geojson: dict[str, Any] = {
		'type': 'FeatureCollection',
		'crs': {'type': 'name', 'properties': {'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'}},
		'features': [],
	}

	m = json.loads(map_json)

	for region in m.get('regions', []):
		feature: dict[str, Any] = {
			'type': 'Feature',
			'properties': {},
			'geometry': {
				'type': 'Polygon',
				# LinearRing is not allowed in GeoJSON, so instead we will create a polygon with only an exterior
				'coordinates': [[[coord['lng'], coord['lat']] for coord in region['coordinates']]],
			},
		}
		extra = m.get('extra')
		if extra:
			feature['properties'].update(extra)
		geojson['features'].append(feature)
	return geojson


# TODO: Convert to DataFrame
