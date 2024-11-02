## geoguessr-map-maker
Unoriginally named Python library/CLI program to help make GeoGuessr maps, converting from GeoJSON or similar.

Under construction. If you are reading this it's not even remotely in a usable state because I haven't even taken this out of the readme yet, I just wanted to upload it first.

To run from command line use python -m geoguessr_map_maker {options}

## TODO
- Convert GTFS stops to map
- Use as_completed in loops (but ensure we limit connections to streetview)
- Ability to resume from where it was stopped
- Handle other CRSes (either project all GeoDataFrames to WGS84, or don't reproject frames that aren't that)
- (optionally) Discover locations via links
- Keep track of rejected locations
- Optionally use get_coverage_tile instead (which returns much less panos and is much less fun, but it is faster)
- Decompress .zst, .gz input files (not sure if geopandas opens gzip automatically but I'm fairly sure it doesn't do zstd)