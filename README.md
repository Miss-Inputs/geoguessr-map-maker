## geoguessr-map-maker
Unoriginally named Python library to help make GeoGuessr maps, converting from GeoJSON or similar.

Under construction. If you are reading this it's not even remotely in a usable state because I haven't even taken this out of the readme yet, I just wanted to upload it first.

## TODO
- Command line interface
	- subcommands or nah? Probably converting a GeoJSON/etc file to a coordinates map is the default action, and then have a flag among the lines of --to-regions
	- and then some other subcommand or argument or whatever to load a GeoGuessr map file instead
- Convert GTFS stops to map
- Use as_completed in loops (but ensure we limit connections to streetview)
- Ability to resume from where it was stopped
- Handle other CRSes (either project all GeoDataFrames to WGS84, or don't reproject frames that aren't that)
- (optionally) Discover locations via links
- Keep track of rejected locations
- Optionally use get_coverage_tile instead (which returns much less panos)