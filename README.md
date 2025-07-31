# LiDAR-HD Downloader

## Description
![cloud](doc/cloud.jpg)
Utility to **download France IGN LiDAR HD** to LAZ file based on AOI (Area of Interest) defined in a GeoPandas GeoDataframe.

##  Usage
- Install Python dependencies : `pip install -r requirements.txt`
- Follow instructions in notebook. Output will be like :  
![out](doc/out.jpg)

## Minimal example 
```python
from lhd import LiDARHD
from shapely.geometry import LineString
import geopandas as gpd

# Download LiDARHD tiling database
lidarhd = LiDARHD(folder_path='./lidarhd/', overwrite=False)

# Create a GeoDataframe as input (here a simple segment in Paris)
linestring = LineString([(2.3522, 48.8566), (2.3639, 48.8667)])
gdf = gpd.GeoDataFrame({
    'name': ['Paris Hotel de Ville - Paris Place de la République'],
    'geometry': [linestring]}, crs=4326).to_crs(2154)
gdf['geometry'] = gdf.geometry.buffer(30)

# Download LiDARHD data covering the GeoDataframe area using PDAL and COPC polygon reading capabilities
lidarhd.download(gdf, "./lidarhd/points.laz")
```
