from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from shapely.geometry import Point
from tqdm.notebook import tqdm
import geopandas as gpd
import json
import pandas as pd
import pdal

URL_LHD = "https://data.geopf.fr/private/wfs/wfs?apikey=interface_catalogue&SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES=IGNF_LIDAR-HD_TA:nuage-dalle"


class LiDARHD:
    def __init__(self, folder_path: str | Path = "./lidarhd_data/", overwrite: bool = False):
        """
        Initialize the LiDARHD class.

        Args:
            folder_path (str | Path): Path to the folder where data will be stored. Defaults to "./lidarhd_data".
            overwrite (bool): Whether to overwrite existing database files. Defaults to False.
        """
        self.folder_path = Path(folder_path)
        self.folder_path.mkdir(parents=True, exist_ok=True)
        path = self._download_database(overwrite=overwrite)
        self.database_path = path
        self.database = self._read_database(path)

    def _read_database(self, path: str | Path = None) -> gpd.GeoDataFrame:
        """
        Read the LiDAR-HD database from a specified path or the default path.

        Args:
            path (str | Path): Path to the database file. If None, uses the default path.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing the LiDAR-HD data.
        """
        if path is None:
            path = self._get_database_path()
        if not isinstance(path, Path):
            path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Database file not found at: {path}")
        return gpd.read_file(path).to_crs(2154)

    def _get_database_path(self) -> str | Path:
        """
        Get the path to the LiDAR-HD database.

        Returns:
            str | Path: Path to the database file.
        """
        files = list(self.folder_path.glob("LidarHD_tiles_database*.gpkg"))
        if files:
            return files[0]
        else:
            return None

    def _check_database(self) -> bool:
        """
        Check if the LiDAR-HD database is already downloaded.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        path = self._get_database_path()
        if path and path.exists():
            print(f"Database already exists at: {path}")
            return True
        else:
            return False

    def _download_database(self, overwrite: bool, url=URL_LHD, max_pages: int = 100, ntiles: int = 5000, cpu_workers: int | float = 12) -> Path | str:
        """
        Fetch or load the LiDAR-HD database.

        Args:
            overwrite (bool): Whether to overwrite the existing database.
            url (str): WFS service URL.
            max_pages (int): Maximum number of pages to fetch. Defaults to 100.
            ntiles (int): Number of tiles per page. Defaults to 5000.
            cpu_workers (int | float): Number of workers for parallel processing. Defaults to 12.

        Returns:
            Path | str: Path to the downloaded or existing database file.
        """

        is_database_present = self._check_database()
        if is_database_present and not overwrite:
            print("No need to download, using existing database.")
            return self._get_database_path()
        if is_database_present and overwrite:
            print("Updating database...")
        else:
            print("Downloading new database...")

        # Fetch new database
        start_indexes = [(n - 1) * ntiles for n in range(1, max_pages + 1)]
        data_chunks = []

        with ThreadPoolExecutor(max_workers=cpu_workers) as executor:
            futures = {executor.submit(
                fetch_chunk, url, ntiles, idx): idx for idx in start_indexes}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching data chunks"):
                result = future.result()
                if result is not None:
                    data_chunks.append(result)

        if data_chunks:
            db = pd.concat(data_chunks, ignore_index=True)
            db['bloc'] = url2bloc(db['url'])
            db = db.drop(
                columns=['gml_id'], errors='ignore')

        database_filename_path = self.folder_path / \
            f"LidarHD_tiles_database_{datetime.today().strftime('%Y-%m-%d')}.gpkg"
        db.to_file(database_filename_path, driver='GPKG')
        print(f"LiDARHD Database saved in: {database_filename_path}")
        return self._get_database_path()

    def _get_clouds_intersecting(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Get the LiDAR-HD tiles intersecting a given GeoDataFrame.

        Args:
            gdf (gpd.GeoDataFrame): GeoDataFrame containing geometries to check against LiDAR-HD tiles.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing the intersecting tiles.
        """
        if self.database is None:
            raise ValueError(
                "Database is not loaded. Call `get_database` first.")

        # Convert to CRS 2154
        gdf = gdf.to_crs(2154)
        union_all = gdf.union_all()

        print(f"Input area is {union_all.area/ 1e6 : .3f} km²")

        intersecting_tiles = self.database[self.database.intersects(union_all)]

        return intersecting_tiles

    def download(self, gdf: gpd.GeoDataFrame, download_path: str) -> pd.DataFrame:
        """
        Download LiDAR points for the tiles intersecting the provided GeoDataFrame.
        This method checks which LiDAR-HD tiles intersect with the geometries in the provided GeoDataFrame,
        and downloads the corresponding LiDAR data to the specified path in LAZ format.
        It uses PDAL to handle the downloading and processing of the LiDAR data.
        If no intersecting tiles are found, it raises a ValueError.

        Args:
            gdf (gpd.GeoDataFrame): GeoDataFrame containing geometries to check against LiDAR-HD tiles.
            download_path (str): Path to save the downloaded LiDAR data as LAZ file.
        Returns:
            pd.DataFrame: DataFrame containing the downloaded LiDAR points.
        """
        if not download_path.endswith('.laz'):
            raise ValueError(
                "Download path must end with '.laz' to indicate a compressed LAS file.")

        intersecting_tiles = self._get_clouds_intersecting(gdf)
        if intersecting_tiles.empty:
            raise ValueError(
                "No intersecting tiles found for the provided GeoDataFrame.")

        download_urls = intersecting_tiles['url'].tolist()
        print(
            f"{len(download_urls)} clouds intersecting with the provided GeoDataFrame.")

        # return

        # Create a PDAL pipeline with multiple readers if needed
        pipeline = {
            "pipeline": []
        }
        for url in download_urls:
            pipeline["pipeline"].append({
                "type": "readers.copc",
                "filename": url,
                "polygon": gdf.union_all().wkt  # Use the union of all geometries in the GeoDataFrame
            })

        # Add a merge step if multiple readers are used
        if len(download_urls) > 1:
            pipeline["pipeline"].append({
                "type": "filters.merge"
            })

        # Add the writer to the pipeline
        pipeline["pipeline"].append({
            "type": "writers.las",
            "filename": download_path,
            "compression": "true"
        })

        # Create a PDAL pipeline object
        pdal_pipeline = pdal.Pipeline(json.dumps(pipeline))
        # Execute the pipeline
        pdal_pipeline.execute()
        print(f"Downloaded LiDAR data to: {download_path}")
        df_points = pdal_pipeline.get_dataframe(idx=0)
        print(f"Number of points downloaded: {len(df_points)}")
        return df_points


# Helper functions
def fetch_chunk(url: str, ntiles: int, start_index: int) -> gpd.GeoDataFrame:
    try:
        params = f"&STARTINDEX={start_index}&COUNT={ntiles}&SRSNAME=urn:ogc:def:crs:EPSG::2154"
        full_url = url + params
        gdf = gpd.read_file(full_url)
        return gdf if not gdf.empty else None
    except Exception:
        return None


def url2bloc(url_series: pd.Series) -> pd.Series:
    return url_series.apply(lambda x: x.split("/")[-1].split(".")[0] if isinstance(x, str) else None)
