import attrs
import duckdb
from attrs import define, field, method
from ingest.data_puller import config
import httpx
import logging
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from attrs import Factory
@define
class PagePuller:
    """Handles the pulling of pages from a specified source."""
    http_client: httpx.Client = field(default_factory=httpx.Client)
    connection: duckdb.DuckDBPyConnection = field(
        default=Factory(lambda: duckdb.connect(config.DATABASE_PATH))
    )
    page_list: list[str] = field(default=config.PAGE_LIST)

    @method
    async def pull_pages(self):
        for page in self.page_list:
            os.sleep(1)  # Simulate delay for rate limiting
            logger.info(f"Pulling page: {page}")
            # Assuming the page is a URL, fetch its content
            # If it's a local file, read its content instead
            response = await self.http_client.get(page)
            if response.status_code == 200:
                self.connection.execute("INSERT INTO pages (content) VALUES (?)", (response.text,))
            else:
                logger.error(f"Failed to retrieve {page}: {response.status_code}")
class MpdFileHandler:
    """Handles MPD file operations."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.connection = duckdb.connect(config.DATABASE_PATH)
        self.connection.execute("CREATE TABLE IF NOT EXISTS mpd_files (content TEXT)")

    def read_file(self):
        with open(self.file_path, 'r') as file:
            content = file.read()
            self.connection.execute("INSERT INTO mpd_files (content) VALUES (?)", (content,))
            logger.info(f"MPD file {self.file_path} read and inserted into database.")

@define
class Mpd:
    """Represents an MPD file."""
    file_path: str = field()
    handler: MpdFileHandler = field(init=False)

    


    