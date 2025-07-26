import attrs
import duckdb
from attrs import define, field, method
import logging
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@define
class MpdCsvfile:
    """Represents an MPD file."""
    file_path: str = field()
    handler: 'MpdFileHandler' = field(init=False)

    def __attrs_post_init__(self):
        self.handler = MpdFileHandler(self.file_path)
        try:
            self.handler.read_file()
        except Exception as e:
            logger.error(f"Failed to read MPD file {self.file_path}: {e}")
                


@define
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
class MpdWebpage:
    """Represents a webpage containing MPD information."""
    url: str = field()
    handler: 'MpdFileHandler' = field(init=False)

    def __attrs_post_init__(self):
        self.handler = MpdFileHandler(self.url)
        self.handler.read_file()

    @method
    def fetch_csv_from_html(self):
        httpx.Client().get(self.url)
        # find all csv links in the HTML content
        # and return them as MpdCsvfile instances
        