from attrs import Factory
import mpd

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
