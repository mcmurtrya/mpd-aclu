import attrs
import duckdb
from attrs import define, field, method
from ingest.data_puller import config
import httpx
import logging
import os
from constants.entity.mpd_entities import MpdFileHandler, MpdCsvfile, MpdWebpage
import re
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from attrs import Factory
@define


class PagePuller(MpdWebpage):
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
                self.process_page(response.text)
                logger.info(f"Successfully pulled page: {page}")
            else:
                logger.error(f"Failed to retrieve {page}: {response.status_code}")
        self.http_client.close()

    @method
    def close(self):
        """Close the HTTP client and database connection."""
        self.http_client.close()
        self.connection.close()
        logger.info("Closed HTTP client and database connection.")
    
    @method
    def process_page(self, page_content: str):
        dict_of_attrs = attrs.asdict(page_content)
        logger.info(f"Processing page content: {dict_of_attrs}")

        class_name = dict_of_attrs.get('class_name', 'resource-item')

        csv_url_dict = {}

        for name in class_name:
            logger.info(f"Processing class name: {name}")
            values = name['href']
            key = name['title']
            csv_url_dict[key] = values

        rdict = {}

        for title, csv_url in csv_url_dict.items():
            logger.info(f"Processing CSV URL: {csv_url} with title: {title}")
            csv_file = MpdCsvfile(file_path=csv_url)
            rdict[title] = csv_file
        logger.info(f"Processed CSV files: {rdict}")
        return rdict

    @method
    def extract_year_and_quarter(self, filename: str) -> tuple[str, str]:
        """Extract year and quarter from the title."""
        parts = filename.split('_')
        year = re.match(r'\d{4}', filename)
        quarter = re.match(r'Q(\d)', filename)
        logger.info(f"Extracted year: {year}, quarter: {quarter} from title: {filename}")
        return year, quarter


    @method
    def save_to_raw_data(self, csv_dict: dict[str, MpdCsvfile]):
        """Save the pulled CSV files to the raw data directory."""
        for title, csv_file in csv_dict.items():
            file_path = os.path.join(config.RAW_DATA_DIR, title + '.csv')
            with open(file_path, 'w') as f:
                f.write(csv_file.handler.read_file())
            logger.info(f"Saved CSV file to raw data: {file_path}")