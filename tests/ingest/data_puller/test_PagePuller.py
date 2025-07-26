import pytest
import httpx
import attrs

from ingest.data_puller.PagePuller import PagePuller
from constants.entity.mpd_entities import MpdCsvfile, MpdWebpage

@pytest.fixture
def mock_http_client():
    """Fixture to mock the HTTP client."""
    return httpx.Client()

