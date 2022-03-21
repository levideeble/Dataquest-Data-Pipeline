import pytest
import src.hacker_news_pipeline


class TestFileToJson(object):
    def test_for_json_file(self):
        assert isinstance(src.hacker_news_pipeline.file_to_json(), list)

