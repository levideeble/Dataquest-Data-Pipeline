import pytest
from src.hacker_news_pipeline import file_to_json, filter_stories, json_to_csv, extract_titles, clean_titles, build_keyword_dictionary, top_100

class TestFileToJson(object):
    def test_for_json_file(self):
        assert isinstance(file_to_json(), list)
