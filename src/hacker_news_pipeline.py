from src.pipeline import Pipeline, build_csv
import json
import io
import csv
from datetime import datetime
import string
from src.stop_words import stop_words
import os

pipeline = Pipeline()


@pipeline.task()
def file_to_json():
    cur_path = os.path.dirname(__file__)
    with open(os.path.join(cur_path, 'hn_stories_2014.json')) as file:
        data = json.load(file)
        stories = data['stories']
        return stories


@pipeline.task(depends_on=file_to_json)
def filter_stories(stories):
    def is_popular(story):
        return story['title'].split(' ')[0:2] != 'Ask HN' and story['points'] > 50 and story['num_comments'] > 1

    return (story for story in stories if is_popular(story))


@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    lines = []
    for story in stories:
        lines.append((story['objectID'], datetime.strptime(story['created_at'], '%Y-%m-%dT%H:%M:%SZ'), story['url'],
                      story['points'], story['title']))
    return build_csv(lines, header=['objectID', 'created_at', 'url', 'points', 'title'], file=io.StringIO())


@pipeline.task(depends_on=json_to_csv)
def extract_titles(csv_file):
    reader = csv.reader(csv_file)
    header = next(reader)
    index = header.index('title')
    return (line[index] for line in reader)


@pipeline.task(depends_on=extract_titles)
def clean_titles(titles):
    punctuation = string.punctuation
    for title in titles:
        for character in punctuation:
            if character in title:
                title = title.replace(character, '')

        yield title.lower()


@pipeline.task(depends_on=clean_titles)
def build_keyword_dictionary(titles):
    frequency_dict = {}
    for title in titles:
        words = title.split(' ')
        for word in words:
            if word and word not in stop_words:
                if word in frequency_dict:
                    frequency_dict[word] += 1
                else:
                    frequency_dict[word] = 1

    return frequency_dict


@pipeline.task(depends_on=build_keyword_dictionary)
def top_100(word_frequencies):
    sorted_words = sorted(word_frequencies.items(), key=lambda item: item[1], reverse=True)
    return sorted_words[:100]

completed_tasks = pipeline.run()
print(completed_tasks[top_100])