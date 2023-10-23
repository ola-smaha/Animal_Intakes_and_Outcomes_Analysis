import openai
import json
import os
from lookups import *
from transformation_handler import *

api_key = "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"
openai.api_key = api_key

def gpt_answer(content_name, content):
    if os.path.exists("ai_animal_types.json"):
        with open("ai_animal_types.json", "r") as json_file:
            existing_data = json.load(json_file)
    else:
        existing_data = {}
    if content_name not in existing_data:
        response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": content}])
        assistant_response = response["choices"][0]["message"]["content"]
        existing_data[content_name] = assistant_response
        with open("ai_animal_types.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
    return assistant_response


content = "Give me a list of all bird types (example pigeon, lovebird...), comma separated, not numbered"
bird = gpt_answer('bird',content)
content = "Give me a list of all livestock animal types (horse,chicken,sheep,...), comma separated, not numbered"
livestock = gpt_answer('livestock',content)
