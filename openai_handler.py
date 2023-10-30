import openai
import json
import os
from lookups import Open_AI, Errors
from logging_handler import log_error_msg

def add_response_to_animal_types_json(existing_data,content_name,content):
    try:
        response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": content}])
        assistant_response = response["choices"][0]["message"]["content"]
        existing_data[content_name] = assistant_response        
        with open(Open_AI.ANIMAL_TYPES_JSON_FILE, "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
    except Exception as e:
        log_error_msg(Errors.ADD_RESPONSE_TO_ANIMAL_TYPES_JSON.value, str(e))

def get_gpt_animal_type_string(content_name, content):
    try:
        openai.api_key = Open_AI.OPENAI_API_KEY.value
        if os.path.exists(Open_AI.ANIMAL_TYPES_JSON_FILE.value) and os.path.getsize(Open_AI.ANIMAL_TYPES_JSON_FILE.value) > 0:
            with open(Open_AI.ANIMAL_TYPES_JSON_FILE.value, "r") as json_file:
                existing_data = json.load(json_file)
            if content_name.value not in existing_data.keys():
                add_response_to_animal_types_json(existing_data,content_name.value,content.value)
        else:
            existing_data = {}
            add_response_to_animal_types_json(existing_data,content_name.value,content.value)
        return existing_data[content_name.value]
    except Exception as e:
        log_error_msg(Errors.GET_GPT_ANIMAL_TYPE_STRING.value, str(e))

