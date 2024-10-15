import requests
import json
import re
import sys

# TikTok API credentials 
CLIENT_KEY = 'TEST'
CLIENT_SECRET = 'TEST'
GRANT_TYPE = 'client_credentials'

# Get OAuth token
def get_oauth_token():
    response = requests.post(
        'https://open.tiktokapis.com/v2/oauth/token/',
        headers={'Content-Type': 'application/x-www-form-urlencoded', 'Cache-Control': 'no-cache'},
        data={'client_key': CLIENT_KEY, 'client_secret': CLIENT_SECRET, 'grant_type': GRANT_TYPE}
    )
    return response.json().get('access_token')


# OAuth token initialization
oauth_token = get_oauth_token()


# JSON large number handling functions
def convert_large_numbers_to_string(raw_json):
    return re.sub(r':\s*(-?\d{16,})', r':"\1"', raw_json)


def convert_int64_strings_to_bigint(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and value.isdigit():
                obj[key] = int(value)
            else:
                convert_int64_strings_to_bigint(value)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            if isinstance(value, str) and value.isdigit():
                obj[i] = int(value)
            else:
                convert_int64_strings_to_bigint(value)


# Make TikTok API request and process the response
def make_request_and_process(body):
    url = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,view_count,username,hashtag_names,video_description,create_time,region_code,share_count,like_count,comment_count,music_id,effect_ids,playlist_id,voice_to_text,is_stem_verified,favorites_count,video_duration"

    response = requests.post(
        url,
        headers={'Authorization': f'Bearer {oauth_token}', 'Content-Type': 'application/json'},
        data=json.dumps(body)
    )

    raw_response = response.text
    try:
        response_data = json.loads(convert_large_numbers_to_string(raw_response))
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}", file=sys.stderr)
        print(f"Raw response: {raw_response}", file=sys.stderr)
        return [], None, False, None

    if 'data' in response_data and 'videos' in response_data['data']:
        convert_int64_strings_to_bigint(response_data)
        return response_data['data']['videos'], response_data['data'].get('cursor'), response_data['data'].get(
            'has_more'), response_data['data'].get('search_id')

    print(f"API Error: {response_data.get('error', 'Unexpected response')}", file=sys.stderr)
    return [], None, False, None


# Filter videos
def filter_videos(videos):
    return [
        {**video, 'url': f"https://www.tiktok.com/@{video['username']}/video/{video['id']}"}
        for video in videos
        if video['view_count'] >= 5000 and video.get('hashtag_names')
    ]


if __name__ == "__main__":
    query_params = {
        "query": {
            "and": [
                {"operation": "IN", "field_name": "keyword", "field_values": ["hashtag"]},
                {"operation": "EQ", "field_name": "video_length", "field_values": ["MID"]},
                {"operation": "IN", "field_name": "region_code", "field_values": ["US"]}
            ]
        },
        "start_date": "20230101",
        "end_date": "20230130",
        "max_count": 100,
        "is_random": False
    }

    all_videos, cursor, has_more, search_id = make_request_and_process(query_params)
    filtered_videos = filter_videos(all_videos)

    # Print filtered videos as JSON string
    print(json.dumps({'filtered_videos': filtered_videos}))

    # Print total videos returned and total filtered videos returned
    print(f'Total videos returned: {len(all_videos)}')
    print(f'Total filtered videos: {len(filtered_videos)}')

    # Print request details
    print(f'Request: cursor={cursor}, has_more={has_more}, search_id={search_id}')
