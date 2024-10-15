import requests
import json
import re
import sys
import time
import pandas as pd
import datetime
import os

# TikTok API credentials 
CLIENT_KEY = 'TEST'
CLIENT_SECRET = 'TEST'
GRANT_TYPE = 'client_credentials'

# Get OAuthentication token
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


# Make TikTok API request and process the response with retry mechanism
def make_request_and_process(body, cursor, search_id, request_number, retries=10):
    url = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,view_count,username,hashtag_names,video_description,create_time,region_code,share_count,like_count,comment_count,music_id,effect_ids,playlist_id,voice_to_text,is_stem_verified,favorites_count,video_duration"
    body['cursor'] = str(cursor)
    if search_id:
        body['search_id'] = str(search_id)

    attempt = 0
    while attempt < retries:
        response = requests.post(
            url,
            headers={'Authorization': f'Bearer {oauth_token}', 'Content-Type': 'application/json'},
            data=json.dumps(body)
        )

        raw_response = response.text
        try:
            response_data = json.loads(convert_large_numbers_to_string(raw_response))
            if 'data' in response_data and 'videos' in response_data['data']:
                convert_int64_strings_to_bigint(response_data)
                total_videos = response_data['data']['videos']
                filtered_videos = filter_videos(total_videos)
                total_video_count = len(total_videos)
                filtered_video_count = len(filtered_videos)
                print(f"Request {request_number} - Total videos returned: {total_video_count} / Filtered videos returned: {filtered_video_count}")
                return total_videos, filtered_videos, response_data['data'].get('cursor'), \
                       response_data['data'].get('has_more'), response_data['data'].get('search_id')
            else:
                print(f"API Error: {response_data.get('error', 'Unexpected response')}", file=sys.stderr)
                # Handle invalid search_id or cursor error
                if response_data.get('code') == 'invalid_params':
                    if 'search_id' in response_data.get('message', ''):
                        return [], [], None, False, None
                    if 'Invalid count or cursor' in response_data.get('message', ''):
                        return [], [], None, False, None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}", file=sys.stderr)
            print(f"Raw response: {raw_response}", file=sys.stderr)

        attempt += 1
        time.sleep(2)

    return [], [], None, False, None


# Filter videos
def filter_videos(videos):
    return [
        {**video, 'url': f"https://www.tiktok.com/@{video['username']}/video/{video['id']}"}
        for video in videos
        if video['view_count'] >= 5000 and video.get('hashtag_names')
    ]


# Recursive function to retrieve videos within a given date range
def retrieve_videos_recursive(query_params):
    cursor = 0
    search_id = None
    all_videos = []
    filtered_videos = []
    total_request_count = 0
    request_info_list = []
    has_more = True

    while has_more:
        total_request_count += 1
        total_videos, filtered_batch, next_cursor, has_more, new_search_id = make_request_and_process(query_params, cursor, search_id, total_request_count)
        all_videos.extend(total_videos)
        filtered_videos.extend(filtered_batch)

        request_info_list.append((total_request_count, cursor, has_more, search_id, len(total_videos), len(filtered_batch)))

        if not has_more:
            break

        if not next_cursor:
            # Reset cursor and search_id if invalid or expired
            cursor = 0
            search_id = None
        else:
            # Update cursor and search_id only if the new ones are valid
            cursor = next_cursor if next_cursor else cursor + len(total_videos)
            search_id = new_search_id if new_search_id else search_id

    return filtered_videos, total_request_count, request_info_list, len(all_videos)


if __name__ == "__main__":
    start_date_str = "20240101"  # Set your desired start date
    final_end_date_str = "20240430"  # Set your desired final end date
    target_filtered_videos = 100  # Set your desired number of filtered videos

    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    final_end_date = datetime.datetime.strptime(final_end_date_str, "%Y%m%d")

    all_filtered_videos = []
    combined_request_info = []
    total_request_count = 0
    total_videos_count = 0
    initial_start_date = start_date_str

    while start_date <= final_end_date:
        # Calculate the next end date, ensuring it does not exceed the final_end_date or the current month's last day
        next_month = start_date.replace(day=28) + datetime.timedelta(days=4)
        last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
        end_date = min(start_date + datetime.timedelta(days=29), final_end_date, last_day_of_month)

        # Reset cursor and search_id for the new date range
        cursor = 0
        search_id = None

        query_params = {
            "query": {
                "and": [
                    {"operation": "IN", "field_name": "keyword", "field_values": ["hashtag"]},
                    {"operation": "EQ", "field_name": "video_length", "field_values": ["MID"]},
                    {"operation": "IN", "field_name": "region_code", "field_values": ["US"]}
                ]
            },
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "max_count": 100,
            "is_random": False
        }

        filtered_videos, request_count, request_info, videos_count = retrieve_videos_recursive(query_params)
        all_filtered_videos.extend(filtered_videos)
        combined_request_info.extend(request_info)
        total_request_count += request_count
        total_videos_count += videos_count

        # Check if the target number of filtered videos has been reached
        if len(all_filtered_videos) >= target_filtered_videos:
            print(f"Target number of filtered videos ({target_filtered_videos}) reached.")
            break

        start_date = end_date + datetime.timedelta(days=1)  # Move to the next date range

    # Determine actual final end date based on the last video retrieved
    if all_filtered_videos:
        last_video_date = max(video['create_time'] for video in all_filtered_videos)
        actual_final_end_date = datetime.datetime.fromtimestamp(last_video_date).strftime("%Y%m%d")
    else:
        actual_final_end_date = final_end_date.strftime("%Y%m%d")

    # Get keyword from query parameters for filename
    keyword = '_'.join(query_params['query']['and'][0]['field_values'])

    formatted_initial_start_date = datetime.datetime.strptime(initial_start_date, "%Y%m%d").strftime("%b%Y").upper()
    formatted_final_end_date = datetime.datetime.strptime(actual_final_end_date, "%Y%m%d").strftime("%b%Y").upper()

    output_filename = f'first_{target_filtered_videos}_FV_{keyword}_{formatted_initial_start_date}_TO_{formatted_final_end_date}.csv'
    output_directory = '/Users/emerson/Github/tiktokbridging/CSV Files'
    output_filepath = os.path.join(output_directory, output_filename)

    # Output filtered videos to CSV with specified column order
    df = pd.DataFrame(all_filtered_videos)
    column_order = [
        "url", "create_time", "favorites_count", "region_code", "video_description",
        "share_count", "comment_count", "hashtag_names", "like_count", "username",
        "id", "music_id", "video_duration", "view_count", "is_stem_verified",
        "playlist_id", "effect_ids", "voice_to_text"
    ]
    df = df[[col for col in column_order if col in df.columns]]  # Only keep columns that exist in df
    df.to_csv(output_filepath, index=False)

    # Print summary of the results
    print(json.dumps({'filtered_videos': all_filtered_videos}))

    print(f'Initial start date: {initial_start_date}')
    print(f'Final end date: {actual_final_end_date}')
    print(f'Total videos returned: {total_videos_count}')
    print(f'Total filtered videos returned: {len(all_filtered_videos)}')
    print(f'Total API requests made: {total_request_count}')
    print(f'Total videos returned by request:')
    for req_num, cursor, has_more, search_id, total_video_count, filtered_video_count in combined_request_info:
        print(f'Request {req_num}: cursor={cursor}, has_more={has_more}, search_id={search_id}, total_videos_returned={total_video_count}, filtered_videos_returned={filtered_video_count}')
