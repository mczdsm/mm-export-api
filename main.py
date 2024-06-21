import os
import sqlite3
from datetime import datetime, date
from typing import Tuple, Dict, List
import pathlib
import json
from flask import Flask, request, jsonify

from mattermostdriver import Driver

app = Flask(__name__)

def connect(host: str, login_token: str = None, username: str = None, password: str = None) -> Driver:
    d = Driver({
        "url": host,
        "port": 443,
        "token": login_token,
        "username": username,
        "password": password
    })
    d.login()
    return d

def get_users(d: Driver) -> Tuple[Dict[str, str], str]:
    my_user = d.users.get_user("me")
    my_username = my_user["username"]
    my_user_id = my_user["id"]

    # Get all usernames as we want to use those instead of the user ids
    user_id_to_name = {}
    page = 0
    while True:
        users_resp = d.users.get_users(params={"per_page": 200, "page": page})
        if len(users_resp) == 0:
            break
        for user in users_resp:
            user_id_to_name[user["id"]] = user["username"]
        page += 1

    return user_id_to_name, my_user_id

def select_team(d: Driver, my_user_id: str) -> Dict:
    teams = d.teams.get_user_teams(my_user_id)
    return teams

def select_channel(d: Driver, team_id: str, my_user_id: str, user_id_to_name: Dict[str, str], verbose: bool = False) -> List[Dict]:
    channels = d.channels.get_channels_for_user(my_user_id, team_id)
    # Add display name to direct messages
    for channel in channels:
        if channel["type"] != "D":
            continue

        # The channel name consists of two user ids connected by a double underscore
        user_ids = channel["name"].split("__")
        other_user_id = user_ids[1] if user_ids[0] == my_user_id else user_ids[0]
        channel["display_name"] = user_id_to_name[other_user_id]
    # Sort channels by name for easier search
    channels = sorted(channels, key=lambda x: x["display_name"].lower())

    return channels

def export_channel(d: Driver, channel: str, user_id_to_name: Dict[str, str], download_files: bool = True, before: str = None, after: str = None) -> dict:
    # Sanitize channel name
    channel_name = channel["display_name"].replace("\\", "").replace("/", "")

    print("Exporting channel", channel_name)
    if after:
        after = datetime.strptime(after, '%Y-%m-%d').timestamp()
    if before:
        before = datetime.strptime(before, '%Y-%m-%d').timestamp()

    # Get all posts for selected channel
    page = 0
    all_posts = []
    while True:
        print(f"Requesting channel page {page}")
        posts = d.posts.get_posts_for_channel(channel["id"], params={"per_page": 200, "page": page})

        if len(posts["posts"]) == 0:
            # If no posts are returned, we have reached the end
            break

        all_posts.extend([posts["posts"][post] for post in posts["order"]])
        page += 1
    print(f"Found {len(all_posts)} posts")
    
    # Simplify all posts to contain only username, date, message and files in chronological order
    simple_posts = []
    for i_post, post in enumerate(reversed(all_posts)):

        # Filter posts by date range
        created = post["create_at"] / 1000
        if (before and created > before) or (after and created < after):
            continue

        user_id = post["user_id"]
        if user_id not in user_id_to_name:
            user_id_to_name[user_id] = d.users.get_user(user_id)["username"]
        username = user_id_to_name[user_id]
        created = datetime.utcfromtimestamp(post["create_at"] / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
        message = post["message"]
        simple_post = dict(idx=i_post, id=post["id"], created=created, username=username, message=message)

        # If a code block is given in the message, add it to the simple_post
        if message.count("```") > 1:
            start_pos = message.find("```") + 3
            end_pos = message.rfind("```")

            cut = message[start_pos:end_pos]
            if not len(cut):
                print("Code has no length")
            else:
                simple_post["code"] = cut

        # If any files are attached to the message, add each to the simple_post
        if "files" in post["metadata"]:
            filenames = []
            for file in post["metadata"]["files"]:
                if download_files:
                    filename = "%03d" % i_post + "_" + file["name"]
                    print("Downloading", file["name"])
                    while True:
                        try:
                            resp = d.files.get_file(file["id"])
                            break
                        except:
                            print("Downloading file failed")
                    filenames.append(file["name"])
            simple_post["files"] = filenames
        simple_posts.append(simple_post)

    output = {
        "channel": {
            "name": channel["name"],
            "display_name": channel["display_name"],
            "header": channel["header"],
            "id": channel["id"],
            "team": d.teams.get_team(channel["team_id"])["name"],
            "team_id": channel["team_id"],
            "exported_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        },
        "posts": simple_posts
    }

    return output


@app.route('/export', methods=['POST'])
def export():
    data = request.json
    config = data.get('config')
    team_id = data.get('team_id')
    channels = data.get('channels')

    d = connect(config["host"], config.get("token", None),
                config.get("username", None), config.get("password", None))
    user_id_to_name, my_user_id = get_users(d)

    result = []
    for channel_id in channels:
        channel = d.channels.get_channel(channel_id)
        result.append(export_channel(d, channel, user_id_to_name, config["download_files"],
                                     config.get("before"), config.get("after")))

    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
