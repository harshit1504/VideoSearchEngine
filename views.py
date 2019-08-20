from flask import Flask, request, render_template
from pymongo import MongoClient
from py2neo import Graph
import MySQLdb
from datetime import datetime

app = Flask(__name__)
url = 'http://localhost:7474'
username = 'neo4j'
password = 'hello'
graph = Graph(url + '/db/data/', username=username, password=password)
db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                     user="root",  # your username
                     passwd="hiten",  # your password
                     db="search_logs")  # name of the data base
cur = db.cursor()
TITLES = []
query_global = 'default'


def get_matching_tags_videos(video_id):
    query = 'match p=(n1)-[r:`Matching Description`]->(n2)<-[s:`Matching Tags`]-(n1) where n1.name=\'' + str(
        video_id) + '\'return  n2.name Order by s.weightage limit(3)'

    return list(graph.run(query))


def get_matching_desc_videos(video_id):
    query = 'match p=(n1)-[r:`Matching Description`]->(n2)<-[s:`Matching Tags`]-(n1) where n1.name=\'' + str(
        video_id) + '\'return  n2.name Order by r.weightage limit(3)'

    return list(graph.run(query))


def get_matching_channel_videos(video_id):
    query = 'match p=(n1)-[r:`Same Channel`]->(n2) where n1.name=\'' + str(
        video_id) + '\'return  n2.name Order by n2.viewCount limit(4)'

    return list(graph.run(query))


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    query = request.form['search_query']
    data_client = MongoClient()
    database = data_client.Hojabc
    videos = database.videos
    global query_global
    global TITLES
    query_global = query
    search_results = videos.find({'$text': {'$search': str(query)}}, {'score': {'$meta': "textScore"}}).sort(
        [('score', {'$meta': "textScore"})])
    search_results = list(search_results)

    titles = []
    thumbnails = []
    for result in search_results:
        thumbnails.append([result['videoInfo']['snippet']['thumbnails']['default']['url'],result['videoInfo']['snippet']['description'][0:200]])

    for result in search_results:
        titles.append([result['videoInfo']['id'], result['videoInfo']['snippet']['localized']['title']])
    TITLES = titles
    return render_template('search.html', data=titles, thumb=thumbnails)


@app.route('/<video_id>')
def video(video_id):
    rank = 0
    global TITLES
    for yo in TITLES:
        rank += 1
        if yo[0] == video_id:
            break
    string = "INSERT INTO search_logs (rank,search_query,video_id,time_stamp) VALUES(" + str(rank) + ',\'' + str(
        query_global) + '\',\'' + str(video_id) + '\',\'' + str(datetime.now().strftime("%Y-%m-%d::%H:%M:%S")) + '\')'
    print(string)
    cur.execute(string,[])
    data_client = MongoClient()
    database = data_client.Hojabc
    videos = database.videos
    active_video = list(videos.find({"videoInfo.id": {'$regex': str(video_id)}}))
    titl = ''
    thum = ''
    for vid in active_video:
        titl = vid['videoInfo']['snippet']['localized']['title']
        thum = vid['videoInfo']['snippet']['thumbnails']['default']['url']

    rel_videos_a = list(get_matching_channel_videos(video_id))
    rel_videos_b = list(get_matching_desc_videos(video_id))
    rel_videos_c = list(get_matching_tags_videos(video_id))
    titles1 = []
    thumbnails1 = []
    rel_videos_a1 = []
    rel_videos_b1 = []
    rel_videos_c1 = []
    for result in rel_videos_a:
        titles1.append([result[0]])

        rel_videos_a1.append(list(videos.find({"videoInfo.id": {'$regex': str(result[0])}})))

    for result in rel_videos_b:
        index =0
        for yup in titles1:

            if yup[0] == result[0]:
                index = 1
        if index ==0:

            titles1.append([result[0]])
            rel_videos_b1.append(list(videos.find({"videoInfo.id": {'$regex': str(result[0])}})))
            index =0
    for result in rel_videos_c:
        index = 0
        for yup in titles1:
            if yup[0] == result[0]:
                index = 1
        if index == 0:
            titles1.append([result[0]])
            rel_videos_c1.append(list(videos.find({"videoInfo.id": {'$regex': str(result[0])}})))
            index =0
    titles = []
    for result in rel_videos_a1:

        thumbnails1.append([result[0]['videoInfo']['snippet']['thumbnails']['default']['url'],result[0]['videoInfo']['snippet']['description'][0:200]])
        titles.append(([result[0]['videoInfo']['id'],result[0]['videoInfo']['snippet']['localized']['title']]))
    for result in rel_videos_b1:
        thumbnails1.append([result[0]['videoInfo']['snippet']['thumbnails']['default']['url'],result[0]['videoInfo']['snippet']['description'][0:200]])
        titles.append(([result[0]['videoInfo']['id'], result[0]['videoInfo']['snippet']['localized']['title']]))
    for result in rel_videos_c1:
        thumbnails1.append([result[0]['videoInfo']['snippet']['thumbnails']['default']['url'],result[0]['videoInfo']['snippet']['description'][0:200]])
        titles.append(([result[0]['videoInfo']['id'], result[0]['videoInfo']['snippet']['localized']['title']]))

    return render_template('play_video.html', video_thumb=thum,
                           video=video_id, video_title=titl, related_vid=titles,thumb_dec = thumbnails1)


if __name__ == '__main__':
    app.run(debug=True)
