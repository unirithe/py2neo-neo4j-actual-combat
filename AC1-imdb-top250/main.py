from mimetypes import init
import sys
import this
import pandas as pd 
from py2neo import Graph, Node, NodeMatcher, RelationshipMatcher, Subgraph
src = sys.argv[0]


'''
    电影的知识图谱类
'''
class MovieKnowledgeGraph:
    def __init__(self) -> None:
        self.data = {}
    '''
        连接 Neo4j
        connect the Neo4j
    '''
    def connect(self, url:str, username:str, password:str) -> None:
        self.graph = Graph(url, auth=(username, password))
        return self
    '''
        加载.csv数据集
        load the .csv data
    '''
    def load_data(self, urlPrefix:str, names:str, urls:list) -> None:
        len1, len2 = len(names), len(urls)
        assert len1 == len2, 'Please input the same number of names and urls.'
        for i in range(len(names)):
            self.data[names[i]] = pd.read_csv(urlPrefix + urls[i])

        # 备注: 执行的顺序不能颠倒
        # Note: the order of execution cannot be reversed
        self.cal_popularity_movie()
        self.cal_popularity_actor_movie()
        self.cal_popularity_actor()
        return self
    '''
        接下来的方法是基于数据集的一些计算，若数据集不同，则可以忽略
        The next method is based on some calculations of the dataset. If the dataset is different, it can be ignored
    '''

    def cal_popularity_movie(self) -> this:
        list_popularity_movieid = self.data['popularity']['movieid_id']

        # 查找受欢迎的电影信息
        flag = self.data['movie']['movieid'].isin(list_popularity_movieid)
        # Find the movies which is popularity 
        self.df_popularity_movie = self.data['movie'][flag]

        # 将DataFrame格式转化为dict，到时候方便插入Neo4j
        # make DataFrame to Dict, in order to insert neo4j
        self.dict_movie = {}

        for i in range(len(self.df_popularity_movie)):
            row = self.df_popularity_movie.iloc[i]
            self.dict_movie.update({row['movieid'] : row.to_dict()})
        return self
    def cal_popularity_actor_movie(self) -> this:
        self.dict_actor_movie = {}
        for mid in self.df_popularity_movie['movieid']:
            flag = self.data['actor_movie']['movieid_id'].eq(mid)
            actors = self.data['actor_movie'][flag]['actorid_id'].to_list()
            self.dict_actor_movie.update({mid : actors})
        return self
    def cal_popularity_actor(self) -> this:
        self.dict_actor = {}
        actors = set()
        for ac in self.dict_actor_movie.values():
            for actor in ac:
                actors.add(actor)
        for aid in actors:
            flag = (self.data['actor']['actorid'] == aid)
            row = self.data['actor'][flag].iloc[0]
            self.dict_actor.update({aid: row.to_dict()})
        return self
    def clear(self) -> this:
        self.graph.delete_all()
    def insertNodeWithActorsAndMovies(self) -> this:
        assert self.graph != None, 'Please connect the correct Neo4j.'
        self.clear()
        node_list = []
        for mid, movie in self.dict_movie.items():
            node_list.append(Node("movie", **movie))
        for aid, actor in self.dict_actor.items():
            node_list.append(Node("actor", **actor))
        self.graph.create(subgraph=Subgraph(node_list))
        return self
    def insertRelationShip(self):
        assert self.graph != None, 'Please connect the correct Neo4j.'
        RelationshipMatcher
if __name__ == '__main__':
    url = "http://localhost:7474"
    username = "neo4j"
    password = "123456"
    mkg =  MovieKnowledgeGraph().connect(url, username, password)
    mkg.load_data(
        urlPrefix=src + '/../dataset/',
        names=['actor_movie', 'actor', 'movie', 'popularity', 'user'], 
        urls=['movie_act.csv', 'movie_actor.csv', 'movie_movie.csv', 'movie_popularity.csv', 'user_user.csv'])

    mkg.cal_popularity_movie().cal_popularity_actor_movie().cal_popularity_actor()
    print(mkg.dict_actor)