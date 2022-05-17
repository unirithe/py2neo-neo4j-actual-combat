from mimetypes import init
import sys
import this
import pandas as pd 
from py2neo import Graph, Node, NodeMatcher, RelationshipMatcher, Subgraph, Relationship
src = sys.argv[0]


'''
    电影的知识图谱类
'''
class MovieKnowledgeGraph:
    def __init__(self) -> this:
        self.data = {}
    '''
        连接 Neo4j
        connect the Neo4j
    '''
    def connect(self, url:str, username:str, password:str) -> this:
        self.graph = Graph(url, auth=(username, password))
        return self
    '''
        加载.csv数据集
        load the .csv data
    '''
    def load_data(self, urlPrefix:str, names:str, urls:list) -> this:
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

    '''
        接下来是关于Neo4j的一些操作, 包括创建节点、建立联系等
        Next is some operations about neo4j, including creating nodes and establishing contacts
    '''
    def clear(self) -> this:
        self.graph.delete_all()
        return self

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
        nodeMatcher = NodeMatcher(self.graph)
        list_rel = []
        for mid, actors in self.dict_actor_movie.items(): 
            node_movie = nodeMatcher.match("movie", movieid=mid).first()
            if node_movie != None:
                for actor in actors:
                    node_actor = nodeMatcher.match("actor", actorid=actor).first()
                    if node_actor != None:
                        list_rel.append(Relationship(node_actor, "acted", node_movie, name='acted'))

        # 批量建立
        # batch build 
        once = 50
        maxi = len(list_rel)
        for i in range(0, maxi, once):
            subgraph = Subgraph(relationships=list_rel[i:i+once])
            self.graph.separate(subgraph)
            self.graph.create(subgraph)
            print(f'[INFO] >> created {len(subgraph)} relations')
        return self
    '''
        接下来是数据分析部分（简单版本）
        Next is the data analysis part (simple version)
    '''
    # 显示所有受欢迎电影与演员的关系
    # Show the relationship between all popular movies and actors
    def showPopularityAllRelations(self) -> this:
        assert self.graph != None, 'Please connect the correct Neo4j.'
        rmatcher = RelationshipMatcher(self.graph)
        i = 0
        for node_movie in self.graph.nodes.match('movie').all():
            print(i, '-' * 10 , node_movie['name'] + '-' *10)
            for rel in self.graph.match([None, node_movie]).all():
                print('--', rel)
            i += 1
            print('\n\n')
        return self
    # 显示 Top10电影（根据演员出现数和电影评分而定）
    # Show top 10 movies (depending on the number of actors and movie ratings)
    def showTop10Movie(self) -> this:
        assert self.graph != None, 'Please connect the correct Neo4j.'
        nodes_movie = self.graph.nodes.match('movie').all()
        rm = RelationshipMatcher(self.graph)
        dict_movie_top10 = {}
        for node_movie in nodes_movie:
            list_actors = rm.match([None, node_movie], r_type='acted').all()
            count = len(list_actors)
            dict_movie_top10.update({node_movie: {'count':int(count), 'actors':list_actors}})

        self.list_movie_top10 = sorted(dict_movie_top10.items(), 
                key = lambda k : (k[1]['count'], float(k[0]['rate'])), reverse=True)[:10]

        # list_movie_top10 is a list([turple(Node, dict)])
        print('------------------ Top10 ------------------')
        for node_movie, dict_count in self.list_movie_top10:
            print(dict_count['count'], node_movie['rate'], node_movie['name'])
        return self
    def saveTop10(self) -> this:
        assert self.graph != None, 'Please connect the correct Neo4j.'
        g = self.graph
        g.delete(Subgraph(g.nodes.match('actor_top10').all()))
        g.delete(Subgraph(g.nodes.match('movie_top10').all()))
        g.delete(Subgraph(RelationshipMatcher(g).match(name='acted_top10')))
        rel_top10 = []
        nodeMatcher = NodeMatcher(g)
        for node_movie, dict_count in self.list_movie_top10:
            for actor_rel in dict_count['actors']:

                actor = Node('actor_top10', **dict(actor_rel.start_node))
                movie = Node('movie_top10', **dict(node_movie))

                actor_find = nodeMatcher.match('actor_top10', name=actor['name']).first()
                movie_find = nodeMatcher.match('movie_top10', name=movie['name']).first()
                if actor_find != None: ator = actor_find 
                if movie_find != None: movie = movie_find
                
                rel_top10.append(Relationship(actor, "acted", movie, name='acted_top10'))
                sub_rels=Subgraph(relationships=rel_top10)
                g.separate(subgraph=sub_rels)
                g.create(subgraph = sub_rels)

        print('The number of actor_top10 node: ',g.nodes.match('actor_top10').count())
        print('The number of moive_top10 node: ', g.nodes.match('movie_top10').count())
        print('The number of relationsip: ', g.relationships.match(name='acted_top10').count())
        print('Save Done. You can select them in neo4j')
    def run(self):
        assert self.graph != None, 'Please connect the correct Neo4j.'
        self.cal_popularity_movie().cal_popularity_actor_movie().cal_popularity_actor()\
            .clear().insertNodeWithActorsAndMovies().insertRelationShip()\
            .showPopularityAllRelations().showTop10Movie().saveTop10()
if __name__ == '__main__':
    url = "http://localhost:7474"
    username = "neo4j"
    password = "123456"
    MovieKnowledgeGraph()\
        .connect(url, username, password)\
        .load_data(
            urlPrefix=src + '/../dataset/',
            names=['actor_movie', 'actor', 'movie', 'popularity', 'user'], 
            urls=['movie_act.csv', 'movie_actor.csv', 'movie_movie.csv', 'movie_popularity.csv', 'user_user.csv'])\
        .run()