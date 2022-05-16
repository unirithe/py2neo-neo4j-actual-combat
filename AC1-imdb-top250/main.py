import sys
import pandas as pd 
src = sys.argv[0]

'''
    加载数据集
'''
def load_data() -> dict[str:pd.DataFrame]:
    df_actor_movie = pd.read_csv(src + '/../dataset/movie_act.csv')
    df_actor = pd.read_csv(src + '/../dataset/movie_actor.csv')
    df_movie = pd.read_csv(src + '/../dataset/movie_movie.csv')
    df_popularity = pd.read_csv(src + '/../dataset/movie_popularity.csv')
    df_user = pd.read_csv(src + '/../dataset/user_user.csv')
    return {
        'actor_movie' : df_actor_movie,
        'actor': df_actor,
        'movie': df_movie,
        'popularity': df_popularity,
        'user': df_user
    }
print(load_data())