import pandas as pd
import numpy  as np
from datetime import datetime
from datetime import timedelta
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


link = 'https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv'
YEAR = 1994 + hash(f'n-shishkova') % 23

default_args = {
    'owner': 'n-shishkova', # владелец дага
    'depends_on_past': False, # зависмость от прошлого
    'retries': 2, # число перезапусков
    'retry_delay': timedelta(minutes=5), # промежуток между запусками
    'start_date': datetime(2024, 7, 22), # начальная дата
    'schedule_interval': '30 10 * * *' # расписание, выражение для Крона
}

columns = ['Rank',
          'Name', # название игры
          'Platform', # платформа
          'Year', # год
          'Genre', # жанр
          'Publisher', # издатель
          'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'] # продажи


@dag(default_args=default_args, catchup=False)
def homework_3_shishkova():
    @task()
    def get_data(retries=3): # считываем файл и определяем год
        games_sales = pd.read_csv(link)
        games_sales = games_sales[games_sales['Year']==YEAR].reset_index()
        return games_sales
    @task(retries=4, retry_delay=timedelta(10)) 
    def get_best_game_global(games_sales): # самая продаваемая игра в мире
        best_game = (games_sales
                            .groupby('Name', as_index=False)
                            .agg({'Global_Sales': 'sum'})
                            .sort_values('Global_Sales', ascending=False).head(1))
        best_game = best_game['Name'].to_string(index=False)
        return best_game
    @task()
    def get_best_genre_EU(games_sales): # самые продаваемые жанры игр в Европе
        best_genre_EU = (games_sales
                            .groupby('Genre', as_index=False)
                            .agg({'EU_Sales': 'sum'})
                            .sort_values('EU_Sales', ascending=False))
        return best_genre_EU['Genre'].to_csv(index=False, header=False)
    @task()
    def get_best_platform_NA(games_sales): # на каких платформах было продано больше всего игр в Северной Америке
        best_platform_NA = (games_sales
                            .query('NA_Sales >= 0.01')
                            .groupby('Platform', as_index=False)
                            .agg({'Name': 'count'})
                            .sort_values('Name', ascending=False).head(5))
        return best_platform_NA['Platform'].to_csv(index=False, header=False)
    @task()
    def get_best_publisher_JP(games_sales): # издатели с самыми высокими средними продажами в Японии
        best_publisher_JP = (games_sales
                             .groupby('Publisher', as_index=False)             
                             .agg({'JP_Sales': 'mean'})             
                             .sort_values('JP_Sales', ascending=False).head(5))
        return best_publisher_JP['Publisher'].to_csv(index=False, header=False)
    @task()
    def get_count_games_EU_JP(games_sales): # кол-во игр, которые лучше продались в Европе, чем в Японии
        games_sales_EU_JP = (games_sales
                             .groupby('Name', as_index=False)             
                             .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}))
        count_games = games_sales_EU_JP.loc[games_sales_EU_JP['EU_Sales'] > games_sales_EU_JP['JP_Sales']].shape[0]
        return count_games
    @task()
    def print_data(best_game, best_genre_EU, best_platform_NA, best_publisher_JP, count_games):

        print(
f'''Дата отчета - {YEAR}

Самая продаваемая игра в мире - {best_game}

Самые продаваемые жанры в Европе - {best_genre_EU}

Топ-5 платформ с большим кол-вом игр, проданные в Северной Америке - {best_platform_NA}

Топ-5 издателей с высокими средними продажами игр в Японии - {best_publisher_JP}

{count_games} игр продались лучше в Европе, чем в Японии
''')

    games_sales = get_data()    
    best_game = get_best_game_global(games_sales)
    best_genre_EU = get_best_genre_EU(games_sales)
    best_platform_NA = get_best_platform_NA(games_sales)
    best_publisher_JP = get_best_publisher_JP(games_sales)
    count_games = get_count_games_EU_JP(games_sales)

    print_data(best_game, best_genre_EU, best_platform_NA, best_publisher_JP, count_games)

homework_3_shishkova = homework_3_shishkova()
                                   






