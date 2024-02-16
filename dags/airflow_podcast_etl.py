from datetime import datetime, timedelta
from airflow.decorators import dag, task


import requests
import os 
import xmltodict


from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner':'Lostahmado',
    'retries':5,
    "retry_delay":timedelta(minutes=5)
}


@dag(
    default_args = default_args,
    dag_id='podcast_summary',
    schedule='@daily',
    start_date= datetime(2024,2,12),
    catchup=False
)



def podcast_summary():
    
    
    create_table = PostgresOperator(
        task_id = "create_postgres_table",
        postgres_conn_id='postgres_localhost',
        sql="""
        CREATE TABLE IF NOT EXISTS episodes (
            link text primary key,
            title text,
            filename text,
            published date,
            description text
        )
        """
    )        
    
    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes
    
    podcast_episodes = get_episodes()
    create_table >> podcast_episodes


    @task()
    def load_episodes(episodes):
        hook = PostgresHook(postgres_conn_id='postgres_localhost')
        stored = hook.get_pandas_df("SELECT * from episodes")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored["link"].values:
                filename = f"{episode['Link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pudDate"], episode["description"], filename])
        hook.insert_rows(table="episodes", rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])

    load_episodes(podcast_episodes)
    
    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename= f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join("episodes", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episodes["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)
    
    download_episodes(podcast_episodes)

summary = podcast_summary 