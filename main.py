import time
import requests
import pytest
from bs4 import BeautifulSoup
import auth_data # Данные для авторизации (токен vkapi, название, хост, логин и пароль от базы данных MySQL, путь до файла входных данных)
import mysql.connector
version = '5.92'

db = mysql.connector.connect(
    host=auth_data.host,
    user=auth_data.user,
    passwd=auth_data.password
  )


def get_id(user_nickname: str) -> str:
  """Перевод символьного ника (user_nickname) в числовое значение id"""
  if user_nickname.isnumeric():
    return user_nickname

  url = f'https://api.vk.com/method/users.get?user_ids={user_nickname}&access_token={auth_data.token}&v={version}'

  while True:
    try:
      response = requests.get(url)
      return response.json()['response'][0]['id']

    except KeyError:
      time.sleep(1)
      continue


def get_videos_vkapi(user_id: int) -> list[tuple[str, str, str]]:
  """Возвращает список видео (id, название, ссылка) со страницы пользователя (user_id) с помощью vkapi"""
  videos = [] # Найденные видеоролики
  offset = 0

  while True:
    url = f'https://api.vk.com/method/video.get?owner_id={user_id}&access_token={auth_data.token}&v={version}&count=200&offset={offset}'
    response = requests.get(url=url).json()
    # Пустой список видео в случае приватного профиля
    if 'error' in response:
      return []

    items = response['response']['items']

    if len(items) == 0:
      break

    offset += 200
    videos.extend(items)

  return list[str](map(lambda x: (x['id'], x['title'], x.get('player')), videos))


def get_videos_no_vkapi(user_id: str) -> list[tuple[str, str, str]]:
  """Возвращает список видео (id, название, ссылка) со страницы пользователя (user_id) без vkapi"""
  try:
    url = f'https://vk.com/video/@id{user_id}'
    response = requests.get(url, auth=('', ''))
    soup = BeautifulSoup(response.text, 'lxml')
    div = soup.find(name='div', id_='video_subtab_pane_all')
    videos = div.find_all(name='a', class_='VideoCard__title')

    return [(video.get('data-id').split('_')[1], video.text, f'https://vk.com{video.get("href")}') for video in videos]

  except Exception:
    return []


def get_videos_by_uid(user_id: str) -> list[tuple[str, str, str]]:
  """Возвращает список видео (id, название, ссылка) со страницы пользователя (user_id)"""
  videos = get_videos_vkapi(user_id)

  if len(videos) == 0:
    print('VK API got 0 videos. Trying simple http request... ')
    videos = get_videos_no_vkapi(user_id)

  return videos


def create_tables() -> None:
  """Создание таблиц users, videos, uservideo"""
  print('Creating tables in database...')
  cursor = db.cursor()
  cursor.execute(f"""USE {auth_data.db_name}""")
  cursor.execute("""CREATE TABLE IF NOT EXISTS `users` (
           `id` int(11) NOT NULL,
           `nickname` text NOT NULL,
           PRIMARY KEY (`id`)
           )""")
  db.commit()

  cursor.execute("""CREATE TABLE IF NOT EXISTS `videos` (
          `id` int(11) NOT NULL,
          `title` text NOT NULL,
          `url` text,    
          PRIMARY KEY (`id`)
          )""")
  db.commit()

  cursor.execute("""CREATE TABLE IF NOT EXISTS `uservideo` (
          `user_id` int(11) NOT NULL,
          `video_id` int(11) NOT NULL,
          FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE,
          FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE,
          PRIMARY KEY (`user_id`,`video_id`)
          )""")
  db.commit()
  pass


def insert_user(user_nick: str) -> None:
  """Добавление пользователя (user_nick) в users"""
  cursor = db.cursor()
  user_id = get_id(user_nick)
  cursor.execute(f"""INSERT IGNORE INTO {auth_data.db_name}.users (id, nickname) VALUES ({user_id}, '{user_nick}')""")
  db.commit()
  print(f'Added user {user_nick}.')
  pass


def insert_video(user_id: str, video: tuple[str, str, str]) -> None:
  """Добавление видео в videos и связка с users"""
  cursor = db.cursor()
  cursor.execute(f"""INSERT IGNORE INTO {auth_data.db_name}.videos (id, title, url) VALUES (%s, %s ,%s)""", params=video)
  db.commit()
  cursor.execute(f"""INSERT IGNORE INTO {auth_data.db_name}.uservideo (user_id, video_id) VALUES ({user_id}, {video[0]})""")
  db.commit()
  pass


def scan_user(user) -> None:
  """Для пользователя (user) заполнить таблицы videos и uservideo"""
  user_id = user[0]
  print(f'Scan user {user[1].encode()}...')
  videos = get_videos_by_uid(user_id)
  for video in videos:
    insert_video(user_id, video)
  print(f'Added {len(videos)} videos.')
  pass


def scan_users() -> None:
  """Для каждого пользователя users заполнить таблицы videos и uservideo"""
  cursor = db.cursor()
  cursor.execute(f"""SELECT * FROM {auth_data.db_name}.users""")
  users = cursor.fetchall()

  for user in users:
    scan_user(user)
  pass


def add_users(filename: str) -> None:
  """Добавление пользователей в базу данных из текстового файла (filename)"""
  print('Adding users...')
  with open(filename, 'r') as file:
    while True:
      user = file.readline()

      if user == '':
        break

      insert_user(user.strip())

  pass

def main():
  start = time.time()
  #create_tables()
  #add_users(auth_data.filename)
  scan_users()
  print(f'Elapsed time: {time.time()-start}s')
  
  pass


if __name__ == '__main__':
  main()


class TestClassSF:
  def test_1(self):
    offset = 0
    assert offset != 0
  
  def test_2(self):
    items=[]
    assert len(items) == 0

  def test_3(self):
    assert auth_data.db_name == "users"
  
  def test_4(self):
    assert auth_data.db_name == "videos"

  def test_5(self):
    params=0
    video=0
    assert params==video
  
  def test_6(self):
    assert auth_data.host == "localhost"

  def test_7(self):
    assert auth_data.user == "root"
  
  def test_8(self):
    assert auth_data.user == ""

  def test_9(self):
    response = ""
    assert response != ""
  
  def test_10(self):
    assert get_videos_by_uid(test)