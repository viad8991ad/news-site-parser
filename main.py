import locale
import sqlite3
from datetime import datetime
from urllib import request
from bs4 import BeautifulSoup
from flask import Flask, render_template
from timeloop import Timeloop
from datetime import timedelta
from config import settings


timer = Timeloop()
app = Flask(__name__)


create_table_connect = sqlite3.connect("resource/news.db")
create_table_cursor = create_table_connect.cursor()
create_table_cursor.execute("""
CREATE TABLE IF NOT EXISTS news 
    (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        title TEXT NOT NULL, 
        text TEXT NOT NULL,
        link_topic TEXT NOT NULL,
        date_publication DATETIME NOT NULL,
        count_view BIGINT8 DEFAULT 0,
        count_comments BIGINT8 DEFAULT 0
    )
""")


@app.route('/')
def root():
    result = []
    with sqlite3.connect("resource/news.db") as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM news")
        result_select = cur.fetchall()
        if result_select is not None:
            for item in result_select:
                result.append([item[0], item[1], item[2], item[3], item[4], item[5], item[6]])
        else:
            result.append(['404', 'Данные отсутсвуют', '', '', '', '', ''])

    return render_template('main.html', data=result)


@timer.job(interval=timedelta(minutes=settings['hours_step']))
def job_news_job():
    log('getting started job')
    log('getting html with a list of news')
    main_html = get_html(url_site() + '/text/')
    if main_html is not None:
        log('getting html with a list of news')
        description_topics = get_links_and_description_topics(main_html)
        log(f'get {len(description_topics)} records')
        for item in description_topics:
            html_topic = get_html(item[0])
            text = get_text_topic(html_topic)
            with sqlite3.connect("resource/news.db") as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM news WHERE title = ? AND link_topic = ?", (item[1], item[0]))
                result = cur.fetchone()
                if result is None:
                    value = (None, item[1], text, item[0], item[4], item[2], item[3], )
                    cur.execute("""INSERT INTO news VALUES (?, ?, ?, ?, ?, ?, ?)""", value)
                    conn.commit()
                    log('insert new row|{} - {}'.format(item[1][:75] + '...', item[0]))
                else:
                    cur.execute("""
                    UPDATE news SET count_view = ?, count_comments = ? 
                    WHERE  title = ? AND link_topic = ?""", (item[2], item[3], item[1], item[0]))
                    conn.commit()
                    log('update row|{} - {}'.format(item[1][:75] + '...', item[0]))


def get_html(url: str):
    log(f'get HTML from site: \"{url}\"')
    query = request.urlopen(url)
    code_response = query.getcode()
    if code_response == 200:
        soup = BeautifulSoup(query.read(), 'html.parser')
        return str(soup)
    else:
        log_error(f'code response is {code_response}')
        return None


def get_links_and_description_topics(html: str):
    log('Получение ссылок на топики')
    result = []
    soup = BeautifulSoup(html, "html.parser")
    div_topics = soup.find_all('div', class_='MZa1t')
    for div_topic in div_topics:
        a_topic = div_topic.find('div', class_='MZa1p').find('a')
        topic_href = url_site() + a_topic['href']
        topic_title = a_topic['title']

        topic_info = div_topic.find('div', class_='MZa13')
        topic_view, topic_comments = view_comments(topic_info)
        topic_time = time_parse(topic_info.find('time').find('a').text)

        result.append([topic_href, topic_title, topic_view, topic_comments, topic_time])
    return result


def view_comments(html):
    div = html.find('div', class_='MZa03')
    list_span = div.find_all('span')
    view_count = list_span[0].text.replace(' ', '')
    comments = list_span[1].text
    if comments == 'Обсудить':
        comments_count = 0
    else:
        comments_count = comments.replace(' ', '')
    return view_count, comments_count


def get_text_topic(html):
    result = ''
    soup = BeautifulSoup(html, "html.parser")
    div_topic = soup.find_all('div', class_='L9ay3')
    for item in div_topic:
        p_texts = item.find_all('p')
        for texts in p_texts:
            result += texts.text + ' '
    return result


def url_site():
    return 'https://v1.ru'


def time_parse(time_str: str):
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
    return datetime.strptime(time_str, '%d %B %Y, %H:%M')


def log(log_info_str):
    print(f'\033[94m[{datetime.now()}] - {log_info_str}\033[0m')


def log_error(log_error_str):
    print(f'\033[91m[{datetime.now()}] - {log_error_str}\033[0m')


timer.start(block=False)

if __name__ == '__main__':
    app.run()
