import os
import re
import sys
import time
import json
import argparse
import traceback
from datetime import datetime as dt
from concurrent.futures import as_completed, ThreadPoolExecutor

import yagmail
import pandas as pd
from sqlalchemy.sql import text
from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

from db import get_db_engine
from logger import get_logger


log = get_logger(__file__)


class UnibetMatchScraper(object):

    def __init__(self, settings, dbconfig, args):
        self.settings = settings
        self.dbconfig = dbconfig
        self.args = args

        self.url = "https://www.unibet.fr"
        self.endpoint = "/sport/{}".format(self.args.sport)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        self.chrome = webdriver.Chrome(self.settings["driver_path"]["value"], chrome_options=options)

        self.matches = {}
        self.events = []
        self.url_events = []

    def scroll_to_bottom(self):
        last_height = self.chrome.execute_script("return document.body.scrollHeight")
        initial_height = last_height
        scroll_to = 1000
        equal_for = 0
        wait = self.settings["page_scroll_wait"]["value"]
        while True:
            self.chrome.execute_script("window.scroll({top: " + str(scroll_to) + ", behavior: 'smooth'});")
            time.sleep(wait)

            new_height = self.chrome.execute_script("return document.body.scrollHeight")
            if new_height == last_height and initial_height < new_height:
                equal_for += 1

            if new_height > last_height:
                equal_for = 0

            if equal_for >= 20:
                log.info("Web page reaches the bottom. Quits scrolling.")
                break

            scroll_to += 1000
            last_height = new_height

    def get_event_concurrent(self, url_event, count):
        match_id = re.search(r".*\/(\S+)\.html", url_event).group(1)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        chrome = webdriver.Chrome(executable_path=self.settings["driver_path"]["value"], chrome_options=options)
        chrome.get(url_event)

        data = []
        try:
            wait = self.settings["page_event_load_timeout"]["value"]
            WebDriverWait(chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "marketbox-item")))
            for elem in chrome.find_elements_by_class_name("ui-collapse-more"):
                elem.click()
                time.sleep(0.5)

            soup = BeautifulSoup(chrome.page_source, "html.parser")
            labels = soup.find_all("span", attrs={"class": "ui-touchlink-needsclick ui-oddbutton"})
            for label in labels:
                children = [child for child in label.children if not isinstance(child, NavigableString)]
                data.append({
                    "gameMatch": self.matches[match_id]["gameMatch"],
                    "label": children[0].find("span", class_="longlabel").get_text().encode("utf-8", "ignore").decode("utf-8", "ignore"),
                    "quoteValue": float(children[2].get_text()),
                    "quoteURL": url_event,
                    "gameMatchId": match_id
                })

            return data
        except (TimeoutException, Exception) as err:
            log.error("Error on loading the events: {}".format(err))
            exit(1)
        finally:
            if count % 5 == 0:
                log.info("{} has been loaded so far ...".format(count))
            chrome.close()

    def get_events(self):
        log.info("{} of match events has to be fetched in total.".format(len(self.url_events)))

        workers = self.settings["workers"]["value"]
        count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for url_event in self.url_events[:1]:
                count += 1
                future = executor.submit(self.get_event_concurrent, url_event, count)
                futures.append(future)

        for future in as_completed(futures):
            self.events.extend(future.result())

    def get(self):
        log.info("Started to scrape {}".format(self.url + self.endpoint))
        self.chrome.get(self.url + self.endpoint)

        try:
            wait = self.settings["page_match_load_timeout"]["value"]
            WebDriverWait(self.chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "bettingbox-item")))
            # self.scroll_to_bottom()
            log.info("Loaded the full web page...")

            soup = BeautifulSoup(self.chrome.page_source, "html.parser")
            cards = soup.find_all(class_="bettingbox-content")
            for idx, date_card in enumerate(cards):
                if idx == self.settings["no_of_days"]["value"]:
                    break

                for row in date_card.find_all(class_="ui-touchlink")[:1]:
                    match = row.find(class_="cell-meta").find(
                        class_="cell-event").get_text().strip().encode("utf-8", "ignore").decode("utf-8", "ignore")
                    href = row.find(class_="cell-meta").find(class_="cell-event").find("a").attrs["href"]
                    quote_url = self.url + href
                    match_id = re.search(r".*\/(\S+)\.html", quote_url).group(1)
                    team1, team2 = match.split(" - ")
                    t1, d, t2 = [span.get_text() for span in row.find(class_="cell-market").find_all(class_="price")]
                    self.matches[match_id] = {
                        "gameMatch": match,
                        "team1": team1,
                        "team2": team2,
                        "quoteTeam1": float(t1),
                        "quoteDraw": float(d),
                        "quoteTeam2": float(t2),
                        "quoteForT1": int(float(t1)),
                        "quoteURL": quote_url,
                        "gameMatchId": match_id
                    }
                    self.url_events.append(quote_url)
        except (TimeoutException, Exception) as err:
            log.error("Error on scarping the page: {}".format(err))
            traceback.print_exc()
            exit(1)
        finally:
            self.chrome.close()

    def delete_from_db(self, conn, query):
        retry, retry_limit = 0, 5
        while True:
            try:
                if retry >= retry_limit:
                    log.error("Retry limit exceeded. Database unreachable/query error in query. Exit!")
                    sys.exit(1)

                conn.execute(query)
                break
            except Exception as e:
                retry += 1
                log.error(f"Error on deleting data from database. Retrying ({retry}) ...")
                print(e)

    def save_in_db(self, conn, query, data):
        log.info(f"{len(data)} rows to be inserted to database.")
        for idx, row in enumerate(data):
            retry, retry_limit = 0, 5
            while True:
                try:
                    if retry >= retry_limit:
                        log.error("Retry limit exceeded. Database unreachable/corrupt in data. Exit!")
                        sys.exit(1)

                    conn.execute(query, **row)
                    if (idx + 1) % 1000 == 0:
                        log.info(f"{idx+1} rows inserted ...")
                    break
                except Exception as e:
                    retry += 1
                    log.error(f"Error on inserting data to database. Retrying ({retry}) ...")
                    print(e)
        log.info(f"Insertion done ({len(data)})")

    def save(self):
        engine = get_db_engine()
        with engine.connect() as conn:
            table_main = self.dbconfig["tables"][self.args.sport]["main"]["table_name"]
            query = text(f"DELETE FROM {table_main}")
            self.delete_from_db(conn, query)
            query = text(f"""
                INSERT INTO {table_main}(gameMatch, team1, team2, quoteTeam1, quoteDraw, quoteTeam2, quoteForT1, quoteURL, gameMatchId) 
                VALUES(:gameMatch, :team1, :team2, :quoteTeam1, :quoteDraw, :quoteTeam2, :quoteForT1, :quoteURL, :gameMatchId);
            """)
            print("                 *** Inserting main to database ***               ")
            self.save_in_db(conn, query, list(self.matches.values()))

            table_detail = self.dbconfig["tables"][self.args.sport]["detail"]["table_name"]
            query = text(f"DELETE FROM {table_detail}")
            self.delete_from_db(conn, query)
            query = text(f"""
                INSERT INTO {table_detail}(gameMatch, label, quoteValue, gameMatchId, quoteURL) 
                VALUES(:gameMatch, :label, :quoteValue, :gameMatchId, :quoteURL);
            """)
            print("                 *** Inserting details to database ***            ")
            self.save_in_db(conn, query, self.events)

    def notify(self):
        data_surebet, data_err = [], []
        table_surebet, table_data_errors = "Nothing to display.", "Nothing to display."

        for match in self.matches.values():
            q1, q2, q3 = match["quoteTeam1"], match["quoteDraw"], match["quoteTeam2"]
            if (1/q1 + 1/q2 + 1/q3) < 1.0:
                data = {}
                for k in ["gameMatchId", "gameMatch", "quoteTeam1", "quoteDraw", "quoteTeam2"]:
                    data[k] = match[k]
                data_surebet.append(data)
        if data_surebet:
            table_surebet = pd.DataFrame(data_surebet).to_html()

        # if data_surebet or data_err:
        if 1 == 1:
            with open("mailcontent.html", "r") as f:
                contents = f.read()
                contents = contents.format(table_surebet=table_surebet, table_data_errors=table_data_errors).replace("\n", "")
                try:
                    user, password, to = self.settings["smtp"]["mail"], self.settings["smtp"]["password"], self.settings["smtp"]["to"]
                    yag = yagmail.SMTP(user=user, password=password)
                    yag.send(to=to, subject='Unibet Scrapy', contents=contents)
                    log.info(f"Validation check. Email sent to {to}.")
                except:
                    log.error("Error on sending the email.. Please check the credentials provided in settings.json")
        else:
            log.error("Validation passed. No email is triggered.")


def get_settings():
    fp = os.path.join(os.getcwd(), "config", "settings.json")
    with open(fp, "r") as f:
        return json.load(f)


def get_dbconfig():
    fp = os.path.join(os.getcwd(), "config", "dbconfig.json")
    with open(fp, "r") as f:
        return json.load(f)


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', "--sport", type=str, required=True)
    arg_parser.add_argument('-log-level', '--log_level', type=str, choices=("INFO", "DEBUG"),
                            default="INFO", help='Where do you want to post the info?')
    return arg_parser.parse_args()


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    settings, dbconfig, args = get_settings(), get_dbconfig(), get_args()
    unibet_match = UnibetMatchScraper(settings, dbconfig, args)
    unibet_match.get()
    unibet_match.get_events()
    unibet_match.notify()
    # unibet_match.save()

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
