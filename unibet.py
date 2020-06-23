import time
import json
import argparse
import traceback
from datetime import datetime as dt
from concurrent.futures import as_completed, ThreadPoolExecutor

import pandas as pd
from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

from logger import get_logger


log = get_logger(__file__)


class UnibetMatchScraper(object):

    def __init__(self, settings, args):
        self.settings = settings
        self.args = args

        self.url = "https://www.unibet.fr"
        self.endpoint = "/sport/{}".format(self.args.sport)
        self.chrome = webdriver.Chrome(self.settings["driver_path"]["value"])
        self.matches = []
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
                    "Label": children[0].find("span", class_="longlabel").get_text(),
                    "Value": children[2].get_text(),
                    "Timestamp": ""
                })

            return data
        except (TimeoutException, Exception) as err:
            log.error("Error on loading the events: {}".format(err))
            exit(1)
        finally:
            if count % 2 == 0:
                log.info("{} has been loaded so far ...".format(count))
            chrome.close()

    def get_events(self):
        log.info("{} of match events has to be fetched in total.".format(len(self.url_events)))

        workers = self.settings["workers"]["value"]
        count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for url_event in self.url_events:
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
            self.scroll_to_bottom()
            log.info("Loaded the full web page...")

            soup = BeautifulSoup(self.chrome.page_source, "html.parser")
            for date_card in soup.find_all(class_="bettingbox-content"):
                for row in date_card.find_all(class_="ui-touchlink"):
                    match = row.find(class_="cell-meta").find(class_="cell-event").get_text().strip()
                    href = row.find(class_="cell-meta").find(class_="cell-event").find("a").attrs["href"]
                    team1, team2 = match.split(" - ")
                    t1, d, t2 = [span.get_text() for span in row.find(class_="cell-market").find_all(class_="price")]
                    self.matches.append({
                        "Match": match,
                        "Team1": team1,
                        "Team2": team2,
                        "Quote for T1": t1,
                        "Quote Draw": d,
                        "Quote for T2": t2,
                        "Timestamp": ""
                    })
                    self.url_events.append(self.url + href)
        except (TimeoutException, Exception) as err:
            log.error("Error on scarping the page: {}".format(err))
            traceback.print_exc()
            exit(1)
        finally:
            self.chrome.close()

    def save(self):
        writer = pd.ExcelWriter('{}.xlsx'.format(self.args.sport))

        df_matches = pd.DataFrame(self.matches)
        df_matches.to_excel(writer, 'matches', index=False, engine='xlsxwriter', encoding="UTF-8")

        df_events = pd.DataFrame(self.events)
        df_events.to_excel(writer, 'events', index=False, engine='xlsxwriter', encoding="UTF-8")

        writer.save()


def get_settings():
    with open("settings.json", "r") as f:
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

    settings, args = get_settings(), get_args()
    unibet_match = UnibetMatchScraper(settings, args)
    unibet_match.get()
    unibet_match.get_events()
    unibet_match.save()

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
