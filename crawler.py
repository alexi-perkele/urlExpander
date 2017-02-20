__author__ = 'alexi'
import json
import bs4
import requests
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from slimit import ast
    from slimit.parser import Parser
    from slimit.visitors import nodevisitor
from apiclient import discovery
from urlExpander import SpreadSheet
from urlExpander import unshorten_url

from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count



warnings.filterwarnings('ignore', '.*slimit.*')

url = "https://gleam.io/cwEX6/vrtalkcom-giveaway-win-a-200-gift-card-or-1-of-3-vr-headsets"


def read_config(settings):
    for x in settings["crawler"]:
        yield x


def parse_js(js=None, token=None):
    if not js:
        return
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        warnings.simplefilter("ignore")
        parser = Parser()
        tree = parser.parse(js, debug=False)
        fields = {getattr(node.left, 'value', ''): getattr(node.right, 'value', '')
                  for node in nodevisitor.visit(tree)
                  if isinstance(node, ast.Assign)}
        if not token:
            return fields
    return fields[token]


def scrape_url(url=None):
    if not url:
        return ()
    print("scraping {0}".format(url))
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.text, "lxml")
    try:
        site_url_block = soup.find('div', {'class': 'popup-blocks-container'}).get('ng-init')  # img url also here
    except AttributeError:
        scrape_url(url)
    js_parsed = parse_js(site_url_block)
    try:
        site_name = js_parsed['"site_name"'][1:-1]
    except KeyError:
        site_name = 'unable to scrape site name'
    try:
        site_url = js_parsed['"site_url"'][1:-1]
    except KeyError:
        site_url = "unable to scrape site url"
    try:
        img_url = js_parsed['"url"'].partition("?")[0][1:]  # partition strips string after "?" character
    except KeyError:
        img_url = "unable to scrape img url"
    try:
        starts_at = js_parsed['"starts_at"']
    except KeyError:
        print("unable to scrape start_at!")
        starts_at = "unable to scrape start_at"
    try:
        ends_at = js_parsed['"ends_at"']
    except KeyError:
        print("Unable to scrape ends_at!")
        ends_at = "Unable to scrape ends_at"
    desc = "unable to scrape description"
    for meta in soup.findAll("meta"):
        metaprop = meta.get('property', '').lower()
        if metaprop == "og:description":
            desc = meta['content'].strip()
        if metaprop == 'og:title':
            title = meta['content'].strip()
    return { 'url': url,
            'desc': desc,
            'title': title,
            'site_url': site_url,
            'site_name': site_name,
            'img_url': img_url,
            'starts_at': starts_at,
            'ends_at': ends_at}


if __name__ == "__main__":
    with open('UrlExpanderSettings.json') as json_data:
        Settings = json.load(json_data)

    config = [x for x in read_config(Settings)]

    sp = SpreadSheet(config[0]['SpreadSheetID'])
    pool = ThreadPool(cpu_count())

    sh_urls = [x for x in sp.get_column(config[0]['ShortUrlColumn'])]
    unshorted_pattern_urls = [x for x in sp.get_column(config[0]['PatternLongurlCol'])]
    unshorted_other_urls = [x for x in sp.get_column(config[0]['OtherUrlCol'])]

    urls = pool.map(unshorten_url, sh_urls)
    pool.close()
    pool.join()

    urls2scrapeTemp = [x for x in urls if config[0]['Url_pattern'] in x]

    other_urls = list()
    other_urls = [x for x in urls if x not in unshorted_other_urls]
    sp.write_column(other_urls, config[0]['OtherUrlCol'])

    s = set(unshorted_pattern_urls)
    urls2scrape = [x for x in urls2scrapeTemp if x not in s]

    pool2 = ThreadPool(cpu_count())
    results = pool2.map(scrape_url, urls2scrape)
    # close the pool and wait for the work to finish
    pool2.close()
    pool2.join()

    lng_urls = []
    lng_urls.append([x.get('url', "none") for x in results])
    Descriptions = list()
    Descriptions.append([x.get('desc', "none") for x in results])
    Titles = list()
    Titles.append([x.get('title', "none") for x in results])
    Starts = list()
    Starts.append([x.get('starts_at', "none") for x in results])
    Ends = list()
    Ends.append([x.get('ends_at', "none") for x in results])
    Images = list()
    Images.append([x.get('img_url', "none") for x in results])

    sp.write_column(lng_urls, config[0]['PatternLongurlCol'])
    sp.write_column(Descriptions, config[0]['Description'])
    sp.write_column(Titles, config[0]['Title'])
    sp.write_column(Starts, config[0]['Starts at'])
    sp.write_column(Ends, config[0]['Ends at'])
    sp.write_column(Images, config[0]['Image'])
    print("good bye!")