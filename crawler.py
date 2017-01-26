__author__ = 'alexi'
import json
import bs4
import requests
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor



url = "https://gleam.io/cwEX6/vrtalkcom-giveaway-win-a-200-gift-card-or-1-of-3-vr-headsets"

def read_config(settings):
    for x in settings["crawler"]:
        yield x['url'],\
              x['xpath1'],\
              x['xpath2'],\
              x['xpath3']

def parse_js(js=None, token=None):
    if not js:
        return
    parser = Parser()
    tree = parser.parse(js, debug=False)
    fields = {getattr(node.left, 'value', ''): getattr(node.right, 'value', '')
          for node in nodevisitor.visit(tree)
          if isinstance(node, ast.Assign)}
    if not token:
        return fields
    return fields[token]

def scrape_url(url=None):
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.text, "lxml")

#    days_left_block = soup.find('div', {'class': 'campaign competition language-en'}).get('ng-init')
    site_url_block = soup.find('div', {'class': 'popup-blocks-container'}).get('ng-init')  # img url also here

    js_parsed = parse_js(site_url_block)

    site_name = js_parsed['"site_name"'][1:-1]
    site_url = js_parsed['"site_url"'][1:-1]
    img_url = js_parsed['"url"'].partition("?")[0][1:] #  partition strips string after "?" character
    starts_at = js_parsed['"starts_at"']
    ends_at = js_parsed['"ends_at"']

    for meta in soup.findAll("meta"):
#        metacontent = meta.get('content', '').lower()
        metaprop = meta.get('property', '').lower()
        if metaprop == "og:description":
            desc = meta['content'].strip()
            print("bingo!")
        if metaprop == 'og:title':
            title = meta['content'].strip()

    print("Description: {0}".format(desc))
    print("Title: {0}".format(title))
    print("Site url: {0}".format(site_url))
    print("Site name: {0}".format(site_name))
    print("IMG url: " + img_url)
    print("Starts at: " + starts_at)
    print("Ends at: " + ends_at)
    return

if __name__ == "__main__":
    with open('UrlExpanderSettings.json') as json_data:
        Settings = json.load(json_data)

    #[print(k) for k in read_config(Settings)]
    print(Settings['crawler'][0]['url'])
    scrape_url(Settings['crawler'][0]['url'])
