from lxml import etree
import urllib2
import pandas as pd
from datetime import datetime
from multiprocessing import Pool

def url2bs(url, htmlparser):
    """
    converts to lxml tree
    :param url: url
    :returns an lxml tree
    """
    print url
    response = urllib2.urlopen(url)
    tree = etree.parse(response, htmlparser)
    return tree

def get_match_data(i):
    base_url = 'http://www.premierleague.com/match/'
    htmlparser = etree.HTMLParser()

    tree = url2bs(base_url + str(i), htmlparser)

    try:
        # get date/season
        match_date = datetime.strptime(tree.xpath('//div[@class="matchDate"]')[0].text, '%A %d %B %Y')

        if match_date.month < 8:
            season = '%d-%d'% (match_date.year - 1, match_date.year)
        else:
            season = '%d-%d'% (match_date.year, match_date.year + 1)

        # get matchweek
        match_week = int(tree.xpath('//div[@class="long"]')[0].text.split(" ")[1])

        # get home/away team names
        home_team = tree.xpath('//div[@class="team home"]//span[@class="long"]')[0].text
        away_team = tree.xpath('//div[@class="team away"]//span[@class="long"]')[0].text

        # team status
        goals = [{'gameID': i, 'date': match_date, 'season': season, 'matchWeek': match_week, 'team': home_team,
                  'isHomeTeam': True, 'opponent': away_team, 'minute': int(x.tail.strip().split('+')[0]),
                  'player': ' '.join(x.xpath('//div[@class="eventPlayerInfo"]//a')[0].text.strip().split(' ')[1:]),
                  'isGameWinner': False}
                 if x.getparent().get('class') == 'event home' else
                 {'gameID': i, 'date': match_date, 'season': season, 'matchWeek': match_week, 'team': away_team,
                  'isHomeTeam': False, 'opponent': home_team, 'minute': int(x.tail.strip().split('+')[0]),
                  'player': ' '.join(x.xpath('//div[@class="eventPlayerInfo"]//a')[0].text.strip().split(' ')[1:]),
                  'isGameWinner': False}
                 for x in
                 tree.xpath('//div[@class="eventLine timeLineEventsContainer"]//span[@class="icn ball-sm-w"]')]

        home_goals = 0
        away_goals = 0
        for goal in goals:
            # status: 0 still behind, 1 to tie, 2 go ahead, 3 increase lead
            if goal['isHomeTeam']:
                home_goals += 1
                if home_goals - away_goals == 1:
                    goal['status'] = 2
                elif home_goals > away_goals:
                    goal['status'] = 3
                elif home_goals == away_goals:
                    goal['status'] = 1
                else:
                    goal['status'] = 0
            else:
                away_goals += 1
                if away_goals - home_goals == 1:
                    goal['status'] = 2
                elif away_goals > home_goals:
                    goal['status'] = 3
                elif away_goals == home_goals:
                    goal['status'] = 1
                else:
                    goal['status'] = 0

        goal['isGameWinner'] = True

        print("\033c")
        print "Game ID: %d" % i
        print "Match Date: %s" % match_date
        print 'Season: %s' % season
        print 'Match Week: %d' % match_week
        print goals

        return goals
    except:
        print 'game %d failed' % i


if __name__ == "__main__":
    p = Pool(processes=None)

    all_data = p.map(get_match_data, xrange(1, 200))  # 12494

    p.close()

    print pd.DataFrame(all_data)  # change this to output to file eventually