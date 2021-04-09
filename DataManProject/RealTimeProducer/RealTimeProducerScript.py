from RealTimeProducer.tweetScraper import start_scrape_tweet
from RealTimeProducer.btcScraper import ScrapeBTC
from RealTimeProducer.tpsScraper import ScrapeTPS
from Utilities import parallel_run, parallel_create

if __name__ == '__main__':
    t_scraper_btc = parallel_create(ScrapeBTC)
    parallel_run(t_scraper_btc)
    t_scraper_tps = parallel_create(ScrapeTPS)
    parallel_run(t_scraper_tps)
    t_scraper_tweet = parallel_create(start_scrape_tweet)
    parallel_run(t_scraper_tweet)
    t_scraper_tweet.join()
