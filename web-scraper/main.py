# Libreria para poder generar un CLI
import argparse
# Poner cosas más elegantes en la consola en vez de la función print
import logging
logging.basicConfig(level=logging.INFO)

from common import config

logger = logging.getLogger(__name__)

def _news_scraper(news_site_uid):
	host = config()['news_sites'][news_site_uid]['url']
	logging.info('Beginning scraper for %s' % host)
	# logging.info('Beginning scraper for {}'.format(host))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	# Obtener los datos de la configuración
	news_site_choices = list(config()['news_sites'].keys())
	parser.add_argument(
		'news_sites',
		help='The news site that you want to scrape',
		type=str,
		choices=news_site_choices
	)
	# Obtener un objeto
	args = parser.parse_args()
	_news_scraper(args.news_sites)
