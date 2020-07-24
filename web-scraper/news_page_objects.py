import requests
import bs4

from common import config

class HomePage:
	def __init__(self, news_site_uid, url):
		self._config = config()['news_sites'][news_site_uid]
		self._queries = self._config['queries']
		self._html = None

		self._visit(url)

	# Visitar la página
	def _visit(self, url):
		response = requests.get(url)
		# Método que nos permite manejar un error
		response.raise_for_status()
		# Y si no hubo un error:
		self._html = bs4.BeautifulSoup(response.text, 'html.parser')
	
	# Obtener información del html que se ha parseado
	def _select(self, query_string):
		return self._html.select(query_string)
	
	# Generar nuestra primera propiedad
	@property
	def article_links(self):
		link_list = []
		for link in self._select(self._queries['homepage_article_links']):
			if link and link.has_attr('href'):
				link_list.append(link)
		# Para no repetir links se crea un set
		return set(link['href'] for link in link_list)
