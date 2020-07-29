import argparse
import logging
logging.basicConfig(level=logging.INFO)

import pandas as pd

from article import Article
from base import Base, engine, Session

logger = logging.getLogger(__name__)

def main(filename):
	# Generar nuestro schema en la base de datos
	Base.metadata.create_all(engine)
	# Inicializar la sesión
	session = Session()
	# Leer los articulos con pandas
	articles = pd.read_csv(filename)
	# Iterar por todos los artículos que tenemos en los archivos limpios
	# iterrows -> generar un bucle dentro de cada una de las filas
	for index, row in articles.iterrows():
		logger.info('Loading article uid %s into DB' % row['uid'])
		# Generar artículo
		article = Article(row['uid'],
						  row['body'],
						  row['title'],
						  row['host'],
						  row['newspaper_uid'],
						  row['n_tokens_body'],
						  row['n_tokens_title'],
						  row['url'])
		# Insertar artículo en la base de datos
		session.add(article)

		session.commit()
		session.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'filename',
		help='The file you want to load into the db',
		type=str
	)

	args = parser.parse_args()

	main(args.filename)