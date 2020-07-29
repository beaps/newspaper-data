import argparse
import logging
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import hashlib

import pandas as pd

logger = logging.getLogger(__name__)


def main(filename):
	logger.info('Starting cleaning process')
	# Leer los datos y crear un DataFrame
	df = _read_data(filename)
	# Añadir newspaper_uid
	newspaper_uid = _extract_newspaper_uid(filename)
	# Añadir el uid a la columna
	df = _add_newspaper_uid_column(df, newspaper_uid)
	# Extraer el host
	df = _extrat_host(df)
	# Rellenar los títulos que faltan
	df = _fill_missing_titles(df)
	# Generar uids para cada una de las filas
	df = _generate_uids_for_rows(df)
	# Eliminar saltos de línea
	df = _remove_new_lines_from_body(df)
	return df

def _read_data(filename):
	logger.info('Reading file %s' % filename)
	return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
	logger.info('Extracting newspaper uid')
	# theguardian_2020_07_28_articles.csv -> theguardian
	newspaper_uid = filename.split('_')[0]
	logger.info('Newspaper uid detected: %s' % newspaper_uid)
	return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
	logger.info('Filling newspaper_uid column with %s' % newspaper_uid)
	df['newspaper_uid'] = newspaper_uid
	return df

def _extrat_host(df):
	logger.info('Extracting host from urls')
	df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
	return df

def _fill_missing_titles(df):
	logger.info('Filling missing titles')
	# Generar máscara booleana
	missing_titles_mask = df['title'].isna()
	missing_titles = (
		df[missing_titles_mask]['url']
			.str.extract(r'(?P<missing_titles>[^/]+)$')
			.applymap(lambda title: title.split('-'))
			.applymap(lambda title_word_list: ' '.join(title_word_list))
		)
	# Asignar los títulos a una columna
	df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
	return df

def _generate_uids_for_rows(df):
	logger.info('Generating uids for each row')
	uids = (
		df
		.apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
		.apply(lambda hash_object: hash_object.hexdigest()) # Convertir el uid a una representación hexadecimal
	)
	# Añadir la columna de uids
	df['uid'] = uids
	return df.set_index('uid')

def _remove_new_lines_from_body(df):
	logger.info('Remove new lines from body')
	stripped_body = df.apply(lambda row: row['body'].replace('\n', ''), axis=1)
	# Reemplazamos el body con el nuevo body sin los saltos de línea
	df['body'] = stripped_body
	return df

if __name__ == '__main__':
	# Preguntarle al usuario con que archivo quiere trabajar
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'filename',
		help='The path to the dirty data',
		type=str)
	# Parsear los argumentos
	args = parser.parse_args()

	df = main(args.filename)
	print(df)