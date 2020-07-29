import logging
logging.basicConfig(level=logging.INFO)
# Nos permite manipular archivos de terminal
import subprocess

logger = logging.getLogger('__name__')
news_sites_uids = ['theguardian', 'elmundo']

def main():
	_extract()
	_transform()
	_load()

def _extract():
	logger.info('Starting extract process')
	for news_sites_uid in news_sites_uids:
		subprocess.run(['python', 'main.py', news_sites_uid], cwd='./extract')
		subprocess.run(['find', '.', '-name', '{}*'.format(news_sites_uid),
						'-exec', 'mv', '{}', '../transform/{}_.csv'.format(news_sites_uids), ';'], cwd='./extract')

def _transform():
	logger.info('Starting transform process')
	for news_sites_uid in news_sites_uids:
		dirty_data_filename = '%s_.csv' % news_sites_uids
		clean_data_filename = 'clean_%s' % dirty_data_filename
		subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform')
		subprocess(['rm', dirty_data_filename], cwd='./transform')
		subprocess(['mv', clean_data_filename, '../load/%s.csv' % news_sites_uid], cwd='./transform')

def _load():
	logger.info('Starting load process')
	for news_sites_uid in news_sites_uids:
		clean_data_filename = '%s.csv' % news_sites_uid
		subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
		subprocess.run(['rm', clean_data_filename], cwd='./load')
	

if __name__ == '__main__':
	main()
