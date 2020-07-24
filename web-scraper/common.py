import yaml

# Cachear nuestra configuración para no leer a disco cada vez
# que queramos utilizar nuestra configuración
__config = None

def config():
	# Acceso a la variable global dentro de la función
	global __config
	if not __config:
		with open('config.yaml', mode='r') as f:
			__config = yaml.load(f, Loader=yaml.FullLoader)
	return __config
