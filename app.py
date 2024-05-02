from pathlib import Path

project_root_dir_path = Path(__file__).parent


class App:
	__conf = {
        "database_host": "DESKTOP-89EAOQ1",
        "database_user": "sa",
        "database_password": "ap",
        "database_name": "M16_DTS4"
    }
	__setters = []

	@staticmethod
	def get(name):
		if App.__conf is None:
			App.load_configuration_from_json()
		return App.__conf[name]

	@staticmethod
	def set(name, value):
		if name in App.__setters:
			App.__conf[name] = value
		else:
			raise NameError("Name not accepted in set() method")

	@staticmethod
	def is_key_exist_and_true(d, key):
		if d.get(key) is None:
			return False
		return d.get(key) is True

	@staticmethod
	def load_configuration_from_json():
		import json
		with open(f'{project_root_dir_path}/application_config.json', 'r') as file:
			App.__conf = json.load(file)
