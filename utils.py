import numpy as np


def convert_temperature_bytes(temperature_bytes: bytes) -> np.ndarray:
	arr = np.frombuffer(temperature_bytes, dtype=np.int16)
	return np.round(arr / 64 - 274, 1)
