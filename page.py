import lstore.config

class Page:

	def __init__(self):
		self.num_records = 1 #reserve the 0th index for the tps
		self.data = bytearray(lstore.config.PageLength)
		self.dirty = False

	def has_capacity(self):
		return (lstore.config.PageEntries - self.num_records) > 0

	def empty_page(self):
		self.num_records = 1

	#If return value is > -1, successful write and returns index written at. Else, need to allocate new page
	def write(self, value):
		if self.has_capacity():
			self.dirty = True

			if isinstance(value, int):
				valueInBytes = value.to_bytes(8, "big")
			elif isinstance(value, str):
				valueInBytes = (0).to_bytes(8, "big")
				#valueInBytes = str.encode(value)
			else:
				print("WEIRD TYPE VALUE FOUND: " + type(value))
			
			self.data[self.num_records * 8 : (self.num_records + 1) * 8] = valueInBytes
			self.num_records += 1
			return self.num_records - 1

		return -1

	def read(self, index):
		result = int.from_bytes(self.data[index * 8 : (index + 1) * 8], "big")
		return result

	def update_tps(self, value):
		self.dirty = True
		valueInBytes = value.to_bytes(8, "big")
		self.data[0 : 8] = valueInBytes

	def get_tps(self):
		result = int.from_bytes(self.data[0 : 8], "big")
		return result

	def inplace_update(self, index, value):
		self.dirty = True
		if isinstance(value, int):
			valueInBytes = value.to_bytes(8, "big")
		elif isinstance(value, str):
			valueInBytes = str.encode(value)

		self.data[index * 8 : (index + 1) * 8] = valueInBytes