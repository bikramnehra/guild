import array

hex_string = "deadbeef"
hex_data = hex_string.decode("hex")
print hex_data
print bytearray(hex_data)
