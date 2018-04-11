import os
import sys

def main():
	INPUT_FILE = open(sys.argv[1], "r")
	SINGLE_CELL = open(sys.argv[2], "r")
	OUTPUT_FILE = open(sys.argv[3], "w")

	for line in INPUT_FILE:
		OUTPUT_FILE.write(line)

	for line in SINGLE_CELL:
		OUTPUT_FILE.write(line)

	INPUT_FILE.close()
	SINGLE_CELL.close()
	OUTPUT_FILE.close()

if __name__ == '__main__':
	main()