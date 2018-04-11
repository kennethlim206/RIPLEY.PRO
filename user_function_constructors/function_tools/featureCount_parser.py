import os
import sys
import commands

def get_names(directory):
	names_list = os.listdir(directory)
	return_list = []

	for name in names_list:
		# Only take feature counts data
		if name[0] != "." and "summary" not in name and ".txt" in name:
			return_list.append(name)

	return sorted(return_list)

def parse(d):
	# inputted directory with feature counts files
	names_list = get_names(d)

	return_matrix = []

	# find names that are from selected lane
	for name in names_list:
		# Open each feature counts file
		file = open("%s/%s" % (d, name), "r")

		info = file.readline()
		header = file.readline()

		# Each sample file is a column in the matrix
		col = []
		col_names = []

		# For each line in a file:
		for line in file:
			line = line.replace("\n","")
			data = line.split("\t")

			# Put column names into each file
			if len(col_names) == 0:
				col_names.append("gene_id")

			if len(col) == 0:
				sample_name = name.split(".",1)[0]
				col.append(sample_name)

			# Grab gene name and count value from each file
			col_names.append(data[0])
			col.append(data[6])

		# Add gene names to matrix once
		if len(return_matrix) == 0:
			return_matrix.append(col_names)

		return_matrix.append(col)

		file.close()

	return return_matrix

def output(m, d):
	output_file = open("%s/GENE_MATRIX.txt" % d, "w")

	for i in range(0,len(m[0])):
		row_string = ""
		for j in range(0,len(m)):
			row_string += "%s\t" % m[j][i]

		row_string += "\n"
		row_string = row_string.replace("\t\n", "\n")

		output_file.write(row_string)

	output_file.close()

def main():
	FEATURE_DIR = sys.argv[1]
	matrix = parse(FEATURE_DIR)
	output(matrix, FEATURE_DIR)

if __name__ == '__main__':
	main()

