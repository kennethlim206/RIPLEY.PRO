import os
import sys
import commands

# Get .fastq files from a path
def get_FASTQs(directory):
	status, sdout = commands.getstatusoutput("find %s -not -path '*/\.*' -type f -name '*.fastq*'" % directory)
	if status == 0:
		return_list = sorted(sdout.split("\n"))

		if return_list[0] == "":
			sys.exit("ERROR: No fastq files found in the given directory: %s" % directory)
		else:
			return return_list
	else:
		sys.exit("ERROR: %s" % sdout)

def check(order, template, path_list):

	order_split = order.split("/")
	for path in path_list:
		path_split = path.split("/")

		if len(path_split) != len(order_split):
			sys.exit("ERROR: your inputted file order does not match the actual fastq path: %s, %s" % (order, path))

# For fucking idiotic use cases where the fastq file hierarchy makes 0 fucking sense
def parse_paths(order, path_list):
	order_split = order.split("/")
	depth = len(order_split)

	return_list = []

	for path in path_list:
		path_split = path.split("/")

		if depth == len(path_split):
			return_list.append(path)

	return return_list

def rename(order, template, path_list):
	# order = /A/B/C/D/E/F/G
	# template = F_G
	# path = /Users/kennethlim/Desktop/TEST_DIR/raw_data_concat

	order_kvp = dict()
	
	order_split = order.split("/")
	for current_path in path_list:

		current_template = template
		path_split = current_path.split("/")

		# Sort components into dictionary
		for i in range(0, len(path_split)):
			order_kvp[order_split[i]] = path_split[i]

		# Create new name
		for k in order_kvp:
			if k in current_template:
				current_template = current_template.replace(k, order_kvp[k])

		# Replace old name with new name
		new_path = "%s/%s" % (current_path.rsplit("/", 1)[0], current_template)
		os.popen("mv %s %s" % (current_path, new_path))

def main():

	RAW_DATA_DIR = sys.argv[1]
	RAW_PATHS = get_FASTQs(RAW_DATA_DIR)

	INPUT_STR = sys.argv[2]
	ORDER = INPUT_STR.split(",", 1)[0]
	TEMPLATE = INPUT_STR.split(",", 1)[1]

	# check(ORDER, TEMPLATE, RAW_PATHS)
	RAW_PATHS_PARSED = parse_paths(ORDER, RAW_PATHS)
	rename(ORDER, TEMPLATE, RAW_PATHS_PARSED)

if __name__ == '__main__':
	main()




