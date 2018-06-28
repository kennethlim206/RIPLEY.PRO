import os
import sys
import commands

# Get files from a path based on a user defined suffix
def get_other(directory, suffix):
	status, sdout = commands.getstatusoutput("find %s -not -path '*/\.*' -type f -name '*%s*'" % (directory, suffix))
	if status == 0:
		return_list = sorted(sdout.split("\n"))

		if return_list[0] == "":
			sys.exit("ERROR: No files with the given suffix: %s found in given directory: %s" % (suffix, directory))
		else:
			return return_list
	else:
		sys.exit("ERROR: %s" % sdout)

# Get .fastq files from a path
def get_FASTQs(directory, depth=0):
	status, sdout = commands.getstatusoutput("find %s -not -path '*/\.*' -type f -name '*.fastq*'" % directory)
	if status == 0:
		return_list = sorted(sdout.split("\n"))

		if return_list[0] == "":
			sys.exit("ERROR: No fastq files found in the given directory: %s" % directory)
		else:

			# Get rid of sanger sequencing
			no_sanger = []
			for item in return_list:
				if item.split(".fastq",1)[1] == "" or item.split(".fastq",1)[1] == ".":
					no_sanger.append(item)

			return_list = no_sanger

			# If order template variable is filled in
			if depth > 0:
				return_list_trimmed = []
				for item in return_list:
					if len(item.split("/")) == depth:
						return_list_trimmed.append(item)
				return_list = return_list_trimmed

			return return_list
	else:
		sys.exit("ERROR: %s" % sdout)

# Get .bam files from a path
def get_BAMs(directory):
	status, sdout = commands.getstatusoutput("find %s -not -path '*/\.*' -type f -name '*.bam'" % directory)
	if status == 0:
		sorted_list = sorted(sdout.split("\n"))
		return_list = []

		for i in sorted_list:
			if ".bai" not in i:
				return_list.append(i)

		if return_list[0] == "":
			sys.exit("ERROR: No bam files found in given directory: %s" % directory)
		else:
			return return_list
	else:
		sys.exit("ERROR: %s" % sdout)

# Populate path with Riply Execution Directory 
def populate(directory, task, function):

	make_d = dict()
	make_d["root directory"] = directory.rsplit("/", 1)[0]
	make_d["directory"] = directory
	make_d["red"] = "%s/RED" % directory
	make_d["output"] = "%s/sbatch_output" % make_d["red"]
	make_d["error"] = "%s/sbatch_error" % make_d["red"]
	make_d["scripts"] = "%s/sbatch_scripts" % make_d["red"]
	make_d["user"] = "%s/user_input" % make_d["red"]
	make_d["archive"] = "%s/archive" % make_d["red"]

	if not os.path.isdir(make_d["root directory"]):
			os.popen("mkdir %s" % make_d["root directory"])
	if not os.path.isdir(make_d["directory"]):
			os.popen("mkdir %s" % make_d["directory"])
	if not os.path.isdir(make_d["red"]):
			os.popen("mkdir %s" % make_d["red"])
			
	for key in make_d:
		if not os.path.isdir(make_d[key]):
			os.popen("mkdir %s" % make_d[key])

	os.popen("cp -r %s %s" % (task, make_d["user"]))
	os.popen("cp -r %s %s" % (function, make_d["user"]))

	return make_d



# Reader for tasks
def task_reader(f):
	file = open(f, "r")
	lines = file.readlines()
	file.close()

	d = dict()
	d["PATH"] = f
	for l in lines:
		l = l.replace("\n", "")

		# Ignore commented out text
		if "#" in l:
			l = l.split("#")[0]

		# Parse <COMMAND> lines and input into dictionary
		if len(l) > 0:
			if l[0] == "<": 
				cmd_name = l[1:].split(">", 1)[0]
				cmd_val = l.split(">", 1)[1]

				if cmd_val[0] == " ":
					cmd_val = cmd_val.replace(" ", "", 1)

				d[cmd_name] = cmd_val
	return d

# Reader for function constructors
def function_reader(f):
	file = open(f, "r")
	lines = file.readlines()
	file.close()

	d = dict()
	d["PATH"] = f
	for i in range(0,len(lines)):
		l = lines[i]
		l = l.replace("\n", "")

		# Ignore commented out text
		if "#" in l:
			l = l.split("#")[0]

		# Parse <COMMAND> lines and input into dictionary
		if len(l) > 0:
			if l[0] == "<": 
				cmd_name = l[1:].split(">", 1)[0]
				cmd_val = l.split(">", 1)[1]

				# Sbatch script command spans multiple lines
				if cmd_name == "SCRIPT COMMAND":
					cmd_val = ""
					cmd_count = i + 1
					cmd_line = lines[cmd_count]

					while "}" not in cmd_line:
						cmd_val += cmd_line
						cmd_count += 1
						cmd_line = lines[cmd_count]

				if cmd_val[0] == " ":
					cmd_val = cmd_val.replace(" ", "", 1)

				d[cmd_name] = cmd_val
	return d

# Reader for special genome table files
def genome_reader(f, ID):

	# Error check for blank ID
	if ID == "":
		sys.exit("ERROR: <REF ID> has not been chosen and is required to run your chosen action.")

	file = open(f, "r")
	lines = file.readlines()
	file.close()

	# Find specific lines under <REF ID>
	choose_lines = []
	for i in range(0,len(lines)):
		if "<REF ID> %s" % ID in lines[i]:
			choose_lines.append(lines[i])
			choose_lines.append(lines[i+1])
			choose_lines.append(lines[i+2])
			choose_lines.append(lines[i+3])
			break

	d = dict()
	for l in choose_lines:
		l = l.replace("\n", "")

		# Ignore commented out text
		if "#" in l:
			l = l.split("#")[0]

		# Parse <COMMAND> lines and input into dictionary
		if len(l) > 0:
			if l[0] == "<": 
				cmd_name = l[1:].split(">", 1)[0]

				cmd_val = l.split(">", 1)[1]
				cmd_val = cmd_val.replace(" ", "")

				d[cmd_name] = cmd_val
	return d
	
