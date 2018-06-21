import os
import sys
import commands
import imp
from datetime import datetime

def main():

	# If true, will not submit sbatch and return pseudo data instead
	test = False
	test_num = 1738

	WORKING_DIR = os.getcwd()

	# Import processing modules
	tools = imp.load_source("tools", "%s/processing_scripts/burst_tools.py" % WORKING_DIR)

	# Load task info from reader
	td = tools.task_reader(sys.argv[1])
	cd = tools.function_reader(sys.argv[2])
	gd = tools.genome_reader("%s/genome_tables/%s" % (WORKING_DIR, td["REF TABLE"]), td["REF ID"])

	td["WORKING DIR"] = WORKING_DIR



	##########################
	###   INPUT SETTINGS   ###
	##########################

	cd["INPUT FILES FULL"] = []
	cd["INPUT FILES TRIMMED"] = []

	# PART 1: Parse URL input separately from other types.
	if cd["INPUT DIR"] == "ONLINE":
		if cd["INPUT TYPE"] == "URL":
			cd["INPUT FILES FULL"].append(td["FTP COMMAND"])
			cd["INPUT FILES TRIMMED"].append("ALL")

		elif cd["INPUT TYPE"] == "SRR":
			cd["INPUT FILES FULL"] = td["SRR IDs"].split(",")
			cd["INPUT FILES TRIMMED"] = td["SRR IDs"].split(",")

		else:
			sys.exit("ERROR: <INPUT TYPE> variable in function constructor, %s must be URL or SRR when <INPUT DIR> is ONLINE.")

	elif cd["INPUT DIR"] == "GENOME":
		if cd["INPUT TYPE"] == "NONE":
			cd["INPUT FILES FULL"].append("INDEX")
			cd["INPUT FILES TRIMMED"].append("INDEX")
		else:
			sys.exit("ERROR: <INPUT TYPE> variable in function constructor, %s must be NONE when <INPUT DIR> is GENOME.")

	else:

		# PART 2: Convert <INPUT DIR> to specified directory
		if cd["INPUT DIR"] == "RAW":
			cd["INPUT DIR"] = td["RAW DATA DIR"]
		elif "POST:" in cd["INPUT DIR"]:
			# Find correct input directory
			POST_NAME = cd["INPUT DIR"].split("POST:")[1]
			INPUT_DIR = "%s/%s" % (td["POST DIR"], POST_NAME)
			cd["INPUT DIR"] = INPUT_DIR
		else:
			sys.exit("ERROR: Incorrect input into <INPUT DIR> command in function constructor.")

		# Check if input directory is actual directory
		if not os.path.isdir(cd["INPUT DIR"]):
			sys.exit("ERROR: task sheet variable associated with <INPUT DIR> does not exist: %s" % cd["INPUT DIR"])

		# PART 3: Retrieve INPUT DIR files based on suffix
		if cd["INPUT TYPE"] == "FASTQ":

			if "ORDER TEMPLATE" in td:
				if td["ORDER TEMPLATE"] != "":

					order = td["ORDER TEMPLATE"].split(",", 1)[0]
					order_split = order.split("/")
					depth = len(order_split)

					if depth == 0:
						sys.exit(" ERROR: at least 1 '/' character must be in <ORDER TEMPLATE> variable.")

					cd["INPUT FILES FULL"] = tools.get_FASTQs(cd["INPUT DIR"], depth)
				else:
					cd["INPUT FILES FULL"] = tools.get_FASTQs(cd["INPUT DIR"])
			else:
				cd["INPUT FILES FULL"] = tools.get_FASTQs(cd["INPUT DIR"])

		elif cd["INPUT TYPE"] == "BAM":
			cd["INPUT FILES FULL"] = tools.get_BAMs(cd["INPUT DIR"])
		elif "OTHER:" in cd["INPUT TYPE"]:
			SUFFIX = cd["INPUT TYPE"]
			SUFFIX = SUFFIX.split(":")[1]
			cd["INPUT FILES FULL"] = tools.get_other(cd["INPUT DIR"], SUFFIX)
		elif cd["INPUT TYPE"] == "NONE":
			cd["INPUT FILES FULL"] = [cd["FUNCTION NAME"]]
		elif cd["INPUT TYPE"] == "URL" or cd["INPUT TYPE"] == "SRR":
			sys.exit("ERROR: <INPUT TYPE> variable in function constructor, %s is only allowed when <INPUT DIR> is ONLINE.")
		else:
			sys.exit("ERROR: Incorrect input into <INPUT TYPE> command in function constructor.")

		# PART 4: If each input file is to be analyzed individually, further parsing is required
		if cd["INPUT MULT"] == "SINGLE":

			# For ALIGN jobs with PE reads, split the sample names in half
			PE_LT = dict()
			FW_STRAND = []
			if cd["FUNCTION NAME"] == "ALIGN" and td["SINGLE PAIR"] == "PE" and "<REVERSE STRAND>" in cd["SCRIPT COMMAND"]:
				for i in range(1, len(cd["INPUT FILES FULL"]), 2):
					FW_path = cd["INPUT FILES FULL"][i-1]
					RV_path = cd["INPUT FILES FULL"][i]

					PE_LT[FW_path] = RV_path
					FW_STRAND.append(FW_path)

				cd["INPUT FILES FULL"] = FW_STRAND

			# PART 5: Trim full input paths
			for file in cd["INPUT FILES FULL"]:
				if cd["INPUT TYPE"] != "NONE":
					trimmed_file = file.rsplit("/", 1)[1]
					cd["INPUT FILES TRIMMED"].append(trimmed_file)
				else:
					cd["INPUT FILES TRIMMED"] = ["ALL"]

			# For ALIGN jobs, replace specified suffix with _SE or _PE <- except don't do this!
			# Yay! Hopefully this will commit now! Woohoo!
			if cd["FUNCTION NAME"] == "ALIGN":
				for i in range(0,len(cd["INPUT FILES TRIMMED"])):
					# 
					if td["FASTQ SUFFIX"] not in cd["INPUT FILES TRIMMED"][i]:
						sys.exit("ERROR: Please adjust <FASTQ SUFFIX> in task sheet. Suffix not found in file name: %s" % cd["INPUT FILES TRIMMED"][i])
					if td["SINGLE PAIR"] == "SE":
						cd["INPUT FILES TRIMMED"][i] = cd["INPUT FILES TRIMMED"][i].replace(td["FASTQ SUFFIX"], "")
					elif td["SINGLE PAIR"] == "PE":
						cd["INPUT FILES TRIMMED"][i] = cd["INPUT FILES TRIMMED"][i].replace(td["FASTQ SUFFIX"], "")
					else:
						sys.exit("ERROR: Incorrect input into <PAIRED SUFFIX> task variable.")

		# Combine all input files into single string with user inputted separator
		elif "ALL:" in cd["INPUT MULT"]:
			if "<INPUT FILES TRIMMED>" in cd["SCRIPT COMMAND"]:
				sys.exit("ERROR: <INPUT FILES TRIMMED> variable in function constructor script command not permitted when <INPUT MULT> = MULT.")

			separater = cd["INPUT MULT"].split(":", 1)[1]
			input_string = ""

			for file in cd["INPUT FILES FULL"]:
				input_string += "%s%s" % (file, separater)

			cd["INPUT FILES FULL"] = [input_string[:-1]]
			cd["INPUT FILES TRIMMED"] = ["ALL"]

		else:
			sys.exit("ERROR: Incorrect input into <INPUT MULT> variable in function constructor: %s" % cd["INPUT MULT"])



	######################
	###   SET OUTPUT   ###
	######################

	# OUTPUT PART 1: Convert OUTPUT DIR type to real directory
	if cd["OUTPUT DIR"] == "RAW":
		cd["OUTPUT DIR"] = td["RAW DATA DIR"]
	elif cd["OUTPUT DIR"] == "INDEX":
		cd["OUTPUT DIR"] = td["INDEX DIR"]
	elif "POST:" in cd["OUTPUT DIR"]:
		NAME = cd["OUTPUT DIR"].split(":")[1]
		cd["OUTPUT DIR"] = "%s/%s" % (td["POST DIR"], NAME)
	else:
		sys.exit("ERROR: Incorrect input into <OUTPUT> command in function constructor.")

	# OUTPUT PART 2: Create and populate OUTPUT DIR
	record = tools.populate(cd["OUTPUT DIR"], td["PATH"], cd["PATH"])



	###########################
	###   REF FA AND ANNO   ###
	###########################

	# Decide whether to use customized ref genome and annotation (currently no customizations for BED files)
	if td["CUSTOMIZE"] == "T":
		td["USE FA"] = "%s/custom_genome/%s" % (td["INDEX DIR"], td["CUSTOM FA"])
		td["USE ANNO"] = "%s/custom_genome/%s" % (td["INDEX DIR"], td["CUSTOM ANNO"])
	elif td["CUSTOMIZE"] == "F":
		td["USE FA"] = gd["REF FA"]
		td["USE ANNO"] = gd["REF ANNO"]

	td["USE BED"] = gd["REF BED"]



	#######################
	###   ZIPPED DATA   ###
	#######################

	if td["ZIPPED"] == ".gz":
		td["ZIPPED"] = "--readFilesCommand gunzip -c"
	elif td["ZIPPED"] == ".bzip2":
		td["ZIPPED"] = "--readFilesCommand bunzip2 -c"
	elif td["ZIPPED"] == "":
		td["ZIPPED"] = ""
	else:
		sys.exit("ERROR: Incorrect input into <ZIPPED> task variable.")



	#############################################
	###   GET AMPLICON DATA (IF APPLICABLE)   ###
	#############################################

	# Error check, if AMP variables are called in function constructor, amp file must exist in task sheet
	if ("AMP FILE" not in td) and ("<AMP NAME>" in cd["SCRIPT COMMAND"] or "<AMP CHROM>" in cd["SCRIPT COMMAND"] or "<AMP START>" in cd["SCRIPT COMMAND"] or "<AMP END>" in cd["SCRIPT COMMAND"]):
		sys.exit("ERROR: No <AMP FILE> variable found in task sheet.")

	if ("AMP FILE" in td) and ("<AMP NAME>" in cd["SCRIPT COMMAND"] or "<AMP CHROM>" in cd["SCRIPT COMMAND"] or "<AMP START>" in cd["SCRIPT COMMAND"] or "<AMP END>" in cd["SCRIPT COMMAND"]):
		file = open("./amp_beds/%s" % td["AMP FILE"], "r")

		cd["AMP INFO"] = dict()

		for line in file:
			line = line.replace("\n", "")

			data = line.split("\t")
			chrom = data[0]
			start = data[1]
			end = data[2]
			name = data[3]

			entry = [chrom, start, end]
			cd["AMP INFO"][name] = entry

		file.close()

	# for amplicon in cd["AMP INFO"]:
	# 	print "%s = %s:%s-%s" % (amplicon, cd["AMP INFO"][amplicon][0], cd["AMP INFO"][amplicon][1], cd["AMP INFO"][amplicon][2])
		


	################################
	###   SUBSTITUTE VARIABLES   ###
	################################

	submit_paths = []

	# Add task variables to script
	cmd = cd["SCRIPT COMMAND"]
	for var in td:
		if "<%s>" % var in cmd:
			cmd = cmd.replace("<%s>" % var, td[var])

	# Add function variables to script
	safe_list = ["INPUT FILES FULL", "INPUT FILES TRIMMED", "SRR SPLIT"]
	for var in cd:
		if var not in safe_list:
			if "<%s>" % var in cmd:
				cmd = cmd.replace("<%s>" % var, cd[var])

	for i in range(0, len(cd["INPUT FILES FULL"])):
		cmd_ind = cmd
		input_full_ind = cd["INPUT FILES FULL"][i]
		input_trim_ind = cd["INPUT FILES TRIMMED"][i]

		cmd_ind = cmd_ind.replace("<INPUT FILES FULL>", input_full_ind)
		cmd_ind = cmd_ind.replace("<INPUT FILES TRIMMED>", input_trim_ind)

		# For ALIGN jobs only
		if cd["FUNCTION NAME"] == "ALIGN" and td["SINGLE PAIR"] == "PE" and "<REVERSE STRAND>" in cd["SCRIPT COMMAND"]:
			cmd_ind = cmd_ind.replace("<REVERSE STRAND>", PE_LT[input_full_ind])
		else:
			cmd_ind = cmd_ind.replace("<REVERSE STRAND>", "")

		# For DOWNLOAD_SRR jobs only
		if cd["FUNCTION NAME"] == "DOWNLOAD_SRR" and td["SINGLE PAIR"] == "PE":
			cmd_ind = cmd_ind.replace("<SRR SPLIT>", "--split-files")
		else:
			cmd_ind = cmd_ind.replace("<SRR SPLIT>", "")

		# Final step turn double empty spaces into single empty spaces to account for non-existent variables
		cmd_ind = cmd_ind.replace("  ", " ")

		# Add jobs for each sample file
		if "AMP INFO" not in cd:
			script_ind_path = "%s/%s_%s.sh" % (record["scripts"], cd["FUNCTION NAME"], input_trim_ind)

			# Place in RED of OUTPUT DIR
			script_ind = open(script_ind_path, "w")

			script_ind.write("#!/bin/bash -l\n\n")

			# Required for sbatch script
			script_ind.write("#SBATCH --job-name=%s\n" % cd["FUNCTION NAME"])
			script_ind.write("#SBATCH --time=%s\n" % cd["TIME"])
			script_ind.write("#SBATCH --output=%s/%s_%s.out\n" % (record["output"], cd["FUNCTION NAME"], input_trim_ind))
			script_ind.write("#SBATCH --error=%s/%s_%s.err\n" % (record["error"], cd["FUNCTION NAME"], input_trim_ind))
			script_ind.write("#SBATCH --workdir=%s\n" % record["directory"])

			# Optional for sbatch script
			if cd["PARTITION"] != "default":
				script_ind.write("#SBATCH --partition=%s\n" % cd["PARTITION"])

			if cd["CORES"] != "default":
				script_ind.write("#SBATCH --cpus-per-task=%s\n" % cd["CORES"])

			if cd["MEM PER CPU"] != "default":
				script_ind.write("#SBATCH --mem-per-cpu=%s\n" % cd["MEM PER CPU"])

			script_ind.write("")
			script_ind.write(cmd_ind)
			script_ind.write("")

			script_ind.close()

			submit_paths.append(script_ind_path)

		# Add jobs for each sample + each amplicon
		else:
			for amplicon in cd["AMP INFO"]:

				cmd_ind_amp = cmd_ind

				cmd_ind_amp = cmd_ind_amp.replace("<AMP NAME>", amplicon)
				cmd_ind_amp = cmd_ind_amp.replace("<AMP CHROM>", cd["AMP INFO"][amplicon][0])
				cmd_ind_amp = cmd_ind_amp.replace("<AMP START>", cd["AMP INFO"][amplicon][1])
				cmd_ind_amp = cmd_ind_amp.replace("<AMP END>", cd["AMP INFO"][amplicon][2])

				input_trim_ind_amp = "%s_%s" % (input_trim_ind, amplicon)

				script_ind_path = "%s/%s_%s.sh" % (record["scripts"], cd["FUNCTION NAME"], input_trim_ind_amp)

				# Place in RED of OUTPUT DIR
				script_ind = open(script_ind_path, "w")

				script_ind.write("#!/bin/bash\n\n")

				# Required for sbatch script
				script_ind.write("#SBATCH --job-name=%s\n" % cd["FUNCTION NAME"])
				script_ind.write("#SBATCH --time=%s\n" % cd["TIME"])
				script_ind.write("#SBATCH --output=%s/%s_%s.out\n" % (record["output"], cd["FUNCTION NAME"], input_trim_ind_amp))
				script_ind.write("#SBATCH --error=%s/%s_%s.err\n" % (record["error"], cd["FUNCTION NAME"], input_trim_ind_amp))
				script_ind.write("#SBATCH --workdir=%s\n" % record["directory"])

				# Optional for sbatch script
				if cd["PARTITION"] != "default":
					script_ind.write("#SBATCH --partition=%s\n" % cd["PARTITION"])

				if cd["CORES"] != "default":
					script_ind.write("#SBATCH --cpus-per-task=%s\n" % cd["CORES"])

				if cd["MEM PER CPU"] != "default":
					script_ind.write("#SBATCH --mem-per-cpu=%s\n" % cd["MEM PER CPU"])

				script_ind.write("")
				script_ind.write(cmd_ind_amp)
				script_ind.write("")

				script_ind.close()

				submit_paths.append(script_ind_path)



	#########################
	###   SUBMIT SCRIPT   ###
	#########################

	return_string = ""
	submission_record = open("%s/submission_record.txt" % record["red"], "a")
	submit_time = commands.getoutput("grep '<SUBMITTED>' %s/submission_record.txt" % record["red"])
	
	if submit_time == "":
		submission_record.write("<SUBMITTED> %s\n\n" % datetime.now().strftime("%m.%d.%Y %H:%M:%S"))

	for i in range (0, len(submit_paths)):
		sp = submit_paths[i]

		pre_submit_print = "%i/%i" % (i+1, len(submit_paths))
		submit_cmd = "sbatch %s" % sp
		
		status = 0
		ID = "Submitting job as %i" % test_num
		test_num += 1

		if not test:
			status, ID = commands.getstatusoutput(submit_cmd)

		# Analyze result of sbatch submission
		if status == 0:

			ID_split = ID.split(" ")
			ID = int(ID_split[3])
			
		else:
			sys.exit("ERROR:\n%s" % ID)

		# Output job submission statements
		submission_record.write("%s\t%s\t%s\tcurrent\n" % (pre_submit_print, sp, ID))

		return_string += "%s:" % ID

	submission_record.write("\n")
	submission_record.close()
	
	print return_string[:-1]

if __name__ == '__main__':
	main()
