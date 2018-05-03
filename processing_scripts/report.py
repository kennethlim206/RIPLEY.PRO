import os
import sys
import imp
import commands
from datetime import datetime

def main(t,f):

	# Number of days passed before live reporting is turned off
	live_cutoff = 15

	# Import processing modules
	tools = imp.load_source("tools", "./processing_scripts/burst_tools.py")
	resubmit = imp.load_source("resubmit", "./processing_scripts/resubmit.py")

	# Load task info from reader
	td = tools.task_reader(t)
	cd = tools.function_reader(f)

	chosen = False

	while not chosen:
		if cd["AUTO CALL"] == "":
			chosen = True
		else:
			auto_cd = tools.function_reader("./user_function_constructors/%s" % cd["AUTO CALL"])

			if auto_cd["OUTPUT DIR"] == cd["OUTPUT DIR"]:
				chosen = True
			else:
				print ""
				print " Your selected function has an auto-called function with data stored in a different directory."
				print " Please select one to view:"
				print " %s" % cd["FUNCTION NAME"]
				print " %s" % auto_cd["FUNCTION NAME"]
				print ""

				new_f = raw_input(" >>> ")

				if new_f == auto_cd["FUNCTION NAME"]:
					cd = auto_cd
				elif new_f == cd["FUNCTION NAME"]:
					chosen = True
				else:
					sys.exit(" ERROR: Incorrect input. Please select one of the given functions.")

	LOOKUP_DIR = ""
	if cd["OUTPUT DIR"] == "RAW":
		LOOKUP_DIR = td["RAW DATA DIR"]
	elif cd["OUTPUT DIR"] == "INDEX":
		LOOKUP_DIR = td["INDEX DIR"]

	elif "POST:" in cd["OUTPUT DIR"]:
		LOOKUP_DIR = "%s/%s" % (td["POST DIR"], cd["OUTPUT DIR"].split(":")[1])

	else:
		sys.exit(" ERROR: Incorrect input into <OUTPUT> command in function constructor.")

	# Get job_ids, output and error file summary from RED
	if not os.path.isdir(LOOKUP_DIR):
		sys.exit(" ERROR: The following directory doesn't exist: %s" % LOOKUP_DIR)

	RED = "%s/RED" % LOOKUP_DIR

	if not os.path.isdir(RED):
		sys.exit(" ERROR: You have chosen a directory that does not contain a RED: %s" % LOOKUP_DIR)

	# Base progress report on submission record (anything downstream could have been cancelled)
	OUT = "%s/sbatch_output" % RED
	ERR = "%s/sbatch_error" % RED
	SCR = "%s/sbatch_scripts" % RED
	SUB = "%s/submission_record.txt" % RED

	if not os.path.isfile(SUB):
		sys.exit(" ERROR: You have chosen a directory that does not contain a submission record: %s" % RED)

	# Get job IDs and names
	file = open(SUB, "r")
	ID_LT = dict()

	for line in file:
		line = line.replace("\n", "")

		if line != "" and "\t" in line:
			data = line.split("\t")

			path = data[1]
			name = path.rsplit("/", 1)[1]
			ID = data[2]
			stage = "current"

			# Check for obselete submissions
			if len(data) == 4:
				stage = int(data[3])

			if stage == "current":
				ID_LT[int(ID)] = name

	file.close()

	# STEP 1: LIVE REPORT
	expired = False

	# Get job submission time
	submit_time = commands.getoutput("grep '<SUBMITTED>' %s" % SUB)
	submit_time = submit_time.split("<SUBMITTED> ")[1]
	submit_time = submit_time.replace("\n", "")

	# Get current time
	start = datetime.strptime(submit_time, "%m.%d.%Y %H:%M:%S")
	now = datetime.now().strftime("%m.%d.%Y %H:%M:%S")
	end = datetime.strptime(now, "%m.%d.%Y %H:%M:%S")
	time_passed = end - start
	time_passed = str(time_passed)

	# If a week has passed since job submission, don't display live status
	if "days" in time_passed:
		num_days = int(time_passed.split(" ")[0])

		if num_days >= live_cutoff:
			expired = True

	# Look up table for ID and job suffix
	report_sum = dict()
	report_sum["PENDING"] = 0
	report_sum["RUNNING"] = 0
	report_sum["COMPLETED"] = 0
	report_sum["FAILED"] = 0
	report_sum["CANCELLED"] = 0

	# Display live status
	if not expired:
		print ""
		print " LIVE REPORT:"
		print " ------------------------------------------------------------------------------- "

		for ID, name in sorted(ID_LT.iteritems()):
			report = commands.getoutput("sacct --jobs=%s" % ID)

			for key in report_sum:
				if key in report:
					report_sum[key] += 1
					print " %s (%s): %s" % (key, ID, name)

		print ""
		for state, count in sorted(report_sum.iteritems()):
			print " %d%% %s" % (float(report_sum[state]*100.0/len(ID_LT)), state)
		print ""

	else:
		print " Live record disabled. Over a week has passed since job submission."
		print ""



	# STEP 2: ERROR CHECKER
	if report_sum["PENDING"] == 0 and report_sum["RUNNING"] == 0:
		print " ERROR REPORT:"
		print " ------------------------------------------------------------------------------- "

		# For displaying error files based on size
		ERR_LT = dict()

		for ID, name in sorted(ID_LT.iteritems()):
			error_name = name.replace(".sh", ".err")
			path = "%s/%s" % (ERR, error_name)

			# Make sure job exists and add to appropriate size list
			if not os.path.isfile(path):
				print "%s does not exist. Job most likely failed." % error_name
			else:
				stat = int(commands.getoutput("stat -c%%s %s" % path))
				
				if stat not in ERR_LT:
					ERR_LT[stat] = [error_name]
				else:
					ERR_LT[stat].append(error_name)

		# Print files based on size
		for stat, names in sorted(ERR_LT.iteritems()):
			print " %i bytes:" % stat

			for n in names:
				print " %s" % n

			print ""

		print " Sbatch will output messages into .err files, even if they are not error messages."
		print " Error files of uniform size can normally be trusted, while error files of differing"
		print " sizes should be checked for real errors."
		print ""

		view = ""

		while view != "done":
			print " ------------------------------------------------------------------------------- "
			print " Input error file name as listed above to view its contents,"
			print " or type 'resubmit', if you wish to resubmit a specific sample script,"
			print " or type 'done' to return to the step 2 menu."
			print ""

			view = raw_input(" >>> ")

			# Go to resubmit menu
			if view == "resubmit":

				resubmit_input = ""

				while resubmit_input != "done":

					print " RESUBMIT OPTIONS:"
					print " ------------------------------------------------------------------------------- "

					# Print files based on size
					for stat, names in sorted(ERR_LT.iteritems()):
						print " %i bytes:" % stat

						for n in names:
							print " %s" % n.replace(".err", ".sh")

						print ""

					print " Resubmit by inputting the name of a singular script as it appears above,"
					print " or resubmit by inputting the number of bytes displayed above to resubmit every script of that size,"
					print " or type 'done' to return to the step 2 menu."
					print ""

					resubmit_input = raw_input(" >>> ")

					if resubmit_input != "done":
						resubmit_input_path = "%s/%s" % (SCR, resubmit_input)

						# Error checking for resubmission input
						if not os.path.isfile(resubmit_input_path):
							# Error for non-numeric input
							try:
								int(resubmit_input)
							except ValueError:
								sys.exit(" ERROR: Input file does not exist or input is not numeric.")

							if int(resubmit_input) not in ERR_LT:
								sys.exit(" ERROR: Input byte size is not an option from the above list.")

						# 'Are you sure' check
						print " You are about to resubmit the following script(s):"
						if os.path.isfile(resubmit_input_path):
							print " %s" % resubmit_input
						else:
							for file in ERR_LT[int(resubmit_input)]:
								print " %s" % file.replace(".err", ".sh")

						print ""
						print " Are you sure you would like to submit these scripts? (y/n)"
						print ""

						sure = raw_input(" >>> ")

						if sure == "y":

							# For file inputs
							if os.path.isfile(resubmit_input_path):
								resubmit.main(resubmit_input_path)
							else:
								for file in ERR_LT[int(resubmit_input)]:
									current_script_path = "%s/%s" % (SCR, file.replace(".err", ".sh"))
									resubmit.main(current_script_path)
						else:
							print " Understood. I will not submit your script."
							print ""

				view = "done"

			elif view != "done":

				path = "%s/%s" % (ERR, view)

				if not os.path.isfile(path):
					sys.exit(" ERROR: The name you inputted does not exist.")

				print " From path: %s" % path
				print " ------------------------------------------------------------------------------- "
				print ""
				print commands.getoutput("cat %s" % path)
				print ""

if __name__ == '__main__':
	main()


