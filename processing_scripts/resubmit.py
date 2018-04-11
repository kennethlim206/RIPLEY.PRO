import os
import sys
import imp
import commands
from datetime import datetime

def main(t,f):

	# Import processing modules
	tools = imp.load_source("tools", "./processing_scripts/burst_tools.py")

	# Load task info from reader
	td = tools.task_reader(t)
	cd = tools.function_reader(f)

	LOOKUP_DIR = ""
	if cd["OUTPUT DIR"] == "RAW":
		LOOKUP_DIR = td["RAW DATA DIR"]
	elif cd["OUTPUT DIR"] == "INDEX":
		LOOKUP_DIR = td["INDEX DIR"]

	elif "POST:" in cd["OUTPUT DIR"]:
		LOOKUP_DIR = "%s/%s" % (td["POST DIR"], cd["OUTPUT DIR"].split(":")[1])

	else:
		sys.exit(" ERROR: Incorrect input into <OUTPUT DIR> command in function constructor.")

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
	ARC = "%s/archive" % RED

	if not os.path.isfile(SUB):
		sys.exit(" ERROR: You have chosen a directory that does not contain a submission record: %s" % RED)

	sbatch_scripts = tools.get_other(SCR, ".sh")

	print ""
	print " SUBMISSION SCRIPTS:"
	print " ------------------------------------------------------------------------------- "

	for full_path in sbatch_scripts:
		print " %s" % full_path.rsplit("/", 1)[1]

	done = False
	while not done:

		print ""
		print " Note: Resubmission is for individual script failure."
		print " If all your jobs failed, consider using the 'submit' option instead."
		print " The existing sbatch files will be archived and replaced with the resubmitted versions."
		print ""
		print " ------------------------------------------------------------------------------- "
		print " Input the name of the selected script below, or type 'done' to return to Step 2."
		print ""

		selected_script = raw_input(" >>> ")

		if selected_script == "done":
			done = True

		else:
			selected_script_path = "%s/%s" % (SCR, selected_script)
			selected_output_path = "%s/%s" % (OUT, selected_script.replace(".sh", ".out"))
			selected_error_path = "%s/%s" % (ERR, selected_script.replace(".sh", ".err"))

			if not os.path.isfile(selected_script_path):
				sys.exit(" ERROR: You have chosen a script name that does not exist: %s" % selected_script)

			print ""
			print " Resubmitting ..."

			# Move output and error to archive
			now = datetime.now().strftime("%m.%d.%Y-%H:%M:%S")
			archive_time_dir = "%s/%s" % (ARC, now)

			if not os.path.isdir(archive_time_dir):
				os.popen("mkdir %s" % archive_time_dir)

			os.popen("mv %s %s" % (selected_output_path, archive_time_dir))
			os.popen("mv %s %s" % (selected_error_path, archive_time_dir))

			# Copied from burst
			status, ID = commands.getstatusoutput("sbatch %s" % selected_script_path)

			# Analyze result of sbatch submission
			if status == 0:

				ID_split = ID.split(" ")
				ID = int(ID_split[3])
			
			else:
				sys.exit("ERROR:\n%s" % ID)

			# Output job submission statements
			submission_record = open(SUB, "a")

			submission_record.write("%s\t%s\t%s\n" % ("1/1", selected_script_path, ID))
			submission_record.write("\n")
			submission_record.close()

			print ""
			print " Your resubmission job was successfully submitted: %s" % ID
			print ""



if __name__ == '__main__':
	main()


