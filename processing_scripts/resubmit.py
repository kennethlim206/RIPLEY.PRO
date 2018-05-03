import os
import sys
import imp
import commands
from datetime import datetime

def main(selected_script_path):

	# Set variable paths based on input script
	RED = selected_script_path.rsplit("/", 2)[0]
	ARC = "%s/archive" % RED
	SUB = "%s/submission_record.txt" % RED

	selected_script = selected_script_path.rsplit("/", 1)[1].replace(".err", ".sh")

	print " Resubmitting: %s" % selected_script

	selected_output_path = selected_script_path.replace("sbatch_scripts", "sbatch_output")
	selected_output_path = selected_output_path.replace(".sh", ".out")
	selected_error_path = selected_script_path.replace("sbatch_scripts", "sbatch_error")
	selected_error_path = selected_error_path.replace(".sh", ".err")

	print " Archiving old error messages..."
	time.sleep(1)

	# Move output and error to archive
	now = datetime.now().strftime("%m.%d.%Y-%H.%M")
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

	# Edit submission record for current/obselete jobs
	submission_record_new = open(SUB.replace("submission_record.txt", "submission_record_new.txt"), "w")
	submission_record = open(SUB, "r")

	# Update submission record
	for entry in submission_record:
		new_entry = entry

		if "<SUBMITTED>" not in new_entry and "\t" in new_entry:
			# Add 'current' annotation, if it doesn't exist already
			if "current" not in new_entry:
				if selected_script in entry:
					new_entry = entry.replace("\n", "old\n")
				else:
					new_entry = entry.replace("\n", "current\n")
			else:
				# If script is resubmitted, annotate old entry with 'old'
				if selected_script in entry:
					new_entry = entry.replace("current", "old")

		submission_record_new.write(new_entry)

	submission_record_new.close()
	submission_record.close()

	# Replace old submission record with new one
	os.popen("mv %s %s" % (SUB.replace("submission_record.txt", "submission_record_new.txt"), SUB))

	# Add new resubmission to submission record.
	submission_record = open(SUB, "a")

	submission_record.write("%s\t%s\t%s\tcurrent\n" % ("1/1", selected_script_path, ID))
	submission_record.write("\n")
	submission_record.close()

	print ""

if __name__ == '__main__':
	main()