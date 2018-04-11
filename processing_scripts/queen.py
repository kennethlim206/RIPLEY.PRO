import os
import sys
import imp
import commands
import time
import datetime

# Figures out the order for function requests
def main():

	# If true, will not submit sbatch and return pseudo data instead
	test = False

	task_path = sys.argv[1]
	function_string = sys.argv[2]

	print " ------------------------------------------------------------------------------- "
	print " Submitting your BURST modules: "
	print ""

	function_list = function_string.split("+")

	
	# Interpret functions
	submitter_dependency = ""
	ID_num = 1738
	
	for i in range(0,len(function_list)):
		function_path = function_list[i]

		cmd = "srun --time=00:10:00 ./processing_scripts/burst_submitter.sh %s %s" % (task_path, function_path)

		print " Submitting function %i/%i - %s" % (i+1, len(function_list), function_path.rsplit("/", 1)[1])
		print " %s" % cmd

		status = 0
		ID_all = "%i:%i" % (ID_num, ID_num+1)
		ID_num += 2

		# Non test case
		if not test:
			status, ID_all = commands.getstatusoutput(cmd)

		# Read sbatch output
		if status != 0:
			sys.exit(" ERROR:\n%s" % ID_all)

		# If there is more than 1 function submitted
		if len(function_list) > 1:
			status = "PENDING"
			
			# While at least 1 job is pending
			while status == "PENDING":
				time.sleep(300)
				print "Check status: %s\n" % datetime.datetime.now().strftime("%m.%d.%Y %H:%M:%S")
				
				ID_list = ID_all.split(":")

				report_sum = dict()
				report_sum["PENDING"] = 0
				report_sum["RUNNING"] = 0
				report_sum["COMPLETED"] = 0
				report_sum["FAILED"] = 0
				report_sum["CANCELLED"] = 0

				for ID in ID_list:

					# Get status report from sbatch
					report = commands.getoutput("sacct --jobs=%s" % ID)

					for key in report_sum:
						if key in report:
							report_sum[key] += 1
							print "%s: %s" % (key, ID)

				print ""

				if report_sum["PENDING"] == 0 and report_sum["RUNNING"] == 0:
					status = "DONE"

			if report_sum["COMPLETED"] == 0:
				sys.exit("ERROR: Queen has found all jobs cancelled or failed. Halting downstream commands.")



if __name__ == '__main__':
	main()

