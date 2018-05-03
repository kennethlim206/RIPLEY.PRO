import time
import os
import sys
import imp
import commands
import datetime

def main():
	
	# Get rid of queen modules that have finished running
	trash = imp.load_source("trash", "./processing_scripts/trash_collector.py")
	trash.main()

	# Queue wait time
	queue_time = 24
	# If true, will not submit sbatch and return pseudo data instead
	test = False

	# Load progress report module
	report = imp.load_source("report", "./processing_scripts/report.py")

	os.popen("chmod +x ./processing_scripts/*.sh")

	print ""
	print "*********************************************************************************  "
	print "*                        __       __         __                                 *  "
	print "*   ________________    |  |  |  |  |  |    |    |   |    ________________|\\    * "
	print "*  |                    |__|  |  |__|  |    |__  |___|                      \\   * "
	print "*  |________________    |\\    |  |     |    |      |      ________________  /   * "
	print "*                       | \\   |  |     |__  |__    |                      |/    * "
	print "*                                                                               *  "
	print "*********************************************************************************  "
	print "         - THE NORDLAB RNA-SEQ INTERACTIVE PIPELINE EXPERIMENT YIELDER -           "
	print "\n\n"

	# Keeps user in the interface, until they choose to exit
	task_exit = False
	function_exit = False

	while not task_exit:
		task_exit = False
		function_exit = False

		# Input task from user
		print " ------------------------------------------------------------------------------- "
		print "|            STEP 1: PLEASE SELECT THE ID OF YOUR TASK FILE OR EXIT             |"
		print " ------------------------------------------------------------------------------- "
		print ""

		# Display all task options
		input_files = os.listdir("./user_tasks")
		input_list = []
		for file in input_files:
			if file[0] != "." and file != "blank_task.txt":
				input_list.append(file)
				
		input_list.sort()
		print " ID   Task Sheet"
		print " ------------------------------------------------------------------------------- "
		for i in range(0, len(input_list)):
			print " %i = %s" % (i, input_list[i].replace(".txt", ""))

		print ""
		print " exit"
		print " ------------------------------------------------------------------------------- "
		print ""

		task_input = raw_input(" >>> ")

		# Exit command
		if task_input == "exit":
			task_exit = True
			break

		# Error for non-numeric input
		try:
			int(task_input)
		except ValueError:
			sys.exit(" ERROR: Input is not ID Number.")

		task_input = int(task_input)

		# Error for numeric input not in range
		if task_input >= len(input_list):
			sys.exit(" ERROR: Input is not listed.")

		task_input_path = "./user_tasks/%s" % input_list[task_input]

		# Error for non-existing user input
		if not os.path.isfile(task_input_path):
			sys.exit(" ERROR: Inputted task name does not exist.")
		else:
			print " You selected the input file: %s" % input_list[task_input].replace(".txt", "")

		while not function_exit:

			# Input function from user
			print "\n\n"
			print " ------------------------------------------------------------------------------- "
			print "|                  STEP 2: PLEASE SELECT A FUNCTION(S) OR EXIT                  |"
			print " ------------------------------------------------------------------------------- "
			print " Note: Users can perform functions sequentially with the '->' symbol."
			print " Example: get_slim -> align -> feature"
			print ""

			print " Function     Description"
			print " ------------------------------------------------------------------------------- "
			# Display all functions from function table
			function_files = open("./user_function_constructors/paths.txt", "r")
			function_options = dict()
			for line in function_files:
				if "#" not in line:
					line = line.replace("\n", "")
					data = line.split("\t")
					name = data[0]
					input_name = data[1]
					description = data[2]

					print " %s = %s" % (input_name, description)

					function_options[input_name] = name

			function_files.close()

			print ""
			print " back"
			print " exit"
			print " ------------------------------------------------------------------------------- "
			print ""

			function_input = raw_input(" >>> ")

			# Exit commands
			if function_input == "exit":
				task_exit = True
				function_exit = True
				break

			if function_input == "back":
				function_exit = True
				break

			# Parse and load function info
			function_input = function_input.replace(" ",  "")
			function_names_list = function_input.split("->")

			function_path_list = []

			# Add time together from all jobs
			time_list = []

			print " You have chosen the following function(s):"

			for name in function_names_list:

				# Error for non-existing user input
				if name not in function_options:
					sys.exit(" ERROR: Inputted function name does not exist: %s" % name)

				function_input_path = "./user_function_constructors/%s" % function_options[name]

				# Error for non-existing user input
				if not os.path.isfile(function_input_path):
					sys.exit(" ERROR: Incorrect function path. Please check the input path in ./user_function_constructors/paths.txt")

				function_path_list.append(function_input_path)

				print " %s" % name

				# Get TIME function from function constructors
				time_call = commands.getoutput("grep '<TIME>' %s" % function_input_path)
				time_call = time_call.split("<TIME>")[1]
				time_call = time_call.replace(" ", "")
				time_call = time_call.replace("\n", "")
				time_list.append(time_call)

				# Get AUTO CALLED function from function constructors
				auto_call = commands.getoutput("grep '<AUTO CALL>' %s" % function_input_path)
				auto_call = auto_call.split("<AUTO CALL>")[1]
				auto_call = auto_call.replace(" ", "")
				auto_call = auto_call.replace("\n", "")

				if auto_call != "":
					auto_path = "./user_function_constructors/%s" % auto_call

					# Error for non-existing autocall input
					if not os.path.isfile(auto_path):
						sys.exit(" ERROR: Incorrect <AUTO CALL> file name. Please check function constructor: %s" % path.rsplit("/", 1)[1])

					# Get AUTO CALLED function from function constructors
					time_call = commands.getoutput("grep '<TIME>' %s" % auto_path)
					time_call = time_call.split("<TIME>")[1]
					time_call = time_call.replace(" ", "")
					time_call = time_call.replace("\n", "")
					time_list.append(time_call)

					function_path_list.append(auto_path)

					print " + AUTO CALLED: %s" % auto_call

			

			# Input function from user
			print "\n\n"
			print " ------------------------------------------------------------------------------- "
			print "|                    STEP 3: PLEASE CHOOSE AN ACTION OR EXIT                    |"
			print " ------------------------------------------------------------------------------- "
			print " Note: If you selected multiple functions using the '->' symbol, the only action"
			print " you may perform is 'submit'."
			print ""

			print " Action     Description"
			print " ------------------------------------------------------------------------------- "
			print " submit = submit your chosen function/string of functions to the sbatch queue"

			if "->" not in function_input:
				print " report = check the progress of your chosen function and resubmit, if failure."
			
			print ""
			print " back"
			print " exit"
			print " ------------------------------------------------------------------------------- "
			print ""

			action_input = raw_input(" >>> ")

			if action_input == "exit":
				function_exit = True
				task_exit = True
				break

			if action_input != "back":

				# Go into report progress
				if action_input == "report" and "->" not in function_input:
					report.main(task_input_path, "./user_function_constructors/%s" % function_options[function_input])
				
				elif action_input == "submit":
					
					# Generate string input to give to queen
					function_string = ""
					for path in function_path_list:
						function_string += "%s+" % path

					print ""
					print " ------------------------------------------------------------------------------- "
					print " Constructing your module ..."
					print ""

					# Add time together
					total_secs = 0
					for tm in time_list:
						time_parts = [int(s) for s in tm.split(':')]
						total_secs += (time_parts[0] * 60 + time_parts[1]) * 60 + time_parts[2]
					total_secs, sec = divmod(total_secs, 60)
					hr, m = divmod(total_secs, 60)
					time_display = "%d:%02d:%02d" % (hr, m, sec)
					t = "%d:%02d:%02d" % (hr+queue_time, m, sec)

					# Sumbit QUEEN module
					cmd = "sbatch --time=%s ./processing_scripts/queen_submitter.sh %s %s" % (t, task_input_path, function_string[:-1])
					
					status = 0
					ID = "Submitted job as 1738"

					if not test:
						status, ID = commands.getstatusoutput(cmd)

					if status == 0:
						ID_split = ID.split(" ")
						ID = int(ID_split[3])
						print " Your queen module has been submitted: %i" % ID
						print " Estimated total run time: %s (+ time in queue)" % time_display
					else:
						sys.exit(" ERROR:\n%s" % ID)

				else:
					sys.exit(" ERROR: Chosen action does not exist.")

			print ""

			sys.stdout.write(" Returning to step 2 ")
			sys.stdout.flush()

			for i in range(0,3):
				sys.stdout.write(".")
				sys.stdout.flush()
				time.sleep(1)

			print ""

		if function_input == "exit":
			task_exit = True

	print " Bye for now!"

if __name__ == '__main__':
	main()

