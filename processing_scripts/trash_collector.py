import os
import sys
import imp
import commands

def main():
	WORKING_DIR = "./temp/"

	sdout = commands.getoutput("find %s -not -path '*/\.*' -type f -name '*%s*'" % (WORKING_DIR, "queen"))
	files = sdout.split("\n")

	# If there are queen modules in temp folder
	if files[0] != "":
		for f in files:
			prefix = f.split("-", 1)[0]

			if prefix != "trash":
				ID = f.split("-", 1)[1]
				ID = ID.split(".", 1)[0]

				report = commands.getoutput("sacct --jobs=%s" % ID)

				if "RUNNING" not in report and "PENDING" not in report:
					os.popen("rm %s" % f)

if __name__ == '__main__':
	main()


