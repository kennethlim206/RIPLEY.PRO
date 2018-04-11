import os
import sys
import commands

# Get files from a path based on a user defined suffix
def get_other(directory, suffix):
	status, sdout = commands.getstatusoutput("find %s -not -path '*/\.*' -type f -name '*%s*'" % (directory, suffix))
	if status == 0:
		return_list = sorted(sdout.split("\n"))

		if return_list[0] == "":
			sys.exit("ERROR: No files with the given suffix found in given directory: %s" % directory)
		else:
			return return_list
	else:
		sys.exit("ERROR: %s" % sdout)

# Returns dictionary of nucleotide count pairs
def match_vals(nucs, vals):
	d = {"A":"0", "C":"0", "G":"0", "T":"0", "N":"0", "<*>":"0"}
	for i in range(0,len(nucs)):
		n = nucs[i]
		v = vals[i]
		if n == "A":
			d["A"] = v
		elif n == "C":
			d["C"] = v
		elif n == "G":
			d["G"] = v
		elif n == "T":
			d["T"] = v
		elif n == "N":
			d["N"] = v
		elif n == "<*>":
			d["<*>"] = v
	return d

def get_nuc(d, n):
	if n == d["A"]:
		return "A"
	elif n == d["C"]:
		return "C"
	elif n == d["G"]:
		return "G"
	elif n == d["N"]:
		return "N"
	elif n == d["T"]:
		return "T"
	elif n ==d["<*>"]:
		return "<*>"

def parse(input_path):
	headers = commands.getoutput("grep '#CHROM' %s" % input_path)
	BAMs = headers.split("\t")[9:]

	counter = 0
	for b in BAMs:
		input_file = open(input_path, "r")
		amp_name = input_path.rsplit("/", 1)[1]

		bam_name = b.rsplit("/", 1)[1]
		bam_name_trimmed = bam_name.replace(".Aligned.sortedByCoord.out.bam", "")
		output_name = "%s_%s" % (bam_name_trimmed, amp_name)

		output_path = input_path.replace("/MP_RAW/", "/MP_PARSED/")
		output_path = output_path.replace(amp_name, output_name)

		output_file = open(output_path, "w")
		output_file.write("CHROM\tPOS\tREF\tALT\tREF_VAL\tALT_VAL\tA\tC\tG\tT\tN\tTOTAL\tDP\n")

		for line in input_file:
			line = line.replace("\n", "")
			if "#" not in line:
				data = line.split("\t")

				# Sometimes INDELs are lines
				USE_LINE = data[7].split(";")[0]

				if "DP" in USE_LINE:

					CHROM = str(data[0])
					POS = str(data[1])
					REF = str(data[3])
					DP = USE_LINE.split("=")[1]

					NUCS = data[4].split(",")
					ALT = NUCS[0]
					NUCS.insert(0, REF)
					VALS = data[9+counter].split(":")[1].split(",")

					D = match_vals(NUCS, VALS)

					A = D["A"]
					C = D["C"]
					G = D["G"]
					T = D["T"]
					N = D["N"]
					STAR = D["<*>"]

					TOTAL = int(A) + int(C) + int(G) + int(T) + int(N)

					REF_VAL = D[REF]
					ALT_VAL = D[ALT]

					OUT_VALS = (CHROM, POS, REF, ALT, REF_VAL, ALT_VAL, A, C, G, T, N, TOTAL, DP)
					OUT_STR = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % OUT_VALS

					output_file.write(OUT_STR)
				
		counter += 1
		output_file.close()
		input_file.close()

def main():

	files = get_other(sys.argv[1], ".txt")
	# files = get_other("./MP_RAW", ".txt")

	for f in files:
		parse(f)

if __name__ == "__main__":
    main()