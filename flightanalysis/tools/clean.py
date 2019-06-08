import sys
import csv

def main(input, output):
	fieldnames = ['AirlineID', 'UniqueCarrier', 'Origin', 'Dest', 'DayOfWeek', 'DepDelay', 'ArrDelay', 'FlightDate', 'DepTime', 'ArrTime']
	with open(output, 'w') as outfile:
		writer = csv.DictWriter(outfile, fieldnames=fieldnames)
		writer.writeheader()
		with open(input) as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				if float(row['Cancelled']) > 0.8:
					continue
				writer.writerow({k : row.get(k, '') for k in fieldnames})
	pass

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print 'Usage python %s filepath outpath' % (sys.argv[0])
	else:
		main(sys.argv[1], sys.argv[2])