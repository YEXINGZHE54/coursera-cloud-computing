origin=("CMI ORD LAX 04/03/2008 1" "JAX DFW CRP 09/09/2008 2" "SLC BFL LAX 01/04/2008 3" "LAX SFO PHX 12/07/2008 4" "DFW ORD DFW 10/06/2008 5" "LAX ORD JFK 01/01/2008 6")

for i in "${origin[@]}"
do
	name=($i)
	bin/hadoop jar flight-analysis-1.0-SNAPSHOT.jar bigdata.coursera.flight.FindFlightsForXYZ /user/ec2-user/aviation/airline_ontime/2008 \
	tmp/${name[4]} ${name[0]} ${name[1]} ${name[2]} ${name[3]}
	bin/hadoop dfs -get tmp/${name[4]}/part-r-00000 /data/tmp
	bin/hadoop dfs -rm -r -f tmp/${name[4]}
	mv /data/tmp/part-r-00000 /data/answer/group3/${name[4]}
done