origin=("CMI ORD" "IND CMH" "DFW IAH" "LAX SFO" "JFK LAX" "ATL PHX")

for i in "${origin[@]}"
do
	name=($i)
	bin/hadoop jar flight-analysis-1.0-SNAPSHOT.jar bigdata.coursera.flight.RankCarrierByArr /user/ec2-user/aviation/airline_ontime \
	tmp/${name[0]}-${name[1]} ${name[0]} ${name[1]}
	bin/hadoop dfs -get tmp/${name[0]}-${name[1]}/part-r-00000 /data/tmp
	bin/hadoop dfs -rm -r -f tmp/${name[0]}-${name[1]}
	cat /data/tmp/part-r-00000 |awk -F $'\t' '{print $2,$1}'|sort -r -n|head -n 10 > /data/answer/group2/3/${name[0]}-${name[1]}
	rm /data/tmp/part-r-00000
Done