origin=("CMI" "BWI" "MIA" "LAX" "IAH" "SFO")

for name in ${origin[@]}
do
	bin/hadoop jar flight-analysis-1.0-SNAPSHOT.jar bigdata.coursera.flight.RankCarrierByDep  /user/ec2-user/aviation/airline_ontime  tmp/${name} ${name}
	bin/hadoop dfs -get tmp/${name}/part-r-00000 /data/tmp
	cat /data/tmp/part-r-00000 |awk -F $'\t' '{print $2,$1}'|sort -r -n|head -n 10 > /data/answer/group2/1/${name}
	rm /data/tmp/part-r-00000
done