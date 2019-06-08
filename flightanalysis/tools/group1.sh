bin/hadoop jar flight-analysis-1.0-SNAPSHOT.jar bigdata.coursera.flight.CountByAirport  /user/ec2-user/aviation/airline_ontime  tmp/3
bin/hadoop dfs -get tmp/3/part-r-00000 data/answer/result/airport-visits
cat /data/answer/result/airport-visits |awk -F $'\t' '{print $2,$1}'|sort -n -r|head -n 10 |awk '{print $2}' > /data/answer/group1/1.result
bin/hadoop jar flight-analysis-1.0-SNAPSHOT.jar bigdata.coursera.flight.RankAirlineByArrival  /user/ec2-user/aviation/airline_ontime  tmp/5
bin/hadoop dfs -get tmp/5/part-r-00000 /data/tmp
cat /data/tmp/part-r-00000 |awk -F $'\t' '{print $2,$1}'|sort -n -r|head -n 10 > /data/answer/group1/2.result