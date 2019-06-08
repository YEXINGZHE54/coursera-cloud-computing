for i in {1990..2008}; do
	/data/software/hadoop-2.9.2/bin/hadoop dfs -mkdir /user/ec2-user/aviation/airline_ontime/${i}
	for j in {1..12}; do
		cp /data/aviation/airline_ontime/${i}/On_Time_On_Time_Performance_${i}_${j}.zip /data/tmp
		unzip /data/tmp/On_Time_On_Time_Performance_${i}_${j}.zip -d /data/tmp
		python clean.py /data/tmp/On_Time_On_Time_Performance_${i}_${j}.csv /data/tmp/${i}_${j}.csv
		/data/software/hadoop-2.9.2/bin/hadoop dfs -put /data/tmp/${i}_${j}.csv /user/ec2-user/aviation/airline_ontime/
		rm /data/tmp/*
	done
done