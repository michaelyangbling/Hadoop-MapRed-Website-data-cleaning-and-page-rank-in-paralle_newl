#seperate control
#e.x, in order to build jar file, : make -f make.mk build 
project="/Users/yzh/IdeaProjects/mapreduce get time series by secondary sort"
jar="/Users/yzh/IdeaProjects/mapreduce get time series by secondary sort/target/wc-1.0.jar"

hadoop="/usr/local/hadoop-2.7.5"
input="/Users/yzh/Desktop/njtest/input"
output="/Users/yzh/Desktop/njtest/output"








awsoutput="s3://michaelyangcs/workFold/output10"
localout="/Users/yzh/Desktop/cour/parallel/RankOutput"

.PHONY:build
build:
	cd ${project};  mvn clean install;

.PHONY:standalone
standalone: 
	cd ${hadoop}; rm -rf ${output}; bin/hadoop jar ${jar} sort2nd ${input} ${output} "";


.PHONY:awsrun
awsrun:
    
#	aws s3 rm ${awsoutput} --recursive
	aws emr create-cluster \
    --name "pageRank" \
    --release-label emr-5.11.1 \
    --instance-groups '[{"InstanceCount":10,"InstanceGroupType":"CORE","InstanceType":"m4.large"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m4.large"}]' \
    --applications Name=Hadoop \
    --steps '[{"Args":["Main","s3://michaelyangcs/workFold"," ","8"],"Type":"CUSTOM_JAR","Jar":"s3://michaelyangcs/wc-1.0.jar","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
--log-uri s3://michaelyangcs/log2 \
--service-role EMR_DefaultRole \
--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-520b7f0f \
--region us-east-1 \
--enable-debugging \
--auto-terminate

.PHONY:sync
sync:
	aws s3 sync ${awsoutput} ${localout}
	

.PHONY:move
move:
	aws s3 mv s3://michaelyangcs/input s3://michaelyangcs/workFold/input --recursive

