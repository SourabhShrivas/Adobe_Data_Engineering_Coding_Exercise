# Adobe_Data_Engineering_Coding_Exercise
-------------------------------------------------------------------
git clone repo 

aws s3 cp lambda.zip s3://s3-adobe-repository/

aws s3 cp src/app.py s3://s3-adobe-repository/etl/app.py

aws cloudformation deploy --template-file deployment_template.yml --stack-name infrastructure --capabilities CAPABILITY_NAMED_IAM

aws s3 cp /Users/soura/Downloads/data.tsv s3://inputs3bucket-adobe/


aws s3 rm s3://inputs3bucket-adobe --recursive

aws s3 rm s3://outputs3bucket-adobe --recursive

Deleting the stack
aws cloudformation delete-stack --stack-name infrastructure

