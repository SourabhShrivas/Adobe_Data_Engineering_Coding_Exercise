# Adobe_Data_Engineering_Coding_Exercise
-------------------------------------------------------------------

aws s3 cp lambda.zip s3://s3-adobe-repository/

aws s3 cp src/app.py s3://s3-adobe-repository/etl/app.py

Rough - Create infrastructure with coludformation stack 
aws cloudformation deploy --template-file deployment_template.yml --stack-name infrastructure --capabilities CAPABILITY_NAMED_IAM

