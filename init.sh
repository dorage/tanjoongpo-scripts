# install jq
sudo yum install jq

# install crontab
sudo yum install cronie

# install duckdb	
curl https://install.duckdb.org | sh
echo "export PATH='/home/ec2-user/.duckdb/cli/latest':$PATH" >> ~/.bashrc
source ~/.bashrc
