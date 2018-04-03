source /home/ubuntu/.bashrc

echo "#######" $(date) "#######" >> ../logs/database.log

echo "@@@ downloading timesheet logs @@@" >> ../logs/database.log
python ../database_management/request_new_timesheet_logs.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading timesheets @@@" >> ../logs/database.log
python ../database_management/request_new_timesheet.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading customfield items @@@" >> ../logs/database.log
python ../database_management/request_new_customfield_items.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading geo locations @@@" >> ../logs/database.log
python ../database_management/request_new_geolocs.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading customfields @@@" >> ../logs/database.log
python ../database_management/request_new_customfields.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading jobcodes @@@" >> ../logs/database.log
python ../database_management/request_new_jobcodes.py 1>>database.log 2>>../logs/database.log

echo "@@@ downloading employees @@@" >> ../logs/database.log
python ../database_management/request_new_employees.py 1>>database.log 2>>../logs/database.log


echo "@@@ uploading employees @@@" >> ../logs/database.log
python ../database_management/upload_employees.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading jobcodes @@@" >> ../logs/database.log
python ../database_management/upload_jobcodes.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading customfields @@@" >> ../logs/database.log
python ../database_management/upload_customfields.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading geo locations @@@" >> ../logs/database.log
python ../database_management/upload_geolocs.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading customfield items @@@" >> ../logs/database.log
python ../database_management/upload_customfield_items.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading timesheets @@@" >> ../logs/database.log
python ../database_management/upload_timesheet.py 1>>database.log 2>>../logs/database.log

echo "@@@ uploading timesheet logs @@@" >> ../logs/database.log
python ../database_management/upload_timesheet_log.py 1>>database.log 2>>../logs/database.log
