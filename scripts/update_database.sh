source /users/ubuntu/.bashrc

echo "#######" $(date) "#######" >> database.log

echo "@@@ downloading timesheet logs @@@" >> database.log
python request_new_timesheet_logs.py 1>>databse.log 2>>database.log

echo "@@@ downloading timesheets @@@" >> database.log
python request_new_timesheet.py 1>>databse.log 2>>database.log

echo "@@@ downloading customfield items @@@" >> database.log
python request_new_customfield_items.py 1>>databse.log 2>>database.log

echo "@@@ downloading geo locations @@@" >> database.log
python request_new_geolocs.py 1>>databse.log 2>>database.log

echo "@@@ downloading customfields @@@" >> database.log
python request_new_customfields.py 1>>databse.log 2>>database.log

echo "@@@ downloading jobcodes @@@" >> database.log
python request_new_jobcodes.py 1>>databse.log 2>>database.log

echo "@@@ downloading employees @@@" >> database.log
python request_new_employees.py 1>>databse.log 2>>database.log


echo "@@@ uploading employees @@@" >> database.log
python upload_employees.py 1>>databse.log 2>>database.log

echo "@@@ uploading jobcodes @@@" >> database.log
python upload_jobcodes.py 1>>databse.log 2>>database.log

echo "@@@ uploading customfields @@@" >> database.log
python upload_customfields.py 1>>databse.log 2>>database.log

echo "@@@ uploading geo locations @@@" >> database.log
python upload_geolocs.py 1>>databse.log 2>>database.log

echo "@@@ uploading customfield items @@@" >> database.log
python upload_customfield_items.py 1>>databse.log 2>>database.log

echo "@@@ uploading timesheets @@@" >> database.log
python upload_timesheet.py 1>>databse.log 2>>database.log

echo "@@@ uploading timesheet logs @@@" >> database.log
python upload_timesheet_log.py 1>>databse.log 2>>database.log
