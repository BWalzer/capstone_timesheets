

# python request_new_timesheet.py 1>../logs/timesheets.log 2>>../logs/timesheets.log
python request_new_customfield_items.py 1>../logs/customfield_items.log  2>>../logs/customfield_items.log
python request_new_geo.py 1>../logs/geo.log 2>>../logs/geo.log
python request_new_customfields.py 1>../logs/customfields.log 2>>../logs/customfields.log
python request_new_jobcodes.py 1>../logs/jobcodes.log 2>>../logs/jobcodes.log
python request_new_employees.py 1>../logs/employees.log 2>>../logs/employees.log

python upload_employees.py 1>>../logs/employees.log 2>>../logs/employees.log
python upload_jobcodes.py 1>>../logs/jobcodes.log 2>>../logs/jobcodes.log
python upload_customfields.py 1>> ../logs/customfields.log 2>> ../logs/customfields.log
python upload_geo.py 1>>../logs/geo.log 2>>../logs/geo.log
python upload_customfield_items.py 1>>../logs/customfield_items.log 2>>../logs/customfield_items.log
# python upload_timesheet.py 1>>../logs/timesheets.log 2>>../logs/timesheets.log
