
# python request_new_timesheet.py > logs/timesheets.log
python request_new_customfield_items.py > logs/customfield_items.log
# python request_new_geo.py > logs/geo.log
python request_new_customfields.py > logs/customfields.log
python request_new_jobcodes.py > logs/jobcodes.log
python request_new_employees.py > logs/employees.log

python upload_employees.py >> logs/employees.log
python upload_jobcodes.py >> logs/jobcodes.log
python upload_customfields.py >> logs/customfields.log
# python upload_geo.py >> logs/geo.log
python upload_customfield_items.py >> logs/customfield_items.py
# python upload_timesheet.py >> logs/timesheets.log
