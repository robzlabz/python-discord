#!/usr/bin/env python3
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import sys
import os

python_command = sys.executable
script_to_run = "upwork.py"


# function to invoke the main script
def cron_process():
    print("checking for new notifications...")
    subprocess.Popen([python_command, script_to_run])
    
# initialize the scheduler
scheduler = BlockingScheduler(timezone="asia/jakarta")

# add the job to the scheduler
scheduler.add_job(
    cron_process, "interval", seconds=10
)

# start the scheduler
scheduler.start()

# sudo nano /etc/systemd/system/upworkbot.service

# [Unit]
# Description=Service to run Upwork Bot Notifications
# After=network.target
# [Service]
# User=de
# Group=www-data
# WorkingDirectory=/home/de/upworknotifier
# ExecStart=/home/de/upworknotifier/env/bin/python runner.py
# [Install]
# WantedBy=multi-user.target

# sudo systemctl daemon-reload
# sudo systemctl enable upworkbot
# sudo systemctl start upworkbot
# sudo systemctl status upworkbot