"""
Demonstrates how to use the blocking scheduler to schedule a job that executes on 3 second
intervals.
"""

from datetime import datetime
import os
import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from jobs.loads import load_events_job


logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.add_job(load_events_job, 'interval', seconds=120)

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass