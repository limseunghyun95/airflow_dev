from datetime import date, timedelta
import time

from numpy import random
import pandas as pd
from faker import Faker


def _generate_events(end_date):
    """Generates a fake dataset with events for 30 days before end date."""

    events = pd.concat(
        [
            _generate_events_for_day(date=end_date - timedelta(days=(30 - i)))
            for i in range(30)
        ],
        axis=0,
    )

    return events


def _generate_events_for_day(date):
    """Generates events for a given day."""

    # Use date as seed.
    seed = int(time.mktime(date.timetuple()))

    Faker.seed(seed)
    random_state = random.RandomState(seed)

    # Determine how many users and how many events we will have.
    n_users = random_state.randint(low=50, high=100)
    n_events = random_state.randint(low=200, high=2000)

    # Generate a bunch of users.
    fake = Faker()
    users = [fake.ipv4() for _ in range(n_users)]

    return pd.DataFrame(
        {
            "user": random_state.choice(users, size=n_events, replace=True),
            "date": pd.to_datetime(date),
        }
    )


def get_events(start_date=None, end_date=None, output_file='/opt/airflow/tmp/events.json'):
    """Get events within a specific date range."""
    print(f'start_date: {start_date} / end_date: {end_date}')

    if end_date:
        year, month, day = list(map(int, end_date.split('-')))
    else:
        year, month, day = 2024, 12, 31
    events = _generate_events(end_date=date(year=year, month=month, day=day))

    if start_date is not None:
        events = events.loc[events["date"] >= start_date]

    if end_date is not None:
        events = events.loc[events["date"] < end_date]

    events.to_json(output_file,
                   orient="records",
                   date_format="iso")
