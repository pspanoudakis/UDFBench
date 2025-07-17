import q12
import datetime
import dateutil.relativedelta

class Q12F(q12.Q12):
    def __calcT__(self):
        return (
            datetime.datetime(2025, 4, 29) -
            dateutil.relativedelta.relativedelta(months=60)
        )
