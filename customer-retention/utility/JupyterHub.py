from pyhive import presto
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import numpy as np

from datetime import datetime, timedelta

import seaborn as sns

conn = presto.connect(
    host= 'bi-presto.serving.data.production.internal', 
    #'presto.processing.yoda.run', 
    port=80,
    username='manoj.kumar@rapido.bike'
)


query = f"""
select *
from
datasets.customer_rf_daily_kpi
where 1=1
and service_name = 'Link'
and city = 'Kolkata'
limit 100
        """
df = pd.read_sql(query,conn)