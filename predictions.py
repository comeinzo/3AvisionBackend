from collections import OrderedDict
from datetime import datetime

def extract_yearly_predictions(prediction_data):
    yearly_predictions = OrderedDict()

    for item in prediction_data:
        year = datetime.strptime(item['category'], "%Y-%m-%d").year
        # Keep only the first occurrence of each year
        if year not in yearly_predictions:
            yearly_predictions[year] = item

    # Convert OrderedDict to list
    return list(yearly_predictions.values())





import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import bar_chart as bc

def load_and_predict(xAxis, yAxis, number_of_periods, timePeriod):
    # Load data
    data = bc.global_df.copy()
    x_axis = xAxis[0]
    y_axis = yAxis[0]

    # Convert to datetime and clean
    data[x_axis] = pd.to_datetime(data[x_axis], errors='coerce')
    data = data.dropna(subset=[x_axis])
    data[y_axis] = pd.to_numeric(data[y_axis], errors='coerce')
    data = data.dropna(subset=[y_axis])

    # Set index
    data.set_index(x_axis, inplace=True)

    # If predicting by year, convert years to months
    if timePeriod == "years":
        freq = 'M'  # Resample monthly
        seasonal_order = (1, 1, 1, 12)  # Annual seasonality
        number_of_months = int(number_of_periods) * 12
        resampled_data = data.resample(freq).sum(numeric_only=True).reset_index()
        prediction_horizon = number_of_months
    elif timePeriod == "months":
        freq = 'M'
        seasonal_order = (1, 1, 1, 12)
        resampled_data = data.resample(freq).sum(numeric_only=True).reset_index()
        prediction_horizon = int(number_of_periods)
    elif timePeriod == "days":
        freq = 'D'
        seasonal_order = (1, 1, 1, 7)
        resampled_data = data.resample(freq).sum(numeric_only=True).reset_index()
        prediction_horizon = int(number_of_periods)
    else:
        raise ValueError(f"Invalid time period: {timePeriod}")

    # Check y-axis column exists
    if y_axis not in resampled_data.columns:
        raise ValueError(f"Column '{y_axis}' missing after resampling.")

    # Time Series Setup
    ts = resampled_data.set_index(x_axis)[y_axis]

    # SARIMA Model
    model = SARIMAX(ts, order=(1, 1, 1), seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    results = model.fit(disp=False)

    # Forecasting
    forecast_obj = results.get_forecast(steps=prediction_horizon)
    forecast_values = forecast_obj.predicted_mean

    # Prepare future dates based on resampling frequency
    last_date = resampled_data[x_axis].max()
    if freq == 'M':
        future_dates = [last_date + pd.DateOffset(months=i) for i in range(1, prediction_horizon + 1)]
    elif freq == 'D':
        future_dates = [last_date + pd.DateOffset(days=i) for i in range(1, prediction_horizon + 1)]

    # Build prediction response
    prediction_data = []
   
    for date, value in zip(future_dates, forecast_values):
        prediction_data.append({
            'category': date.date().isoformat(),
            'value': round(value, 2)
        })

    print("prediction_data is :", prediction_data)  
    return prediction_data



