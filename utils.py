
from collections.abc import Sequence
import datetime
import pandas as pd
import requests
from tqdm import tqdm
    
    
def api_to_parquet(agencies: Sequence[str], reports: Sequence[str], api_key: str, response_limit=1000) -> None:
    """Function to make API calls to the DAP API and write the data to a parquet file.

    Args:
        agencies (Sequence[str]): List of agencies to pull data for.
        reports (Sequence[str]): List of reports to pull data for.
        api_key (str): API key for the DAP API.
        response_limit (int, optional): Sets a limit on the number of responses. Defaults to 1000.
    """
    start_date = datetime.datetime.strptime('2020-03-01', '%Y-%m-%d')
    end_date = datetime.datetime.strptime('2020-03-31', '%Y-%m-%d')
    increment_days = 30
    
    num_periods = ((datetime.datetime.now() - start_date).days + 1) // increment_days
    
    # Loop through the reports.
    for report in reports:
        new_dataframe = pd.DataFrame()
        # Loop through the agencies.
        for agency in agencies:
            print(f"I'm on {report}/{agency}")
            # Create the URL for the API call.
            url = f"https://api.gsa.gov/analytics/dap/v1.1/agencies/{agency}/reports/{report}/data?api_key={api_key}"
            with tqdm(total=num_periods) as pbar:
                # Loop through the date range.
                while start_date < datetime.datetime.now():
                    
                    # Add the date range parameters to the URL and increment the dates by 1 day.
                    url_date_params = f"&after={start_date.strftime('%Y-%m-%d')}&before={end_date.strftime('%Y-%m-%d')}&limit={response_limit}"
                    full_url = url + url_date_params

                    response = requests.get(full_url).json() # api call
                    response = pd.DataFrame(response) # make the json response a dataframe.

                    # Concatenate the new data to the existing dataframe.
                    new_dataframe = pd.concat([new_dataframe, response])
                    
                    # Increment the dates by the specified number of days.
                    start_date += datetime.timedelta(days=increment_days)
                    end_date += datetime.timedelta(days=increment_days)
                    
                    # Update progress bar.
                    pbar.update(1)
            start_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
            end_date = datetime.datetime.strptime('2020-03-31', '%Y-%m-%d')
            
            print(full_url)            
        new_dataframe['date'] = pd.to_datetime(new_dataframe['date'])

        # write to parquet file to the data folder.
        new_dataframe.to_parquet(f"./data/2{report}_report_all_agencies.parquet")
        
def remove_outliers(s: pd.Series) -> pd.Series:
    
    Q1 = s.quantile(0.25)
    Q3 = s.quantile(0.75)
    
    # Calculate the interquartile range
    IQR = Q3 - Q1

    # Filter out the outliers
    no_outliers = s[(s >= (Q1 - 1.5 * IQR)) & (s <= (Q3 + 1.5 * IQR))]
    
    return no_outliers