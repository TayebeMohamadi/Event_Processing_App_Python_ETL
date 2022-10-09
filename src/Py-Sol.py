import pandas as pd
import tarfile
import os
from os import listdir
from os.path import  join
from datetime import datetime
import numpy as np
import functools as ft

def main():

    input_zip_file=os.path.abspath("../data/input/appEventProcessingDataset.tar.gz")
    input_main_data_path=os.path.abspath("../data/input")
    input_data_path=os.path.abspath("../data/input/dataset")
    output_data_path=os.path.abspath("../data/output/output.csv")
    data_getter = DataGetter(input_zip_file,input_data_path,input_main_data_path) # creating object from DataGetter class
    data_getter.unzip_data() #unzipping input file
    data_getter.create_datasets() #creating input dataFrames
    query_data=QueryData(data_getter) #creating object from QueryData class

    firstDf= query_data.JoinDfs(howjoin='right', leftDf=query_data.pollingDf, 
                                rightDf=query_data.ordersDF, leftkey='device_id', 
                                rightkey='device_id') # joining orders and polling dataFrames
    secondDf = query_data.JoinDfs(howjoin='right', leftDf=query_data.connectivity_statusDF,
                                  rightDf=query_data.ordersDF, leftkey='device_id', 
                                  rightkey='device_id') # joining orders and connectivity status dataFrames

    tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore=query_data.calculate_tot_cnt_of_polling(firstDf) #returning output DFs for calculate total count of polling
    tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore=query_data.calculate_tot_cnt_of_polling_stat_code(firstDf) #returning output DFs for calculate total count of polling type based on status_code
    tot_cnt_of_poll_stat_code_evnt_without_err_3minbefore,tot_cnt_of_poll_stat_code_evnt_without_err_3minafter,tot_cnt_of_poll_stat_code_evnt_without_err_OneHourBefore=query_data.calculate_tot_cnt_of_polling_error_code(firstDf) #returning output DFs for calculate count of polling error_code and the count of responses without error codes.
    polling_event_creation_time_before_order,polling_event_creation_time_after_order=query_data.calculate_preced_follow_poll_time(firstDf) #returning output DFs for calculate the time of the polling event immediately preceding, and immediately following the order creation time.
    
    DFS = [query_data.ordersDF,
           tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore,
           tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore,
           tot_cnt_of_poll_stat_code_evnt_without_err_3minbefore,tot_cnt_of_poll_stat_code_evnt_without_err_3minafter,tot_cnt_of_poll_stat_code_evnt_without_err_OneHourBefore,
           polling_event_creation_time_before_order,polling_event_creation_time_after_order,
           query_data.calculate_most_recent_cn_stat(secondDf)] # create a list of all created DFs

    df_final = ft.reduce(lambda left, right: pd.merge(left, right,how='left', on='order_id'), DFS) # apply merge join on each output DFs and create a single output dataset.
    df_final=df_final.drop('Unnamed: 0',axis=1) # drop unused column
    df_final['MOST_RECENT_CON_STAT'] = df_final['MOST_RECENT_CON_STAT'].fillna('UNKNOWN') # fill null values for MOST_RECENT_CON_STAT column
    df_final.fillna(value=0, inplace=True) # fill null cells for rest of columns
    df_final.to_csv(output_data_path) # write the output dataFrame to a single CSV output.

class DataGetter:
    """
    This is a class for extracting input files from zipfile and load them into correspondent dataframes .
      
    Attributes:
        input_zip_file (string): Name of input zip file.
        input_data_path (string): directory which input file has been located.
    """

    def __init__(self,input_zip_file :str,input_data_path: str,input_main_data_path :str):
        """
        The constructor for DataGetter class.
  
        Parameters:
           input_zip_file (string): Name of input zip file.
           input_data_path (string): directory which input file has been extracted.
           input_main_data_path (string) : parent directory of input files.
        """
        self.input_zip_file = input_zip_file
        self.input_data_path=input_data_path
        self.input_main_data_path=input_main_data_path
        self.unzip_data()


    def unzip_data(self):
        """
        The function to unzip input file.
        """
        with tarfile.open(self.input_zip_file, "r") as zip_ref:
            zip_ref.extractall(self.input_main_data_path)
            zip_ref.close()


    def get_file(self,input_file : str)-> str:
        """
        The function to return and validate file names that had been extracted before.
  
        Parameters:
            input_file (string): file name.
          
        Returns:
            file : validated file name.
        """
        path=self.input_data_path
        for file in listdir(path):
            if join(path, file)==join(path,input_file):
                return file


    def data_into_df(self,input_file: str, date_col: str)-> pd.DataFrame:
        """
        The function to load csv files into Pandas dataFrames.
  
        Parameters:
            input_file (string): file name.
            date_col (string): column name in a input data set with datetime format.
          
        Returns:
            df (pd.dataFrame): a dataframe that corresponds to each input data files.
        """
        files=self.get_file(input_file)
        df=pd.read_csv(join(self.input_data_path,files), parse_dates=[date_col])
        return df


    def create_datasets(self):
        """
        The function to return dataframe for each of input data files.
            
        Returns:
            ordersDF (pd.dataFrame): a dataframe that corresponds to orders.csv.
            pollingDF (pd.dataFrame): a dataframe that corresponds to polling.csv.
            connectivity_statusDF (pd.dataFrame): a dataframe that corresponds to connectivity_status.csv.
        """
        ordersDF=self.data_into_df('orders.csv', 'order_creation_time')
        pollingDF=self.data_into_df('polling.csv', 'creation_time')
        connectivity_statusDF=self.data_into_df('connectivity_status.csv', 'creation_time')
        return ordersDF,pollingDF,connectivity_statusDF


class QueryData(DataGetter):
    """
    This is a class for querying data based on proposed KPIs .
      
    Attributes:
        dg (object): object of DataGetter class.
    """

    def __init__(self,dg):
        """
        The constructor for QueryData class.
  
        Parameters:
          dg (object): object of DataGetter class.   
        """

        self.ordersDF,self.pollingDf,self.connectivity_statusDF=dg.create_datasets()

    def JoinDfs(self,leftDf:pd.DataFrame,rightDf:pd.DataFrame,leftkey:str,rightkey:str,howjoin:str)-> pd.DataFrame:
        """
        A user-defined function to execute join operation on given dataframes.

        Parameters:
            leftDf (pd.dataFrame): a dataFrame that is indicating a dataset on the left side of the join operation.
            rightDf (pd.dataFrame): a dataFrame that is indicating a dataset on the right side of the join operation.
            leftkey (string): a name of a column as a join key that belongs to a dataset on the left side of the join operation.
            rightkey (string): a name of a column as a join key that belongs to a dataset on the right side of the join operation.
            howjoin (string): a name of a type of join operation.

        Returns:
            (pd.dataFrame): a dataFrame that is a result of a merge operation on two given dataFrames.
        """
        return pd.merge(leftDf,rightDf,how=howjoin,left_on=leftkey,right_on=rightkey)

    def calculate_tot_cnt_of_polling(self,df:pd.DataFrame)  :
        """
        A user-defined function to calculate total count of polling events before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_evnt_3minbefore (pd.dataFrame): a dataFrame that returns total count of polling event for each order 3 minutes before order creation time.
            tot_cnt_of_poll_evnt_3minafter (pd.dataFrame): a dataFrame that returns total count of polling event for each order 3 minutes after order creation time.
            tot_cnt_of_poll_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns total count of polling event for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        tot_cnt_of_poll_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_3minbefore') \
            [['order_id', 'tot_cnt_of_poll_evnt_3minbefore']]
        tot_cnt_of_poll_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_3minafter') \
            [['order_id', 'tot_cnt_of_poll_evnt_3minafter']]
        tot_cnt_of_poll_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_OneHourBefore') \
            [['order_id', 'tot_cnt_of_poll_evnt_OneHourBefore']]
        return tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore


    def calculate_tot_cnt_of_polling_stat_code(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate count of each type of polling status_code before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_stat_code_evnt_3minbefore (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 3 minutes before order creation time.
            tot_cnt_of_poll_stat_code_evnt_3minafter (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 3 minutes after order creation time.
            tot_cnt_of_poll_stat_code_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        tot_cnt_of_poll_stat_code_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minbefore_')
        tot_cnt_of_poll_stat_code_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minafter_')
        tot_cnt_of_poll_stat_code_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_OneHourBefore_')

        return tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore

    def calculate_tot_cnt_of_polling_error_code(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate The count of each type of polling error_code and the count of responses without error
        codes before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_error_code_evnt_3minbefore (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                       codes before and after specific period of time for each order 3 minutes before order creation time.
            tot_cnt_of_poll_error_code_evnt_3minafter (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                      codes before and after specific period of time for each order 3 minutes after order creation time.
            tot_cnt_of_poll_error_code_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                          codes before and after specific period of time for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        df['error_code'] = df['error_code'].fillna('WITHOUT_ERR')
        tot_cnt_of_poll_error_code_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minbefore_')
        tot_cnt_of_poll_error_code_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minafter_')
        tot_cnt_of_poll_error_code_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_OneHourBefore_')
        return tot_cnt_of_poll_error_code_evnt_3minbefore,tot_cnt_of_poll_error_code_evnt_3minafter,tot_cnt_of_poll_error_code_evnt_OneHourBefore

    def calculate_preced_follow_poll_time(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate the time of the polling event immediately preceding, and immediately following the order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            timePollingEventPrecedDF (pd.dataFrame): a dataFrame that returns The time of the polling event immediately preceding the order creation time.
            timePollingEventFollowDF (pd.dataFrame): a dataFrame that returns The time of the polling event immediately following the order creation time.
        """
        
        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        df['rank'] = df[df['diff'] <= 0 ].groupby('order_id')['diff'].rank(ascending=0, method='dense')
        timePollingEventPrecedDF = df[ (df['rank'] == 1)][['order_id', 'creation_time']]\
        .rename(columns={'creation_time': 'polling_event_creation_time_before_order'})

        df['rank'] = df[df['diff'] >= 0 ].groupby('order_id')['diff'].rank(ascending=1, method='dense')
        timePollingEventFollowDF = df[ (df['rank'] == 1)][['order_id', 'creation_time']]\
        .rename(columns={'creation_time': 'polling_event_creation_time_after_order'})

        return timePollingEventPrecedDF,timePollingEventFollowDF

    def calculate_most_recent_cn_stat(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate the most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at
        what time the order changed to this status. 

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            connectivityDF (pd.dataFrame): a dataFrame that returns the most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at what time the order changed to this status.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 's')
        df['rank'] = df[df['diff'] <= 0].groupby('order_id')['diff'].rank(ascending=0, method='first').astype(int)
        connectivityDF = df[(df['diff'] <= 0) & (df['rank'] == 1)][['order_id', 'creation_time', 'status']] \
        .rename(columns={'creation_time': 'TIME_OF_MOST_RECENT_CON_STAT', 'status': 'MOST_RECENT_CON_STAT'})
        return connectivityDF

if __name__ == "__main__":
    main()