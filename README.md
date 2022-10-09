# Events Processing Application (ETL)
## Context

A data processing pipeline to parse the input event files from local and process them base on business requirements and export the output for further analysis and processing.

## Datasets

You have been given a dataset (`appEventProcessingDataset.tar.gz`) of three csv files
containing the following:
1. Client HTTP endpoint polling event data for a set of devices running a web
application.
2. Internet connectivity status logs for the above set of devices, generated when a device
goes offline whilst running the application.
3. Orders data for orders that have been dispatched to devices running the above web
application.


## Requirements

The business team interested in knowing about the connectivity environment of a device in the period of
time surrounding when an order is dispatched to it.

For each order dispatched to a device:
* The total count of all polling events
* The count of each type of polling status_code
* The count of each type of polling error_code and the count of responses without error
codes.

...across the following periods of time:
* Three minutes before the order creation time
* Three minutes after the order creation time
* One hour before the order creation time

In addition to the above, across an unbounded period of time, we would like to know:
1. The time of the polling event immediately preceding, and immediately following the
order creation time.
2. The most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at
what time the order changed to this status. This can be across any period of time
before the order creation time. Not all devices have a connectivity status.



# Solution

## Description
This is a solution using python from reading data from local to processing and applying transformation and storing the final result in a csv formatted output dataset. 

## Getting Started

This section explains how to run this App. I tried to make it very simple. 

### Prerequisites
The required prerequisites are:

* Python 3
* Internet connection to download required libraries.

### Installation

Follow the steps below to run the App.

1. running all containers
   ```sh
   $ pip install -r /path/to/requirements.txt
   ```
   ```sh
   $ python Py-Sol.py
   ```

## Author

👤 **Tayebe Mohamadi**

- Github: [@TayebeMohamadi](https://github.com/TayebeMohamadi)

## Version History
* 0.1
    * Initial Release
