## Shutterfly Customer Lifetime Value

One way to analyze acquisition strategy and estimate marketing cost is to calculate the Lifetime Value (“LTV”) of a customer. Simply speaking, LTV is the projected revenue that customer will generate during their lifetime.

A simple LTV can be calculated using the following equation: `52(a) x t`. Where `a` is the average customer value per week (customer expenditures per visit (USD) x number of site visits per week) and `t` is the average customer lifespan. The average lifespan for Shutterfly is 10 years.  

## Code Environmnet

Python 3 environment used and basic date time,json used in this program

## Running the Program

When the program is invoked first it will ingest the data from "events.txt" file and asks user to input a Top value he intrested in.
Before running the program please update the direcotry location of "events.txt" file in the [src code](https://github.com/srikanth4075/shdwhfly/blob/master/src/Shfly_challenge.py) file at line number 219.
Also,make sure you have read access to that "events.txt" file.

Sample run of the program see below:
```
MacBook-Pro:src srmc$ python shfly.py
Enter a Number to get top list: 5  <-- user inputs his top Value 5 of his choice
Customer_key|Last_name
96f55c7d8f47|James
96f55c7d8f44|Jimmy
96f55c7d8f43|Ray
96f55c7d8f46|Lucy
96f55c7d8f40|Sam
```

 

### Ingest(e, D)
Given event e, update data D
Ingest function reads data from "events.txt" file and loads into a data store.



### TopXSimpleLTVCustomers(x, D)
Return the top x customers with the highest Simple Lifetime Value from data D. 

If you have further questions you can email <mailto:srikanth4075@gmail.com>.


