
# DbSynchro with RabbitMQ

- The main of this lab is to create a distributed application that synchronisation databases from the product sales tables. This application needs to use the RabbitMQ to
send data on the related queues. We run 2 distributed processes that synchronise data from first BO to HO and the second BO to the HO.

- We will use Java with JDBC connector for MySQL databases for sales. So, 3 databases are needed for the 2BO and one HO. The synchronisation is made by the 2
BO.

- Product Sales Table columns :

| Date  | Region | Product | Qty | Cost | Amt | Tax | Total
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |

