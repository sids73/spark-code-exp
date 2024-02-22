# Data Processing Details for Yellow Taxi Data
1. Cast total amount collected per ride to int and group by vendor in each partition to partially sum the total amount of each vendor (partially because this vendor can also appear in other partitions)
2. Shuffle the data so that it is partitioned by vendor, summing the total ride amount for each vendor and calculating the average total ride amount for each vendor
3. Create another DataFrame that contains the average total ride amount (of all vendors) and join both data frames to add the average amount for each row
4. Calculate compared_to_average_total_amount_by_vendor  as the total ride amount by vendor / average total ride amount across vendors and write again as csv, partitioned by compared_to_average_total_amount_by_vendor