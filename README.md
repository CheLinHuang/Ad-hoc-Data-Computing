Ad-hoc Data Computing
=================================
The project is designed for UIUC Fall 2017 Course CS411 Database Systems 
Perform SQL queries on CSVs data without loading CSVs into a database.

Prerequisite
-----------
Python 3 or above version should be installed
pip or other kinds of installation tools should be installed 

Installation
-----------
sudo pip install pandas  
sudo pip install feather

Run
-----------
python3 query.py

Syntax
-----------
a) SELECT attribute name should be separated by comma without space.

b) All the strings do not need quotation marks.

After running translate.py, when you see "Type your SQL and press Enter: \n> ",input your query
Here are some test queries to try:

* SELECT R.review_id,R.stars,R.useful FROM review-1m.csv R WHERE R.stars >= 4 AND R.useful > 20
* SELECT review_id,stars,useful FROM review-1m.csv WHERE stars >= 4 AND useful > 20
* SELECT B.name,B.postal_code,R.review_id,R.stars,R.useful FROM business.csv B JOIN review-1m.csv R ON B.business_id = R.business_id WHERE B.city = Champaign AND B.state = IL
* SELECT B.name FROM business.csv B JOIN review-1m.csv R ON B.business_id = R.business_id JOIN photos.csv P ON B.business_id = P.business_id WHERE B.city = Champaign AND B.state = IL AND R.stars = 5 AND P.label = inside
