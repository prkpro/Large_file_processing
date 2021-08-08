# Large_file_processing
This is assignment for Postman

STEPS : 

1. cd to the directory where requirements.txt is located
2. activate your virtualenv
3. run: "pip install -r requirements.txt"
4. run: python main.py

Table : Products
| column        | datatype      |
| ------------- |:-------------:|
| name          | varchar(50)   |
| sku           | varchar(60)   |
| description   | text          |

```
Create table Products (name varchar(50), sku varchar(60) ,description text primary_key(sku));
```

Table : Agg
| column         | datatype      |
| -------------  |:-------------:|
| name           | varchar(50)   |
| no. of products| int           |

```
Create table Agg as Select name,count(*) as 'no. of products' from Products group by name;
```

Points to Achieve :
1.Your code should follow concept of OOPS (tick)
2.Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.(tick)
3.Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it)(tick)
4.All product details are to be ingested into a single table.(tick)
5.An aggregated table on above rows with `name` and `no. of products` as the columns.(tick)

|name|sku|description|
|:--:|:-:|:---------:|
|Bryce Jones|lay-raise-best-end|Art community floor adult your single type. Per back community former stock thing.|
|John Robinson|cup-return-guess|Produce successful hot tree past action young song. Himself then tax eye little last state vote. Country down list that speech economy leave.|
|Theresa Taylor|step-onto|"Choice should lead budget task. Author best mention.Often stuff professional today allow after door instead. Model seat fear evidence. Now sing opportunity feeling no season show."|
|Roger Huerta|citizen-some-middle|Important fight wrong position fine. Friend song interview glass pay. Organization possible just.|
|John Buckley|term-important|"Alone maybe education risk despite way.|Want benefit manage financial story term must. Former wife activity firm example later. Black win rest ask."|
|Tiffany Johnson|do-many-avoid|"Born tree wind.Boy marriage begin value. Record health laugh ask under notice federal. Hard lose need sell treatment.Certain throw executive front late. Because truth risk."|
|Roy Golden DDS|help-return-art|"Pm daughter thousand.Process eat employee have they example past.Increase author water. Magazine child mention start."|
|David Wright|listen-enough-check|"Under its near. Necessary education game everybody.Hospital upon suffer year discussion south government nothing. Knowledge race population exist against must wear level. Coach girl situation."|
|Anthony Burch|anyone-executive|I lose positive manage reason option. Crime structure space both traditional teacher that.|
|Lauren Smith|grow-we-decide-job|Smile yet fear society theory help. Rather thing language skill since heart across wait. According ask them government or.|

|name|no. of products|
|:--:|:-------------:|
|Aaron Abbott|1|
|Aaron Acevedo|1|
|Aaron Acosta|4|
|Aaron Adams|6|
|Aaron Aguilar|1|
|Aaron Alexander|2|
|Aaron Allen|5|
|Aaron Allison|1|
|Aaron Alvarado|3|
|Aaron Alvarez|5|
