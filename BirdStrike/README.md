# DATA MODELING BIRD STRIKE DATA

The data is provided by Kaggle.com

I will be modeling the data to provide a more robust analytics model.  Breaking down the csv file into dimensions and facts. 

Brief note on the data.  The data has been collected by the FAA database to keep track of birdstrike involving aircraft.  Weather is commercial and general aviation the FAA created the database to keep track of these events.  

Kaggle.com provided a csv file that contains the data this data is a flat file in which i will model the data into a star schemal using the below tables below.   


### Conceptual Data Model
Dimension Tables: 
1. DimDate
2. DimOperator
3. DimAirport
4. DimSpecies
5. DimPhase 

Fact Table: 
1. FactImpact

Below is a picture of the Conceptual Data Model. 

<img src="img\conceptual_model.PNG" />

Below is the logical data model in progress that shows the releationship between the dim tables and fact table. 

<img src="img\logical_model.PNG" />


