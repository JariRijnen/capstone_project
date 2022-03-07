## project capstone Wildfires

## data
- wildfires data (gewoon alles van gebruiken, 1 file) rom (https://www.kaggle.com/rtatman/188-million-us-wildfires)
- weather data (eigenlijk 1001 files, eerst samen lezen, dan de domme kolommen eruit halen) from (https://www.kaggle.com/cavfiumella/us-weather-daily-summaries-1991-2016)

- (momenteel in de oefen fase doe ik met de kant en klare weather data werken)

## preprocessen
-US weather steps:
check missing values columns:
-remove all columns with only missing values
-remove all columns with over 75% missing values
- check welke stations leeg zijn of niet (GEEN ENKELE)
- aggegraten op state niveau?? is uiteraard niet perfect (vermelden) maar geeft wel een benadering
  --- (alternatief zou zijn zoek het dichtste station, maar dan heb je missing value problemen)

- wildfires:
geen columns met 100% missing values
wel met veel, maar dat zijn misschien dat belangrijkste of grootste wildfires

geen duplicate rows

missing data, duplicate data, data types, etc. checken 

## define schema

- us weather stations = split per state, city, etc.