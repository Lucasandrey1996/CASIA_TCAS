# Clustering rules GESA
Le clustering des SST CAD de Gruyère Energie SA est basé sur le fichier d'export de la base de donnée GIS.
Le fichier se trouve dans le dossier du projet sous : [Point_transmission_CAD_YYYYMMDD.xlsx](../2_Program/0_Data/0_Raw/RefFiles/Point_transmission_CAD_20260202.xlsx)

## SST non remontées (SST2.0) (U_NO_EGID abscent du DF LYNX)
 - cluster 1 : La SST consomme moin de 100kW (U_PUISSANCE < 100kW)
 - cluster 2 : La SST consomme plus de 100kW (U_PUISSANCE >= 100kW)

## SST remontées (SST3.0 & SST3.1) (U_NO_EGID présent dans le DF LYNX)
 - cluster 3 : La SST consomme moin de 100kW et ne dispose pas d'ECS (U_PUISSANCE < 100kW AND U_PROD_ECS = 0)
 - cluster 4 : La SST consomme plus de 100kW et ne dispose pas d'ECS (U_PUISSANCE >= 100kW AND U_PROD_ECS = 0)
 - cluster 5 : La SST consomme moin de 100kW et dispose d'ECS (U_PUISSANCE < 100kW AND U_PROD_ECS = 1)
 - cluster 6 : La SST consomme plus de 100kW et dispose d'ECS (U_PUISSANCE >= 100kW AND U_PROD_ECS = 1)