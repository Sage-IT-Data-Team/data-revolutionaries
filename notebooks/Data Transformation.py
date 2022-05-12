# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Data Transformation and Preparation
# MAGIC 
# MAGIC The goal of ths notebook is to take the data from **<a href='https://screeningtool.geoplatform.gov/en/#16.4/29.721163/-95.361853'>Climate and Economic Justice Screening Tool</a>** and combine this with spatial data and external data sources. 
# MAGIC 
# MAGIC We then define a rudimentary scoring metric to rank the areas which we believe to be the most disadvantaged for later use with a Mapping Dashboard which is built with <a href='https://plotly.com/dash/'>Dash</a>

# COMMAND ----------

from pyspark.sql.functions import col, sum, max, percent_rank, concat, lit, rank, collect_list, concat_ws
from pyspark.sql.window import Window
import pandas as pd

# COMMAND ----------

cols = ['State', 'County_Name', 'Census_tract_ID', 'Total_threshold_criteria_exceeded',
       'Total_categories_exceeded', 'Total_population', 'census_population_density', 'county_population_density', 
        'state_population_density', 'PM25', 'Diesel_Particulate',
       'Asthma',
       'Household_Income',
       'Census_Population_Density_Percentile',
       'Building_Loss_Rate',
       'Agricultural_Loss_Rate',
        'Traffic_Proximity'
       ]


description_cols = [
       'PM25', 'Diesel_Particulate',
       'Asthma',
       'Household_Income',
       'Building_Loss_Rate',
       'Agricultural_Loss_Rate',
        'Traffic_Proximity']

# COMMAND ----------

census_tract_centres = spark.sql('select * from one_tree_planted_census_tract_centres')
state_centres = spark.sql('select * from one_tree_planted_state_centres')
county_centres = spark.sql('select * from one_tree_planted_county_centres')

df = (spark.sql('select * from one_tree_planted_ejst'))


df = (df
       .withColumnRenamed('PM2.5_in_the_air__percentile_', 'PM25')
       .withColumnRenamed('Diesel_particulate_matter_exposure__percentile_', 'Diesel_Particulate')
       .withColumnRenamed('Current_asthma_among_adults_aged_greater_than_or_equal_to_18_years__percentile_', 'Asthma')
       .withColumnRenamed('Low_median_household_income_as_a_percent_of_area_median_income__percentile_', 'Household_Income')
       .withColumnRenamed('Expected_building_loss_rate__Natural_Hazards_Risk_Index___percentile_', 'Building_Loss_Rate')
       .withColumnRenamed('Expected_agricultural_loss_rate__Natural_Hazards_Risk_Index___percentile_', 'Agricultural_Loss_Rate')
       .withColumnRenamed('Traffic_proximity_and_volume__percentile_', 'Traffic_Proximity')
     )
                           



cities_df = (spark.sql('select * from one_tree_planted_cities_lookup')
             .select('city', 'county_name', 'population', 'lat', 'lng', 'id')
             .withColumnRenamed('lat', 'city_lat')
             .withColumnRenamed('lng', 'city_lon')
             .withColumnRenamed('id', 'city_id')
             .withColumnRenamed('county_name', 'countyName')
             .withColumn('countyName', concat(col('countyName'), lit(' County')))
             .withColumn('pop_rank', rank().over(Window.partitionBy(col('countyName')).orderBy(col('population').desc())))
             .filter(col('pop_rank') == 1)
             .drop('population', 'pop_rank')
            )

df = df.join(cities_df, on=cities_df.countyName == df.County_Name, how='left').drop('countyName')

census_pop_df = (df
                 .select('Census_tract_ID', 'Total_population', 'geo_area')
                 .withColumn('census_population_density', col('Total_population') / col('geo_area'))
                 .drop('Total_population', 'geo_area')
                )

county_pop_df = (df
                .groupBy('County_Name').agg((sum('total_population') / max('cf_area')).alias('county_population_density'))
                )

state_pop_df = (df
                .groupBy('State').agg((sum('total_population') / max('sf_area')).alias('state_population_density'))
                )

city_pop_df = (df
                .groupBy('city').agg((sum('total_population') / sum('geo_area')).alias('city_population_density'))
                )


full_df = (df
          .join(census_pop_df, on='Census_tract_id', how='left')
          .join(county_pop_df, on='County_Name', how='left')
          .join(state_pop_df, on='State', how='left')
          .join(city_pop_df, on='city', how='left')
         )

full_df = (full_df
         .withColumn('Census_Population_Density_Percentile', percent_rank().over(Window.partitionBy().orderBy(full_df['census_population_density'].desc())))
         )

# COMMAND ----------

display(full_df.select(cols).orderBy('Census_Population_Density_Percentile', ascending=False))

# COMMAND ----------

full_df = (full_df
          .withColumn('summed_percentiles', 
                     col('PM25') + 
                     col('Diesel_Particulate') +
                     col('Asthma') +
                     col('Household_Income') +
                     col('Census_Population_Density_Percentile') +
                     col('Building_Loss_Rate') +
                     col('Agricultural_Loss_Rate') +
                     col('Traffic_Proximity')
                     )
          .orderBy('summed_percentiles', ascending=False)
         )

display(full_df)

# COMMAND ----------

state_description_df = full_df.select('state', *description_cols).groupBy('state').agg(                     
                     sum('PM25').alias('PM25'),
                     sum('Diesel_Particulate').alias('Diesel_Particulate'),
                     sum('Asthma').alias('Asthma'),
                     sum('Household_Income').alias('Household_Income'),
                     sum('Building_Loss_Rate').alias('Building_Loss_Rate'),
                     sum('Agricultural_Loss_Rate').alias('Agricultural_Loss_Rate'),
                     sum('Traffic_Proximity').alias('Traffic_Proximity')
                                                                                      )

state_description_df = (state_description_df
                        .withColumn('PM25', percent_rank().over(Window.partitionBy().orderBy(state_description_df['PM25'])))
                        .withColumn('Diesel_Particulate', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Diesel_Particulate'])))
                        .withColumn('Asthma', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Asthma'])))
                        .withColumn('Household_Income', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Household_Income'])))
                        .withColumn('Building_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Building_Loss_Rate'])))
                        .withColumn('Agricultural_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Agricultural_Loss_Rate'])))
                        .withColumn('Traffic_Proximity', percent_rank().over(Window.partitionBy().orderBy(state_description_df['Traffic_Proximity'])))
                       )

city_description_df = full_df.select('city', *description_cols).groupBy('city').agg(                     
                     sum('PM25').alias('PM25'),
                     sum('Diesel_Particulate').alias('Diesel_Particulate'),
                     sum('Asthma').alias('Asthma'),
                     sum('Household_Income').alias('Household_Income'),
                     sum('Building_Loss_Rate').alias('Building_Loss_Rate'),
                     sum('Agricultural_Loss_Rate').alias('Agricultural_Loss_Rate'),
                     sum('Traffic_Proximity').alias('Traffic_Proximity')
                                                                                      )

city_description_df = (city_description_df
                        .withColumn('PM25', percent_rank().over(Window.partitionBy().orderBy(city_description_df['PM25'])))
                        .withColumn('Diesel_Particulate', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Diesel_Particulate'])))
                        .withColumn('Asthma', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Asthma'])))
                        .withColumn('Household_Income', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Household_Income'])))
                        .withColumn('Building_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Building_Loss_Rate'])))
                        .withColumn('Agricultural_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Agricultural_Loss_Rate'])))
                        .withColumn('Traffic_Proximity', percent_rank().over(Window.partitionBy().orderBy(city_description_df['Traffic_Proximity'])))
                       )

county_description_df = full_df.select('county_name', *description_cols).groupBy('county_name').agg(                     
                     sum('PM25').alias('PM25'),
                     sum('Diesel_Particulate').alias('Diesel_Particulate'),
                     sum('Asthma').alias('Asthma'),
                     sum('Household_Income').alias('Household_Income'),
                     sum('Building_Loss_Rate').alias('Building_Loss_Rate'),
                     sum('Agricultural_Loss_Rate').alias('Agricultural_Loss_Rate'),
                     sum('Traffic_Proximity').alias('Traffic_Proximity')
                                                                                      )

county_description_df = (county_description_df
                        .withColumn('PM25', percent_rank().over(Window.partitionBy().orderBy(county_description_df['PM25'])))
                        .withColumn('Diesel_Particulate', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Diesel_Particulate'])))
                        .withColumn('Asthma', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Asthma'])))
                        .withColumn('Household_Income', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Household_Income'])))
                        .withColumn('Building_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Building_Loss_Rate'])))
                        .withColumn('Agricultural_Loss_Rate', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Agricultural_Loss_Rate'])))
                        .withColumn('Traffic_Proximity', percent_rank().over(Window.partitionBy().orderBy(county_description_df['Traffic_Proximity'])))
                        )

# COMMAND ----------

state_description_pandas = state_description_df.toPandas()

state_description_pandas = (state_description_pandas
                            .melt(id_vars=['state'])
                            .groupby(['state'])
                            .apply(lambda x: x.nlargest(3, columns=['value']))
                           )

state_description_nlargest = spark.createDataFrame(state_description_pandas)

state_description_nlargest = (state_description_nlargest
                              .withColumn('description', concat(col('variable'), lit(': '), (col('value') * 100).cast('int'),lit('%')))
                              .groupby('state').agg(collect_list('description').alias('description'))
                              .withColumn('description', concat(lit('Ranks highly in: '), concat_ws(', ', col('description'))))
                             )
display(state_description_nlargest)

# COMMAND ----------

city_description_pandas = city_description_df.toPandas()

city_description_pandas = (city_description_pandas
                            .melt(id_vars=['city'])
                            .groupby(['city'])
                            .apply(lambda x: x.nlargest(3, columns=['value']))
                           )

city_description_nlargest = spark.createDataFrame(city_description_pandas)

city_description_nlargest = (city_description_nlargest
                              .withColumn('description', concat(col('variable'), lit(': '), (col('value') * 100).cast('int'),lit('%')))
                              .groupby('city').agg(collect_list('description').alias('description'))
                              .withColumn('description', concat(lit('Ranks highly in: '), concat_ws(', ', col('description'))))
                             )
display(city_description_nlargest)

# COMMAND ----------

county_description_pandas = county_description_df.toPandas()

county_description_pandas = (county_description_pandas
                            .melt(id_vars=['county_name'])
                            .groupby(['county_name'])
                            .apply(lambda x: x.nlargest(3, columns=['value']))
                           )

county_description_nlargest = spark.createDataFrame(county_description_pandas)

county_description_nlargest = (county_description_nlargest
                              .withColumn('description', concat(col('variable'), lit(': '), (col('value') * 100).cast('int'),lit('%')))
                              .groupby('county_name').agg(collect_list('description').alias('description'))
                              .withColumn('description', concat(lit('Ranks highly in: '), concat_ws(', ', col('description'))))
                             )
display(county_description_nlargest)

# COMMAND ----------

city_ranks = full_df.groupBy('city_id').agg(sum('summed_percentiles').alias('rank')).orderBy('rank', ascending=False).limit(501)
census_ranks = full_df.groupBy('census_tract_id').agg(sum('summed_percentiles').alias('rank')).orderBy('rank', ascending=False).limit(501)
state_ranks = full_df.groupBy('state').agg(sum('summed_percentiles').alias('rank')).orderBy('rank', ascending=False).limit(501)
county_ranks = full_df.groupBy('county_name').agg(sum('summed_percentiles').alias('rank')).orderBy('rank', ascending=False).limit(501)

county_final = (county_ranks
                .join(county_centres, on=county_ranks.county_name == county_centres.CF, how='left')
                .join(county_description_nlargest, on='county_name', how='left')
                .select('county_name', 'rank', 'lat', 'lon', 'description')
                .withColumn('rank', rank().over(Window.partitionBy().orderBy(county_ranks['rank'].desc())))
               )

state_final = (state_ranks
                .join(state_centres, on=state_ranks.state == state_centres.SF, how='left')
                .join(state_description_nlargest, on='state', how='left')
                .select('state', 'rank', 'lat', 'lon', 'description')
                .withColumn('rank', rank().over(Window.partitionBy().orderBy(state_ranks['rank'].desc())))
               )

census_final = (census_ranks
                .join(census_tract_centres, on=census_ranks.census_tract_id == census_tract_centres.GEOID10, how='left')
                .select('census_tract_id', 'rank', 'lat', 'lon')
                .withColumn('rank', rank().over(Window.partitionBy().orderBy(census_ranks['rank'].desc())))
               )



city_final = (city_ranks
              .join(cities_df, on='city_id', how='left')
              .join(city_description_nlargest, on='city', how='left')
              .select('city', 'rank', 'city_lat', 'city_lon', 'description')
              .withColumnRenamed('city_lat', 'lat')
              .withColumnRenamed('city_lon', 'lon')
             ).na.drop().withColumn('rank', rank().over(Window.partitionBy().orderBy(city_ranks['rank'].desc())))



# COMMAND ----------

display(city_final)

# COMMAND ----------


