# beeminder-goal-tracking-analysis

### Status: 🚧 Currently in development 🚧
Updates coming soon as I continue exploring Beeminder's user behavior and engagement.<br><br>

## Project Overview
Beeminder is a goal-tracking app where users set their own measurable goals and face financial penalties if they fail to meet their commitments. The app incentivizes users to make consistent progress through behavioral nudges.
<br>
This project explores and analyzes how user behavior and goal engagement in 2019 impact platform retention and revenue. Identifying trends in user engagement, goal derailments, premium plan subscriptions, goal success rates, time of entries, tracking methods, etc. are all key components under investigation.
<br>
Analysis is conducted using a combination of SQL, Excel, and Tableau to clean, analyze, and summarize these insights to deliver to the marketing, product and operations teams.

## Data Structure Overview
The dataset provides user and goal data. The raw dataset consists of about 1 Billion rows:

[Data Cleaning & 3NF Normalization SQL File](/beeminder-data-cleaning.sql) - Steps taken to clean, check data quality and prep the dataset for analysis through 3NF Normalization.

[Beeminder ERD](/beeminder-entity-relationship-diagram.png) - Diagram representing the relational data model, capturing the structure and relationships among datasets.<br><br>
<img width="3643" height="3073" alt="beeminder-entity-relationship-diagram" src="https://github.com/user-attachments/assets/a8ba4bb8-b6b2-4dde-8985-70ae0da6a05a" />
<br>
