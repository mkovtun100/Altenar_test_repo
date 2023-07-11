# Altenar test project

This repository represents a pipeline which loads JSON-files from the GITHUB archive and performs a few analytical tasks.
Currently following tasks are ready in the butch mode:
  1. List of Developers that own more than one repository;
  2. List of Developers who did more than one commit in a day, ordered by name and number of commits;
  3. List of Developers with less than one commit in a day;

Currently this project is in Development stage. To check the code's work, you should download GITHUB archives to the "source" directory near the "python_home". Example URL: https://data.gharchive.org/2015-01-01-15.json.gz

Project structure.
-
root

  |-output
  
  |-python_home
  
    |-batch_load.py
    
  |-sources
  
  |-requirements.txt
  

Action plan for the code execution.
-
1. Clone the repo using the command:
   
   git clone https://github.com/mkovtun100/Altenar_test_repo.git
   
2. Install requirements
   pip install -r requirements.txt

3. Go to the "python_home" directory

4. Execute following command:

   ./bin/spark-submit \
   --driver-memory 8g \
   --executor-memory 16g \
   --executor-cores 2  \
   batch_load.py
