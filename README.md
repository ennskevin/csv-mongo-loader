### Connection String for Local database
mongodb://localhost:27017

### PopulateMongoDB
ARG 1 = 'your-file'
  located in either the maven root or the same directory as the packaged jar

PopulateMongoDB is a mvn project, make sure mvn is installed and run 'mvn clean install'

To run PopulateMongoDB, cd into directory and run 
  'mvn exec:java -Dexec.args="\<your-file\>"'

This app does NOT clean up the collection or documents when done.

Load time for 4 million is ~90 sec

Load time for 40k is < 20 sec



### mongosh
The app uses (or creates if not there) the 'cs314' database. So when using the shell, make sure to 'use cs314' before anything else
  \>mongosh

  \>use cs314

  \>have fun
