# cb_auto_resolve

A python project of building stand alone executable program for MacOS and Windows.

Use python flask app to build the web service framework and use HTML/CSS/Javascript to build the front end user portal.


How to use:

1. Go to src folder and unpack zip file which contains chrome driver for python selenium

2. Use cmd to create a python virenv using Anaconda.

3. Activate the virenv and install all the necessary python modules as listed in run.py

4. in the virenv, run command: 


In Windows:

pyinstaller --add-data ".\templates\*.html;.\templates" --add-data ".\static\css\*.css;.\static\css" --add-data ".\src\chromedriver.exe;.\src" --add-data ".\download\results.csv;.\download" -F run.py

or 

In MacOS:

pyinstaller --add-data './templates/*.html:./templates' --add-data './static/css/*.css:./static/css' --add-data './src/chromedriver:./src' --add-data './download/results.csv:./download' -F run.py


5. Executable file would be generated under dist folder.


6. For other refrenece , please refer to PyInstaller Documentation or Knowledge Reference Folder in Mis Folder.

