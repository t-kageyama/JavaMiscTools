# JavaMiscTools
====

Overview

## Version
1.0.1

## Description
Java miscellaneous tools.<br/>
we have 2 tools so far.<br/>
one is copy record utility copy_record.sh in JavaMiscTools/shells directory.<br/>
this command line tool will copy 1 SQL table record & in sert another record.<br/>
most column values were copied, and you only to care, not to violate SQL key/index consistency.<br/>
so, you do not to count a number of columns of your talbes have.
you only to care about few columns.<br/>
another one is insert record utility insert_record.shin JavaMiscTools/shells directory.<br/>
this command line tool let you insert 1 record into your SQL table.<br/>
if your table columns are well defined with default values, this tool helps you a lot.<br/>
if your table columns are not well defined with default values, you are better to user vendor official SQL tools.

## Requirement
* Java 1.8 or above.
* Gradle (tested on version 6.8).

## Usage
* pull from git & do "./gradle shadowJar" in your JavaMiscTools directory.
* fat jar JavaMiscTools-$VERSION-all.jar will generated in your JavaMiscTools/build/libs directory.
* see shell scripts in JavaMiscTools/shells directory & create your own shell scripts, which suitable to your environment.
* Enjoy!

## Licence

[MIT](https://github.com/t-kageyama/JavaMiscTools/blob/master/LICENSE)

## Author

[t-kageyama](https://github.com/t-kageyama)
