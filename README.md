# statements_recognition
It's an utility to recognise statements uploaded from remote banking and 1C applications (statement on account 51) at excel or pdf files and transform it to csv files.
Currently it well recognises excel form 51 from 1C of a few formats (with accuracy circa 90%) and quite well recognises over 60 types of statements from different banks (well, excel is always better and faster than pdf).
For pdf table extraction if uses camelot library.
Current issues with statements is accuracy of footers and headers recognition (I would guess, slightly different approach for the tables extraction would be usefull)

The resulting csv files could be used for different data analysis or model training.

It's an fully open sourced, so you may clone and use it for free.
