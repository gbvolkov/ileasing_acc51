sudo apt-get install python3.11-distutils
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
python3.11 -m pip install --upgrade pip
python3.11 -m pip install pandas
python3.11 -m pip install PyPDF2==2.12.1
python3.11 -m pip install pdfminer-six
python3.11 -m pip uninstall xlrd
python3.11 -m pip install xlrd==2.0.1 --ignore-installed
python3.11 -m pip install camelot-py[base]