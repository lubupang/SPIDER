cd %~dp0
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python getdata.py 1
pause