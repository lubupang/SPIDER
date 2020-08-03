cd %~dp0
pip3.6 install -r requirements.txt -i https://pypi.douban.com/simple/
python36 MyWeiboSpider.py config.json
pause