cd %~dp0
pip3.6 install -r requirements.txt -i https://pypi.douban.com/simple/
python MyWeiboSpider.py config.json
pause