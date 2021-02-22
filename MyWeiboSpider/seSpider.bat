cd /d %~dp0
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python seSpider.py config.json
python SpiderByUsers_localfile.py config.json
pause