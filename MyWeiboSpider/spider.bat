cd %~dp0
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python SpiderByUsers_localfile.py config.json
pause